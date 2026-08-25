use chrono::{Duration, Utc};
use insight_platform_artifacts::{
    encode_canonical_skill_package, ArtifactObjectReadAuthority, ArtifactObjectReadAuthorityError,
    SchedulerRunValueLease, SchedulerRunValueRequestResolver, SchedulerSkillPackageLease,
    SchedulerSkillPackageRequestResolver, SchedulerTypedPlanLease,
    SchedulerTypedPlanRequestResolver, SkillPackageContent, MAX_SCHEDULER_SKILL_PACKAGE_BYTES,
};
use insight_platform_contracts::{
    canonical_digest, canonical_json, checked_in_hard_limit_profile, pinned_nominal_reference,
    AgentDeploymentClosure, AgentResourceSpec, ArtifactRef, AuthoringPackage,
    CandidateSelectionMode, CandidateSelectionPolicyDocument, ClosedJsonSchema, ClosedJsonValue,
    CommandAudit, CommandOutcome, DataClassification, DeploymentClosure, ExactDeploymentRef,
    ExactPolicyBinding, ExactVersionRef, Failure, FailureClass, FailureCode, FailureSource,
    FrozenSlotBinding, FrozenSlotTarget, InteractionKind, JsonLimits, Permission, PermissionSet,
    PlanNodeKind, PlatformFailureCode, PolicyDeploymentClosure, PolicyKind, PolicyResourceSpec,
    PrincipalBindingsPayload, PrincipalKind, PrincipalSnapshot, PublicRunEventType,
    PublishedVersionPayload, ResourceDocument, ResourceId, Retryability, RunBindingsSnapshot,
    SchedulingPolicyDocument, Sha256Digest, SkillArtifactSliceRef, SkillDeploymentClosure,
    SkillInstructionAudience, SkillInstructionPhase, SkillInstructionSection, SkillInterface,
    SkillPackageEntry, SkillPackageEntryKind, SkillPackageManifest, SkillResourceSpec,
    TenantConfig, TenantPrincipalPayload, ValidationSummary, ValueRef, SKILL_PACKAGE_MEDIA_TYPE,
};
use insight_platform_jobs::WakeSource;
use insight_platform_orchestrator::{
    derive_candidate_selection, derive_expression_controller, AdmitRun, ChildBudget,
    ChildCancellationPolicy, ChildOutcome, CommittedExpressionInput, ControllerObservation,
    DataPortKey, ExpressionLimits, HumanTaskDefinition, JoinPolicy, JoinRemainderPolicy,
    LoopCarriedPort, MapFailurePolicy, ObserveRunTimeout, PlanLimits, PlanNodeKey, PortAssignment,
    RequestRunCancel, RunInputValue, RuntimeDependencyKind, RuntimeDependencySlot, RuntimeNode,
    RuntimePlan, SetRunPause, TypedExpressionProgram, TypedInstruction,
};
use insight_platform_postgres::{
    repository::{
        ApplyDerivedExpressionControllerStep, ApplyOrchestrationControllerStep,
        ChildRunCancellationSlot, ClaimOrchestrationJobs, ClaimedOrchestrationJob,
        CommitPlanTerminal, ControllerActivationSlot, ControllerFacts, ControllerLoopRolloverSlot,
        ControllerPendingNodeSlot, ControllerPendingWakeSlot, ControllerRemainderCancellationSlot,
        ControllerScopeSlot, ControllerStepMutationIds, ControllerStructuralExitSlot,
        DeferOrchestrationChildMutationIds, DeferOrchestrationTaskMutationIds,
        DeferOrchestrationToChildRun, DeferOrchestrationToTask, DriveChildRunCancellations,
        DriveDueOrchestrationRetries, DriveDueOrchestrationWaits, DriveExpiredOrchestrationJobs,
        DriveExpiredOrchestrationTasks, DriveOrchestrationConvergence, DriveTerminalChildRuns,
        DueOrchestrationRetrySlot, DueOrchestrationWaitSlot, ExpiredOrchestrationRecoverySlot,
        ExpiredOrchestrationTaskSlot, FailOrchestrationJob, HeartbeatJob, JobFence,
        MaterializedTerminalValue, NewPrincipal, NewQuotaAccount, NewTenant, NewTenantPrincipal,
        OrchestrationClaimSlot, OrchestrationConvergenceSlot, OrchestrationFailureCause,
        OrchestrationTerminalMutationIds, OrchestrationWakeMutationIds, OrchestrationYield,
        OrchestrationYieldMutationIds, PgRepository, RepositoryError, ResolveOrchestrationTask,
        ResolveOrchestrationTaskMutationIds, RunRecord, SafetyScanShard, StartOrchestrationJob,
        TerminalChildRunSlot, TypedPayload, WakeOrchestrationJob, YieldOrchestrationJob,
    },
    verify_schema,
};
use insight_platform_security::BindTenantSchedulingPolicy;
use insight_platform_tasks::{TaskDefinition, TaskState};
use serde_json::{json, Value};
use sha2::{Digest as _, Sha256};
use sqlx::{postgres::PgPoolOptions, PgPool};
use std::{collections::BTreeMap, fmt::Write as _};

const TENANT_ID: &str = "ten_0198f1c3-9a00-7c3e-b1f3-773c28367001";
const TENANT_B_ID: &str = "ten_0198f1c3-9a00-7c3e-b1f3-773c28367002";
const PRINCIPAL_ID: &str = "prn_0198f1c3-9a00-7c3e-b1f3-773c28367003";
const DENIED_PRINCIPAL_ID: &str = "prn_0198f1c3-9a00-7c3e-b1f3-773c28367004";
const POLICY_ID: &str = "pol_0198f1c3-9a00-7c3e-b1f3-773c28367005";
const POLICY_REVISION_ID: &str = "prev_0198f1c3-9a00-7c3e-b1f3-773c28367006";
const POLICY_DEPLOYMENT_ID: &str = "pdep_0198f1c3-9a00-7c3e-b1f3-773c2836700f";
const SELECTION_POLICY_ID: &str = "pol_0198f1c3-9a00-7c3e-b1f3-773c28367105";
const SELECTION_POLICY_REVISION_ID: &str = "prev_0198f1c3-9a00-7c3e-b1f3-773c28367106";
const SELECTION_POLICY_DEPLOYMENT_ID: &str = "pdep_0198f1c3-9a00-7c3e-b1f3-773c2836710f";
const POLICY_EVIDENCE_ARTIFACT_ID: &str = "art_0198f1c3-9a00-7c3e-b1f3-773c2836700e";
const POLICY_EVIDENCE_BLOB_ID: &str = "iblb_0198f1c3-9a00-7c3e-b1f3-773c2836700e";
const AGENT_ID: &str = "agt_0198f1c3-9a00-7c3e-b1f3-773c28367007";
const AGENT_INTERFACE_ID: &str = "aif_0198f1c3-9a00-7c3e-b1f3-773c28367008";
const AGENT_PLAN_ID: &str = "arev_0198f1c3-9a00-7c3e-b1f3-773c28367009";
const AGENT_DEPLOYMENT_ID: &str = "adep_0198f1c3-9a00-7c3e-b1f3-773c2836700a";
const SKILL_ID: &str = "skl_0198f1c3-9a00-7c3e-b1f3-773c28367201";
const SKILL_REVISION_ID: &str = "srev_0198f1c3-9a00-7c3e-b1f3-773c28367202";
const SKILL_DEPLOYMENT_ID: &str = "skdep_0198f1c3-9a00-7c3e-b1f3-773c28367203";
const SKILL_PACKAGE_ARTIFACT_ID: &str = "art_0198f1c3-9a00-7c3e-b1f3-773c28367204";
const SKILL_PACKAGE_BLOB_ID: &str = "blb_0198f1c3-9a00-7c3e-b1f3-773c28367205";
const SKILL_PACKAGE_ENCRYPTION_DOMAIN_ID: &str = "enc_0198f1c3-9a00-7c3e-b1f3-773c28367206";
const CHILD_AGENT_ID: &str = "agt_0198f1c3-9a00-7c3e-b1f3-773c2836700b";
const CHILD_AGENT_INTERFACE_ID: &str = "aif_0198f1c3-9a00-7c3e-b1f3-773c2836701b";
const CHILD_AGENT_PLAN_ID: &str = "arev_0198f1c3-9a00-7c3e-b1f3-773c2836702b";
const CHILD_AGENT_DEPLOYMENT_ID: &str = "adep_0198f1c3-9a00-7c3e-b1f3-773c2836703b";
const WORKER_ID: &str = "wrk_0198f1c3-9a00-7c3e-b1f3-773c2836700c";
const WORKER_B_ID: &str = "wrk_0198f1c3-9a00-7c3e-b1f3-773c2836700f";
const WORKER_C_ID: &str = "wrk_0198f1c3-9a00-7c3e-b1f3-773c2836701f";
const WORKER_D_ID: &str = "wrk_0198f1c3-9a00-7c3e-b1f3-773c2836702f";
const QUOTA_ACCOUNT_ID: &str = "qac_0198f1c3-9a00-7c3e-b1f3-773c2836700d";
const AGENT_AUTHORING_ARTIFACT_ID: &str = "art_0198f1c3-9a00-7c3e-b1f3-773c2836704c";
const TYPED_PLAN_ARTIFACT_ID: &str = "art_0198f1c3-9a00-7c3e-b1f3-773c2836702c";
const TYPED_PLAN_BLOB_ID: &str = "blb_0198f1c3-9a00-7c3e-b1f3-773c2836702c";
const TYPED_PLAN_ENCRYPTION_DOMAIN_ID: &str = "enc_0198f1c3-9a00-7c3e-b1f3-773c2836702c";

fn id(value: &str) -> ResourceId {
    value.parse().unwrap()
}

fn digest(character: char) -> Sha256Digest {
    format!("sha256:{}", character.to_string().repeat(64))
        .parse()
        .unwrap()
}

fn digest_bytes(bytes: &[u8]) -> Sha256Digest {
    let mut value = String::with_capacity(71);
    value.push_str("sha256:");
    for byte in Sha256::digest(bytes) {
        write!(&mut value, "{byte:02x}").unwrap();
    }
    value.parse().unwrap()
}

fn agent_schema() -> ClosedJsonSchema {
    ClosedJsonSchema::build(json!({
        "$schema": "https://json-schema.org/draft/2020-12/schema",
        "type": "object",
        "properties": {
            "question": {
                "type": "string",
                "minLength": 0,
                "maxLength": 1024,
                "x-platform-max-bytes": 4096
            }
        },
        "required": ["question"],
        "additionalProperties": false
    }))
    .unwrap()
}

fn agent_error_schema() -> ClosedJsonSchema {
    ClosedJsonSchema::build(json!({
        "$schema": "https://json-schema.org/draft/2020-12/schema",
        "$ref": pinned_nominal_reference("Failure").unwrap()
    }))
    .unwrap()
}

fn literal_program(value: Value) -> TypedExpressionProgram {
    TypedExpressionProgram::build(
        vec![],
        vec![TypedInstruction::Literal {
            value: ClosedJsonValue::build(digest('1'), value).unwrap(),
        }],
        digest('1'),
        ExpressionLimits::ABSOLUTE,
    )
    .unwrap()
}

fn run_input_predicate() -> TypedExpressionProgram {
    TypedExpressionProgram::build(
        vec![insight_platform_orchestrator::ExactDataPortRef::RunInput {
            schema_digest: agent_schema().canonical_digest,
        }],
        vec![TypedInstruction::Literal {
            value: ClosedJsonValue::build(digest('1'), json!(true)).unwrap(),
        }],
        digest('1'),
        ExpressionLimits::ABSOLUTE,
    )
    .unwrap()
}

fn input_compute_output_port() -> insight_platform_orchestrator::ExactDataPortRef {
    insight_platform_orchestrator::ExactDataPortRef::NodeOutput {
        producer_node_id: PlanNodeKey::new("input_compute".to_owned()).unwrap(),
        port_id: DataPortKey::new("echo".to_owned()).unwrap(),
        schema_digest: agent_schema().canonical_digest,
    }
}

fn return_run_input() -> RuntimeNode {
    RuntimeNode::Return {
        value: insight_platform_orchestrator::ExactDataPortRef::RunInput {
            schema_digest: agent_schema().canonical_digest,
        },
    }
}

fn return_compute_output() -> RuntimeNode {
    RuntimeNode::Return {
        value: input_compute_output_port(),
    }
}

fn echo_run_input_program() -> TypedExpressionProgram {
    let input = insight_platform_orchestrator::ExactDataPortRef::RunInput {
        schema_digest: agent_schema().canonical_digest,
    };
    TypedExpressionProgram::build(
        vec![input.clone()],
        vec![TypedInstruction::LoadPort { port: input }],
        agent_schema().canonical_digest,
        ExpressionLimits::ABSOLUTE,
    )
    .unwrap()
}

fn item_port(node: &str) -> insight_platform_orchestrator::ExactDataPortRef {
    insight_platform_orchestrator::ExactDataPortRef::NodeOutput {
        producer_node_id: PlanNodeKey::new(node.to_owned()).unwrap(),
        port_id: DataPortKey::new("item".to_owned()).unwrap(),
        schema_digest: digest('1'),
    }
}

fn loop_next_port() -> insight_platform_orchestrator::ExactDataPortRef {
    insight_platform_orchestrator::ExactDataPortRef::NodeOutput {
        producer_node_id: PlanNodeKey::new("loop".to_owned()).unwrap(),
        port_id: DataPortKey::new("carried".to_owned()).unwrap(),
        schema_digest: digest('1'),
    }
}

fn loop_body_output_port() -> insight_platform_orchestrator::ExactDataPortRef {
    insight_platform_orchestrator::ExactDataPortRef::NodeOutput {
        producer_node_id: PlanNodeKey::new("loop_body".to_owned()).unwrap(),
        port_id: DataPortKey::new("carried_out".to_owned()).unwrap(),
        schema_digest: digest('1'),
    }
}

fn runtime_plan() -> RuntimePlan {
    let entry = PlanNodeKey::new("entry".to_owned()).unwrap();
    let finish = PlanNodeKey::new("finish".to_owned()).unwrap();
    let fork = PlanNodeKey::new("fork".to_owned()).unwrap();
    let left = PlanNodeKey::new("left".to_owned()).unwrap();
    let right = PlanNodeKey::new("right".to_owned()).unwrap();
    let join = PlanNodeKey::new("join".to_owned()).unwrap();
    let quorum_fork = PlanNodeKey::new("quorum_fork".to_owned()).unwrap();
    let quorum_left = PlanNodeKey::new("quorum_left".to_owned()).unwrap();
    let quorum_right = PlanNodeKey::new("quorum_right".to_owned()).unwrap();
    let quorum_join = PlanNodeKey::new("quorum_join".to_owned()).unwrap();
    let cancel_fork = PlanNodeKey::new("cancel_fork".to_owned()).unwrap();
    let cancel_left = PlanNodeKey::new("cancel_left".to_owned()).unwrap();
    let cancel_right = PlanNodeKey::new("cancel_right".to_owned()).unwrap();
    let cancel_join = PlanNodeKey::new("cancel_join".to_owned()).unwrap();
    let boundary = PlanNodeKey::new("boundary".to_owned()).unwrap();
    let boundary_body = PlanNodeKey::new("boundary_body".to_owned()).unwrap();
    let boundary_handler = PlanNodeKey::new("boundary_handler".to_owned()).unwrap();
    let loop_node = PlanNodeKey::new("loop".to_owned()).unwrap();
    let loop_body = PlanNodeKey::new("loop_body".to_owned()).unwrap();
    let map_node = PlanNodeKey::new("map".to_owned()).unwrap();
    let map_body = PlanNodeKey::new("map_body".to_owned()).unwrap();
    let fail_fast_map = PlanNodeKey::new("fail_fast_map".to_owned()).unwrap();
    let fail_fast_map_body = PlanNodeKey::new("fail_fast_map_body".to_owned()).unwrap();
    let input_branch = PlanNodeKey::new("input_branch".to_owned()).unwrap();
    let input_otherwise = PlanNodeKey::new("input_otherwise".to_owned()).unwrap();
    let input_compute = PlanNodeKey::new("input_compute".to_owned()).unwrap();
    let child_call = PlanNodeKey::new("child_call".to_owned()).unwrap();
    let child_call_2 = PlanNodeKey::new("child_call_2".to_owned()).unwrap();
    let human_task = PlanNodeKey::new("human_task".to_owned()).unwrap();
    let expiring_task = PlanNodeKey::new("expiring_task".to_owned()).unwrap();
    let timer_wait = PlanNodeKey::new("timer_wait".to_owned()).unwrap();
    let signal_wait = PlanNodeKey::new("signal_wait".to_owned()).unwrap();
    let signal_timeout = PlanNodeKey::new("signal_timeout".to_owned()).unwrap();
    let raise = PlanNodeKey::new("raise".to_owned()).unwrap();
    RuntimePlan {
        plan_version: 4,
        interface_revision_id: id(AGENT_INTERFACE_ID),
        entry_node_id: entry.clone(),
        dependency_slots: BTreeMap::from([(
            "child_worker".to_owned(),
            RuntimeDependencySlot {
                kind: RuntimeDependencyKind::ChildAgent,
                requirement_digest: digest('b'),
            },
        )]),
        nodes: BTreeMap::from([
            (
                entry,
                RuntimeNode::Start {
                    next: finish.clone(),
                },
            ),
            (
                fork,
                RuntimeNode::Fork {
                    legs: vec![left.clone(), right.clone()],
                    join: join.clone(),
                },
            ),
            (
                left,
                RuntimeNode::Compute {
                    assignments: vec![],
                    next: join.clone(),
                },
            ),
            (
                right,
                RuntimeNode::Compute {
                    assignments: vec![],
                    next: join.clone(),
                },
            ),
            (
                join,
                RuntimeNode::Join {
                    policy: JoinPolicy::AllSuccess,
                    quorum: None,
                    remainder: None,
                    next: finish.clone(),
                },
            ),
            (
                quorum_fork,
                RuntimeNode::Fork {
                    legs: vec![quorum_left.clone(), quorum_right.clone()],
                    join: quorum_join.clone(),
                },
            ),
            (
                quorum_left,
                RuntimeNode::Compute {
                    assignments: vec![],
                    next: quorum_join.clone(),
                },
            ),
            (
                quorum_right,
                RuntimeNode::Compute {
                    assignments: vec![],
                    next: quorum_join.clone(),
                },
            ),
            (
                quorum_join,
                RuntimeNode::Join {
                    policy: JoinPolicy::Quorum,
                    quorum: Some(1),
                    remainder: Some(JoinRemainderPolicy::Drain),
                    next: finish.clone(),
                },
            ),
            (
                cancel_fork,
                RuntimeNode::Fork {
                    legs: vec![cancel_left.clone(), cancel_right.clone()],
                    join: cancel_join.clone(),
                },
            ),
            (
                cancel_left,
                RuntimeNode::Compute {
                    assignments: vec![],
                    next: cancel_join.clone(),
                },
            ),
            (
                cancel_right,
                RuntimeNode::Compute {
                    assignments: vec![],
                    next: cancel_join.clone(),
                },
            ),
            (
                cancel_join,
                RuntimeNode::Join {
                    policy: JoinPolicy::Quorum,
                    quorum: Some(1),
                    remainder: Some(JoinRemainderPolicy::Cancel),
                    next: finish.clone(),
                },
            ),
            (
                boundary,
                RuntimeNode::ErrorBoundary {
                    body: boundary_body.clone(),
                    handlers: BTreeMap::from([(
                        "dependency_unavailable".to_owned(),
                        boundary_handler.clone(),
                    )]),
                },
            ),
            (
                boundary_body,
                RuntimeNode::Compute {
                    assignments: vec![],
                    next: finish.clone(),
                },
            ),
            (
                boundary_handler,
                RuntimeNode::Compute {
                    assignments: vec![],
                    next: finish.clone(),
                },
            ),
            (
                loop_node.clone(),
                RuntimeNode::Loop {
                    condition: literal_program(json!(true)),
                    carried_ports: vec![LoopCarriedPort {
                        body_output_port: loop_body_output_port(),
                        next_iteration_port: loop_next_port(),
                    }],
                    body: loop_body.clone(),
                    exit: finish.clone(),
                    maximum_iterations: 2,
                },
            ),
            (
                loop_body,
                RuntimeNode::Compute {
                    assignments: vec![PortAssignment {
                        output_port: loop_body_output_port(),
                        expression: literal_program(json!({"iteration": 0})),
                    }],
                    next: loop_node,
                },
            ),
            (
                map_node.clone(),
                RuntimeNode::Map {
                    items: literal_program(json!([
                        {"item": 0},
                        {"item": 1},
                        {"item": 2}
                    ])),
                    item_port: item_port("map"),
                    body: map_body.clone(),
                    next: finish.clone(),
                    maximum_items: 3,
                    failure_policy: MapFailurePolicy::AllSettled,
                },
            ),
            (
                map_body,
                RuntimeNode::Compute {
                    assignments: vec![],
                    next: map_node,
                },
            ),
            (
                fail_fast_map.clone(),
                RuntimeNode::Map {
                    items: literal_program(json!([])),
                    item_port: item_port("fail_fast_map"),
                    body: fail_fast_map_body.clone(),
                    next: finish.clone(),
                    maximum_items: 3,
                    failure_policy: MapFailurePolicy::FailFast,
                },
            ),
            (
                fail_fast_map_body,
                RuntimeNode::Compute {
                    assignments: vec![],
                    next: fail_fast_map,
                },
            ),
            (
                input_branch,
                RuntimeNode::Branch {
                    ordered_arms: vec![insight_platform_orchestrator::BranchArm {
                        when: run_input_predicate(),
                        target: finish.clone(),
                    }],
                    otherwise: input_otherwise.clone(),
                },
            ),
            (input_otherwise, return_run_input()),
            (
                input_compute,
                RuntimeNode::Compute {
                    assignments: vec![insight_platform_orchestrator::PortAssignment {
                        output_port: input_compute_output_port(),
                        expression: echo_run_input_program(),
                    }],
                    next: finish.clone(),
                },
            ),
            (
                child_call.clone(),
                RuntimeNode::ChildAgentCall {
                    child_agent_slot_id: "child_worker".to_owned(),
                    input: insight_platform_orchestrator::ExactDataPortRef::RunInput {
                        schema_digest: agent_schema().canonical_digest,
                    },
                    candidate_route: None,
                    output: insight_platform_orchestrator::ExactDataPortRef::NodeOutput {
                        producer_node_id: child_call,
                        port_id: DataPortKey::new("response".to_owned()).unwrap(),
                        schema_digest: agent_schema().canonical_digest,
                    },
                    budget: insight_platform_orchestrator::ChildBudgetLimit {
                        maximum_duration_milliseconds: 60_000,
                        maximum_model_tokens: 1_000,
                        maximum_capability_calls: 10,
                        maximum_artifact_bytes: 1_048_576,
                        maximum_descendant_runs: 8,
                    },
                    cancellation_policy: ChildCancellationPolicy::CascadeAndWait,
                    attempt_limit: 3,
                    retry_backoff_milliseconds: 100,
                    resume: child_call_2.clone(),
                },
            ),
            (
                child_call_2.clone(),
                RuntimeNode::ChildAgentCall {
                    child_agent_slot_id: "child_worker".to_owned(),
                    input: insight_platform_orchestrator::ExactDataPortRef::RunInput {
                        schema_digest: agent_schema().canonical_digest,
                    },
                    candidate_route: None,
                    output: insight_platform_orchestrator::ExactDataPortRef::NodeOutput {
                        producer_node_id: child_call_2,
                        port_id: DataPortKey::new("response".to_owned()).unwrap(),
                        schema_digest: agent_schema().canonical_digest,
                    },
                    budget: insight_platform_orchestrator::ChildBudgetLimit {
                        maximum_duration_milliseconds: 60_000,
                        maximum_model_tokens: 1_000,
                        maximum_capability_calls: 10,
                        maximum_artifact_bytes: 1_048_576,
                        maximum_descendant_runs: 8,
                    },
                    cancellation_policy: ChildCancellationPolicy::CascadeAndWait,
                    attempt_limit: 3,
                    retry_backoff_milliseconds: 100,
                    resume: finish.clone(),
                },
            ),
            (
                human_task.clone(),
                RuntimeNode::HumanTask {
                    definition: HumanTaskDefinition::Interaction {
                        interaction_kind: InteractionKind::Form,
                        eligible_principal_rule_digest: digest('6'),
                        safe_prompt_key: "provide_input".to_owned(),
                    },
                    response: insight_platform_orchestrator::ExactDataPortRef::NodeOutput {
                        producer_node_id: human_task,
                        port_id: DataPortKey::new("response".to_owned()).unwrap(),
                        schema_digest: digest('5'),
                    },
                    timeout_milliseconds: 60_000,
                    resume: finish.clone(),
                },
            ),
            (
                expiring_task.clone(),
                RuntimeNode::HumanTask {
                    definition: HumanTaskDefinition::Interaction {
                        interaction_kind: InteractionKind::Form,
                        eligible_principal_rule_digest: digest('2'),
                        safe_prompt_key: "short_lived_input".to_owned(),
                    },
                    response: insight_platform_orchestrator::ExactDataPortRef::NodeOutput {
                        producer_node_id: expiring_task,
                        port_id: DataPortKey::new("response".to_owned()).unwrap(),
                        schema_digest: digest('3'),
                    },
                    timeout_milliseconds: 4_000,
                    resume: finish.clone(),
                },
            ),
            (
                timer_wait,
                RuntimeNode::TimerWait {
                    delay_milliseconds: 200,
                    resume: finish.clone(),
                },
            ),
            (
                signal_wait.clone(),
                RuntimeNode::SignalWait {
                    signal_key: "release".to_owned(),
                    payload: Some(
                        insight_platform_orchestrator::ExactDataPortRef::NodeOutput {
                            producer_node_id: signal_wait,
                            port_id: DataPortKey::new("payload".to_owned()).unwrap(),
                            schema_digest: digest('7'),
                        },
                    ),
                    timeout_milliseconds: 60_000,
                    resume: finish.clone(),
                },
            ),
            (
                signal_timeout,
                RuntimeNode::SignalWait {
                    signal_key: "short_lived_release".to_owned(),
                    payload: None,
                    timeout_milliseconds: 200,
                    resume: finish.clone(),
                },
            ),
            (finish, return_compute_output()),
            (
                raise,
                RuntimeNode::Raise {
                    failure: insight_platform_orchestrator::ExactDataPortRef::RunInput {
                        schema_digest: agent_error_schema().canonical_digest,
                    },
                },
            ),
        ]),
    }
}

fn child_runtime_plan() -> RuntimePlan {
    let entry = PlanNodeKey::new("entry".to_owned()).unwrap();
    let finish = PlanNodeKey::new("finish".to_owned()).unwrap();
    RuntimePlan {
        plan_version: 4,
        interface_revision_id: id(CHILD_AGENT_INTERFACE_ID),
        entry_node_id: entry.clone(),
        dependency_slots: BTreeMap::new(),
        nodes: BTreeMap::from([
            (
                entry,
                RuntimeNode::Start {
                    next: finish.clone(),
                },
            ),
            (finish, return_run_input()),
        ]),
    }
}

fn plan_limits() -> PlanLimits {
    let profile = checked_in_hard_limit_profile();
    PlanLimits::from_profile(&profile).unwrap()
}

fn audit(
    tenant_id: &str,
    principal_id: &str,
    suffix: &str,
    idempotency: char,
    request: char,
) -> CommandAudit {
    CommandAudit {
        tenant_id: id(tenant_id),
        principal_id: id(principal_id),
        principal_kind: PrincipalKind::AgentRunner,
        receipt_id: id(&format!("rcp_0198f1c3-9a00-7c3e-b1f3-773c2836{suffix}")),
        event_id: id(&format!("evt_0198f1c3-9a00-7c3e-b1f3-773c2836{suffix}")),
        outbox_id: id(&format!("obx_0198f1c3-9a00-7c3e-b1f3-773c2836{suffix}")),
        idempotency_key_digest: digest(idempotency),
        request_digest: digest(request),
        receipt_expires_at: Utc::now() + Duration::hours(1),
    }
}

fn applied(outcome: CommandOutcome<RunRecord>) -> RunRecord {
    match outcome {
        CommandOutcome::Applied(value) => value,
        CommandOutcome::Replayed(_) => panic!("expected an applied command"),
    }
}

macro_rules! run_command {
    ($repository:expr, $method:ident, $command:expr) => {{
        let mut transaction = $repository.begin_run_transaction().await.unwrap();
        let result = transaction.$method($command).await;
        match result {
            Ok(value) => {
                transaction.commit().await.unwrap();
                Ok(value)
            }
            Err(failure) => {
                transaction.rollback().await.unwrap();
                Err(failure)
            }
        }
    }};
}

#[test]
fn run_admission_and_controls_are_atomic_exact_and_first_winner() {
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .thread_stack_size(16 * 1024 * 1024)
        .enable_all()
        .build()
        .unwrap();
    runtime.block_on(async {
        tokio::spawn(async {
    let Ok(database_url) = std::env::var("PLATFORM_TEST_DATABASE_URL") else {
        eprintln!("PLATFORM_TEST_DATABASE_URL is unset; real PostgreSQL fixture skipped");
        return;
    };
    let pool = PgPoolOptions::new()
        .max_connections(12)
        .connect(&database_url)
        .await
        .unwrap();
    verify_schema(&pool).await.unwrap();
    let repository = PgRepository::new(pool.clone());
    seed_authorities(&repository, &pool).await;
    let (bindings, policy_deployment) = seed_agent_registry(&pool).await;
    let root_target = repository
        .resolve_root_run_target(&id(TENANT_ID), &id(AGENT_ID))
        .await
        .unwrap();
    assert_eq!(root_target.agent, bindings.agent);
    assert_eq!(root_target.closure.entry_node_id, "start");
    assert_eq!(root_target.closure.entry_node_kind, PlanNodeKind::Start);
    assert!(root_target.context_dataset_views.is_empty());
    let mut security = repository.begin_security_transaction().await.unwrap();
    assert!(matches!(
        security
            .bind_tenant_scheduling_policy(BindTenantSchedulingPolicy {
                audit: audit(TENANT_ID, PRINCIPAL_ID, "700e", 'e', 'f'),
                expected_tenant_version: 1,
                policy: policy_deployment,
            })
            .await
            .unwrap(),
        CommandOutcome::Applied(_)
    ));
    security.commit().await.unwrap();
    repository
        .create_quota_account(NewQuotaAccount {
            tenant_id: TENANT_ID.to_owned(),
            quota_account_id: QUOTA_ACCOUNT_ID.to_owned(),
            scope_kind: "tenant".to_owned(),
            scope_id: TENANT_ID.to_owned(),
            work_class: "orchestration".to_owned(),
            metric: "concurrent_jobs".to_owned(),
            limit_value: 4,
            payload: TypedPayload::empty(1).unwrap(),
        })
        .await
        .unwrap();

    let scheduler_command = admission(
        AdmissionIds {
            run: "run_0198f1c3-9a00-7c3e-b1f3-773c28367010",
            scope: "scp_0198f1c3-9a00-7c3e-b1f3-773c28367011",
            node: "nod_0198f1c3-9a00-7c3e-b1f3-773c28367012",
            job: "job_0198f1c3-9a00-7c3e-b1f3-773c28367013",
            value: "val_0198f1c3-9a00-7c3e-b1f3-773c28367014",
        },
        audit(TENANT_ID, PRINCIPAL_ID, "7015", '0', '1'),
        bindings.clone(),
        Utc::now() + Duration::minutes(5),
    );
    applied(run_command!(repository, admit_run, scheduler_command.clone()).unwrap());
    let claim = orchestration_claim();
    let mut scheduler = repository.begin_scheduler_transaction().await.unwrap();
    let rolled_back = scheduler
        .claim_orchestration_jobs(claim.clone())
        .await
        .unwrap();
    assert_eq!(rolled_back.len(), 1);
    scheduler.rollback().await.unwrap();
    assert_eq!(
        sqlx::query_scalar::<_, String>(
            "SELECT state FROM insight_platform.jobs WHERE tenant_id = $1 AND job_id = $2",
        )
        .bind(TENANT_ID)
        .bind(scheduler_command.orchestration_job_id.to_string())
        .fetch_one(&pool)
        .await
        .unwrap(),
        "ready"
    );
    let mut scheduler = repository.begin_scheduler_transaction().await.unwrap();
    let claimed = scheduler.claim_orchestration_jobs(claim).await.unwrap();
    scheduler.commit().await.unwrap();
    assert_eq!(claimed.len(), 1);
    assert_eq!(claimed[0].job.state, "leased");
    assert_eq!(claimed[0].job.lease_epoch, 1);
    assert_eq!(claimed[0].run_version, 2);
    assert_eq!(claimed[0].node_version, 2);
    assert_eq!(claimed[0].quota_account_ids, vec![QUOTA_ACCOUNT_ID]);
    let scheduler_atomic = sqlx::query(
        r#"
        SELECT
          (SELECT state FROM insight_platform.runs WHERE tenant_id = $1 AND run_id = $2) AS run_state,
          (SELECT active_work_count FROM insight_platform.runs WHERE tenant_id = $1 AND run_id = $2) AS active_work,
          (SELECT state FROM insight_platform.run_nodes WHERE tenant_id = $1 AND node_id = $3) AS node_state,
          (SELECT reserved_value FROM insight_platform.quota_accounts WHERE tenant_id = $1 AND quota_account_id = $4) AS reserved,
          (SELECT count(*) FROM insight_platform.events WHERE tenant_id = $1 AND event_type IN ('run.work_claimed','node.work_claimed','job.claimed') AND run_id = $2) AS claim_events
        "#,
    )
    .bind(TENANT_ID)
    .bind(scheduler_command.run_id.to_string())
    .bind(scheduler_command.entry_node_execution_id.to_string())
    .bind(QUOTA_ACCOUNT_ID)
    .fetch_one(&pool)
    .await
    .unwrap();
    use sqlx::Row as _;
    assert_eq!(
        scheduler_atomic.try_get::<String, _>("run_state").unwrap(),
        "running"
    );
    assert_eq!(
        scheduler_atomic.try_get::<i32, _>("active_work").unwrap(),
        1
    );
    assert_eq!(
        scheduler_atomic.try_get::<String, _>("node_state").unwrap(),
        "ready"
    );
    assert_eq!(scheduler_atomic.try_get::<i64, _>("reserved").unwrap(), 1);
    assert_eq!(
        scheduler_atomic.try_get::<i64, _>("claim_events").unwrap(),
        3
    );

    let concurrent_a = admission(
        AdmissionIds {
            run: "run_0198f1c3-9a00-7c3e-b1f3-773c28367600",
            scope: "scp_0198f1c3-9a00-7c3e-b1f3-773c28367601",
            node: "nod_0198f1c3-9a00-7c3e-b1f3-773c28367602",
            job: "job_0198f1c3-9a00-7c3e-b1f3-773c28367603",
            value: "val_0198f1c3-9a00-7c3e-b1f3-773c28367604",
        },
        audit(TENANT_ID, PRINCIPAL_ID, "7605", '6', '7'),
        bindings.clone(),
        Utc::now() + Duration::minutes(5),
    );
    let concurrent_b = admission(
        AdmissionIds {
            run: "run_0198f1c3-9a00-7c3e-b1f3-773c28367700",
            scope: "scp_0198f1c3-9a00-7c3e-b1f3-773c28367701",
            node: "nod_0198f1c3-9a00-7c3e-b1f3-773c28367702",
            job: "job_0198f1c3-9a00-7c3e-b1f3-773c28367703",
            value: "val_0198f1c3-9a00-7c3e-b1f3-773c28367704",
        },
        audit(TENANT_ID, PRINCIPAL_ID, "7705", '8', '9'),
        bindings.clone(),
        Utc::now() + Duration::minutes(5),
    );
    applied(run_command!(repository, admit_run, concurrent_a.clone()).unwrap());
    applied(run_command!(repository, admit_run, concurrent_b.clone()).unwrap());
    let claim_a = orchestration_claim_with_ids(
        WORKER_B_ID,
        'a',
        "7606",
        ["7607", "7608", "7609", "760a"],
        ["760b", "760c", "760d"],
    );
    let claim_b = orchestration_claim_with_ids(
        WORKER_C_ID,
        'b',
        "7706",
        ["7707", "7708", "7709", "770a"],
        ["770b", "770c", "770d"],
    );
    let (claimed_a, claimed_b) = tokio::join!(
        claim_with_serializable_retry(&repository, claim_a),
        claim_with_serializable_retry(&repository, claim_b),
    );
    let claimed_a = claimed_a.unwrap();
    let claimed_b = claimed_b.unwrap();
    assert_eq!(claimed_a.len(), 1);
    assert_eq!(claimed_b.len(), 1);
    assert_ne!(claimed_a[0].job.job_id, claimed_b[0].job.job_id);
    assert_eq!(
        [
            claimed_a[0].job.worker_id.as_deref(),
            claimed_b[0].job.worker_id.as_deref()
        ],
        [Some(WORKER_B_ID), Some(WORKER_C_ID)]
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT reserved_value FROM insight_platform.quota_accounts WHERE tenant_id = $1 AND quota_account_id = $2",
        )
        .bind(TENANT_ID)
        .bind(QUOTA_ACCOUNT_ID)
        .fetch_one(&pool)
        .await
        .unwrap(),
        3
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT count(*) FROM insight_platform.scheduler_state WHERE work_class = 'orchestration'",
        )
        .fetch_one(&pool)
        .await
        .unwrap(),
        1
    );

    let recovery_command = admission(
        AdmissionIds {
            run: "run_0198f1c3-9a00-7c3e-b1f3-773c28367a00",
            scope: "scp_0198f1c3-9a00-7c3e-b1f3-773c28367a01",
            node: "nod_0198f1c3-9a00-7c3e-b1f3-773c28367a02",
            job: "job_0198f1c3-9a00-7c3e-b1f3-773c28367a03",
            value: "val_0198f1c3-9a00-7c3e-b1f3-773c28367a04",
        },
        audit(TENANT_ID, PRINCIPAL_ID, "7a05", '5', '6'),
        bindings.clone(),
        Utc::now() + Duration::minutes(5),
    );
    applied(run_command!(repository, admit_run, recovery_command.clone()).unwrap());
    let mut short_claim = orchestration_claim_with_ids(
        WORKER_D_ID,
        'f',
        "7a06",
        ["7a07", "7a08", "7a09", "7a0a"],
        ["7a0b", "7a0c", "7a0d"],
    );
    // Keep the negative pre-expiry assertion outside normal CI scheduling jitter. The fixture
    // still observes the database lease deadline and then crosses it deliberately below.
    short_claim.lease_milliseconds = 1_000;
    let mut scheduler = repository.begin_scheduler_transaction().await.unwrap();
    let leased_for_recovery = scheduler
        .claim_orchestration_jobs(short_claim)
        .await
        .unwrap();
    scheduler.commit().await.unwrap();
    assert_eq!(leased_for_recovery.len(), 1);
    assert_eq!(leased_for_recovery[0].job.attempt_no, 0);
    assert_eq!(
        sqlx::query_scalar::<_, String>(
            "SELECT state FROM insight_platform.run_nodes WHERE tenant_id = $1 AND node_id = $2",
        )
        .bind(TENANT_ID)
        .bind(recovery_command.entry_node_execution_id.to_string())
        .fetch_one(&pool)
        .await
        .unwrap(),
        "ready"
    );
    let leased_recovery = DriveExpiredOrchestrationJobs {
        shard: SafetyScanShard { index: 1, count: 2 },
        after: None,
        limit: 1,
        slots: vec![ExpiredOrchestrationRecoverySlot {
            quota_entry_ids: ["7a0e", "7a0f", "7a10", "7a11"]
                .map(|suffix| id(&format!("qle_0198f1c3-9a00-7c3e-b1f3-773c2836{suffix}")))
                .to_vec(),
            run_event_id: id("evt_0198f1c3-9a00-7c3e-b1f3-773c28367a12"),
            run_outbox_id: id("obx_0198f1c3-9a00-7c3e-b1f3-773c28367a12"),
            node_event_id: id("evt_0198f1c3-9a00-7c3e-b1f3-773c28367a13"),
            node_outbox_id: id("obx_0198f1c3-9a00-7c3e-b1f3-773c28367a13"),
            job_event_id: id("evt_0198f1c3-9a00-7c3e-b1f3-773c28367a14"),
            job_outbox_id: id("obx_0198f1c3-9a00-7c3e-b1f3-773c28367a14"),
        }],
    };
    let mut scheduler = repository.begin_scheduler_transaction().await.unwrap();
    assert!(scheduler
        .drive_expired_orchestration_jobs(leased_recovery.clone())
        .await
        .unwrap()
        .is_empty());
    scheduler.commit().await.unwrap();
    tokio::time::sleep(std::time::Duration::from_millis(1_100)).await;
    let mut scheduler = repository.begin_scheduler_transaction().await.unwrap();
    let first_page = scheduler
        .drive_expired_orchestration_jobs(leased_recovery.clone())
        .await
        .unwrap();
    assert_eq!(first_page.len(), 1);
    assert!(!first_page.exhausted);
    let first_cursor = first_page.next_cursor.clone().unwrap();
    scheduler.rollback().await.unwrap();
    let mut after_first = leased_recovery.clone();
    after_first.after = Some(first_cursor);
    let mut scheduler = repository.begin_scheduler_transaction().await.unwrap();
    let after_first_page = scheduler
        .drive_expired_orchestration_jobs(after_first)
        .await
        .unwrap();
    scheduler.commit().await.unwrap();
    assert!(after_first_page.is_empty());
    assert!(after_first_page.exhausted);
    assert!(after_first_page.next_cursor.is_none());
    let mut other_shard = leased_recovery.clone();
    other_shard.shard = SafetyScanShard { index: 0, count: 2 };
    let mut scheduler = repository.begin_scheduler_transaction().await.unwrap();
    let other_shard_page = scheduler
        .drive_expired_orchestration_jobs(other_shard)
        .await
        .unwrap();
    scheduler.commit().await.unwrap();
    assert!(other_shard_page.is_empty());
    assert!(other_shard_page.exhausted);
    assert_eq!(
        sqlx::query_scalar::<_, String>(
            "SELECT state FROM insight_platform.jobs WHERE tenant_id = $1 AND job_id = $2",
        )
        .bind(TENANT_ID)
        .bind(recovery_command.orchestration_job_id.to_string())
        .fetch_one(&pool)
        .await
        .unwrap(),
        "leased"
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT reserved_value FROM insight_platform.quota_accounts WHERE tenant_id = $1 AND quota_account_id = $2",
        )
        .bind(TENANT_ID)
        .bind(QUOTA_ACCOUNT_ID)
        .fetch_one(&pool)
        .await
        .unwrap(),
        4
    );
    let mut scheduler = repository.begin_scheduler_transaction().await.unwrap();
    let recovered_lease = scheduler
        .drive_expired_orchestration_jobs(leased_recovery.clone())
        .await
        .unwrap();
    scheduler.commit().await.unwrap();
    assert_eq!(recovered_lease.len(), 1);
    assert_eq!(recovered_lease[0].job.state, "ready");
    assert_eq!(recovered_lease[0].job.attempt_no, 0);
    assert_eq!(recovered_lease[0].run.active_work_count, 0);
    assert_eq!(
        recovered_lease[0].settled_quota_account_ids,
        vec![QUOTA_ACCOUNT_ID]
    );
    let mut scheduler = repository.begin_scheduler_transaction().await.unwrap();
    assert!(scheduler
        .drive_expired_orchestration_jobs(leased_recovery)
        .await
        .unwrap()
        .is_empty());
    scheduler.commit().await.unwrap();

    let mut running_claim = orchestration_claim_with_ids(
        WORKER_D_ID,
        '0',
        "7a15",
        ["7a16", "7a17", "7a18", "7a19"],
        ["7a1a", "7a1b", "7a1c"],
    );
    running_claim.lease_milliseconds = 500;
    let mut scheduler = repository.begin_scheduler_transaction().await.unwrap();
    let claimed_for_running_recovery = scheduler
        .claim_orchestration_jobs(running_claim)
        .await
        .unwrap();
    scheduler.commit().await.unwrap();
    assert_eq!(claimed_for_running_recovery.len(), 1);
    let recovery_start = StartOrchestrationJob {
        fence: JobFence {
            tenant_id: TENANT_ID.to_owned(),
            job_id: claimed_for_running_recovery[0].job.job_id.clone(),
            worker_id: id(WORKER_D_ID),
            lease_epoch: claimed_for_running_recovery[0].job.lease_epoch,
            expected_job_version: claimed_for_running_recovery[0].job.version,
            lease_token_digest: digest('0'),
        },
        receipt_id: id("rcp_0198f1c3-9a00-7c3e-b1f3-773c28367a1d"),
        idempotency_key_digest: digest('1'),
        request_digest: digest('2'),
        receipt_expires_at: Utc::now() + Duration::hours(1),
        job_event_id: id("evt_0198f1c3-9a00-7c3e-b1f3-773c28367a1e"),
        job_outbox_id: id("obx_0198f1c3-9a00-7c3e-b1f3-773c28367a1e"),
        node_event_id: id("evt_0198f1c3-9a00-7c3e-b1f3-773c28367a1f"),
        node_outbox_id: id("obx_0198f1c3-9a00-7c3e-b1f3-773c28367a1f"),
    };
    let mut scheduler = repository.begin_scheduler_transaction().await.unwrap();
    let running_for_recovery = match scheduler
        .start_orchestration_job(recovery_start)
        .await
        .unwrap()
    {
        CommandOutcome::Applied(record) => record,
        CommandOutcome::Replayed(_) => panic!("recovery start must apply"),
    };
    scheduler.commit().await.unwrap();
    assert_eq!(running_for_recovery.attempt_no, 1);
    let typed_plan_bytes = canonical_json(&serde_json::to_value(runtime_plan()).unwrap()).unwrap();
    let expected_typed_plan_artifact = ArtifactRef::new(
        id(TYPED_PLAN_ARTIFACT_ID),
        runtime_plan().canonical_digest(plan_limits()).unwrap(),
        u64::try_from(typed_plan_bytes.len()).unwrap(),
        "application/json",
        DataClassification::Internal,
        Some("typed-plan.json".to_owned()),
    )
    .unwrap();
    let typed_plan_read = repository
        .resolve_typed_plan_read(SchedulerTypedPlanLease {
        tenant_id: id(TENANT_ID),
        run_id: id(running_for_recovery.run_id.as_deref().unwrap()),
        orchestration_job_id: id(&running_for_recovery.job_id),
        worker_process_generation_id: id(WORKER_D_ID),
        lease_generation: u64::try_from(running_for_recovery.lease_epoch).unwrap(),
        lease_token_digest: digest('0'),
        request_digest: digest('3'),
        maximum_bytes: typed_plan_bytes.len(),
        deadline: Utc::now() + Duration::milliseconds(400),
    })
        .await
        .unwrap();
    assert_eq!(typed_plan_read.plan_revision_id, id(AGENT_PLAN_ID));
    assert_eq!(typed_plan_read.artifact, expected_typed_plan_artifact);
    let authorized = repository
        .authorize_object_read(&typed_plan_read)
        .await
        .unwrap();
    assert_eq!(authorized.blob_id, id(TYPED_PLAN_BLOB_ID));
    let mut wrong_fence = typed_plan_read.clone();
    wrong_fence.lease_token_digest = digest('4');
    assert!(matches!(
        repository.authorize_object_read(&wrong_fence).await,
        Err(ArtifactObjectReadAuthorityError::Denied)
    ));
    let skill_package_lease = SchedulerSkillPackageLease {
        tenant_id: id(TENANT_ID),
        run_id: id(running_for_recovery.run_id.as_deref().unwrap()),
        orchestration_job_id: id(&running_for_recovery.job_id),
        worker_process_generation_id: id(WORKER_D_ID),
        lease_generation: u64::try_from(running_for_recovery.lease_epoch).unwrap(),
        lease_token_digest: digest('0'),
        skill_slot_id: "review_skill".to_owned(),
        skill_deployment_id: id(SKILL_DEPLOYMENT_ID),
        request_digest: digest('8'),
        maximum_bytes: MAX_SCHEDULER_SKILL_PACKAGE_BYTES,
        deadline: Utc::now() + Duration::milliseconds(400),
    };
    let skill_package_read = repository
        .resolve_skill_package_read(skill_package_lease.clone())
        .await
        .unwrap();
    assert_eq!(skill_package_read.skill_revision_id, id(SKILL_REVISION_ID));
    assert_eq!(
        skill_package_read.artifact.artifact_id(),
        &id(SKILL_PACKAGE_ARTIFACT_ID)
    );
    assert_eq!(
        repository
            .authorize_object_read(&skill_package_read)
            .await
            .unwrap()
            .blob_id,
        id(SKILL_PACKAGE_BLOB_ID)
    );
    let mut wrong_skill_slot = skill_package_lease.clone();
    wrong_skill_slot.skill_slot_id = "missing_skill".to_owned();
    assert!(matches!(
        repository
            .resolve_skill_package_read(wrong_skill_slot)
            .await,
        Err(ArtifactObjectReadAuthorityError::Denied)
    ));
    let mut unbound_skill = skill_package_lease;
    unbound_skill.skill_deployment_id =
        id("skdep_0198f1c3-9a00-7c3e-b1f3-773c28367204");
    assert!(matches!(
        repository.resolve_skill_package_read(unbound_skill).await,
        Err(ArtifactObjectReadAuthorityError::Denied)
    ));
    let artifact_value_body = json!({"question": "artifact terminal"});
    let artifact_value_bytes = canonical_json(&artifact_value_body).unwrap();
    let artifact_value_digest: Sha256Digest = canonical_digest(&artifact_value_body)
        .unwrap()
        .parse()
        .unwrap();
    let artifact_value_metadata =
        TypedPayload::new(1, &json!({"display_name": "terminal.json"})).unwrap();
    sqlx::query(
        r#"
        INSERT INTO insight_platform.artifact_blobs (
            tenant_id, blob_id, backend, storage_binding_digest,
            security_domain_digest, object_reference_ciphertext, object_generation, key_id,
            encryption_domain_id, content_digest, size_bytes, state, verified_at,
            created_at, updated_at
        ) VALUES ($1, $2, 'fixture', $3, $4, $5, 'run-value-generation-1',
                  'fixture-key', $6, $7, $8, 'verified', statement_timestamp(),
                  statement_timestamp(), statement_timestamp())
        "#,
    )
    .bind(TENANT_ID)
    .bind("blb_0198f1c3-9a00-7c3e-b1f3-773c2836ae00")
    .bind(digest('5').to_string())
    .bind(digest('6').to_string())
    .bind(vec![10_u8, 11, 12])
    .bind("enc_0198f1c3-9a00-7c3e-b1f3-773c2836ae03")
    .bind(artifact_value_digest.to_string())
    .bind(i64::try_from(artifact_value_bytes.len()).unwrap())
    .execute(&pool)
    .await
    .unwrap();
    sqlx::query(
        r#"
        INSERT INTO insight_platform.artifacts (
            tenant_id, artifact_id, blob_id, purpose, classification,
            expected_size_bytes, expected_digest, declared_media_type,
            verified_media_type, state, metadata_schema_version, metadata,
            metadata_digest, retention_policy_revision_id, retain_until, created_by
        ) VALUES ($1, $2, $3, 'run_output', 'internal', $4, $5,
                  'application/json', 'application/json', 'ready', $6, $7, $8,
                  $9, $10, $11)
        "#,
    )
    .bind(TENANT_ID)
    .bind("art_0198f1c3-9a00-7c3e-b1f3-773c2836ae01")
    .bind("blb_0198f1c3-9a00-7c3e-b1f3-773c2836ae00")
    .bind(i64::try_from(artifact_value_bytes.len()).unwrap())
    .bind(artifact_value_digest.to_string())
    .bind(artifact_value_metadata.schema_version)
    .bind(&artifact_value_metadata.value)
    .bind(&artifact_value_metadata.digest)
    .bind(POLICY_REVISION_ID)
    .bind(Utc::now() + Duration::days(30))
    .bind(PRINCIPAL_ID)
    .execute(&pool)
    .await
    .unwrap();
    sqlx::query(
        r#"
        INSERT INTO insight_platform.run_values (
            tenant_id, value_id, run_id, node_id, value_kind, classification,
            schema_digest, content_digest, inline_value, artifact_id
        ) VALUES ($1, $2, $3, NULL, 'terminal_fixture', 'internal', $4, $5, NULL, $6)
        "#,
    )
    .bind(TENANT_ID)
    .bind("val_0198f1c3-9a00-7c3e-b1f3-773c2836ae02")
    .bind(running_for_recovery.run_id.as_deref().unwrap())
    .bind(agent_schema().canonical_digest.to_string())
    .bind(artifact_value_digest.to_string())
    .bind("art_0198f1c3-9a00-7c3e-b1f3-773c2836ae01")
    .execute(&pool)
    .await
    .unwrap();
    let run_value_lease = SchedulerRunValueLease {
        tenant_id: id(TENANT_ID),
        run_id: id(running_for_recovery.run_id.as_deref().unwrap()),
        orchestration_job_id: id(&running_for_recovery.job_id),
        worker_process_generation_id: id(WORKER_D_ID),
        lease_generation: u64::try_from(running_for_recovery.lease_epoch).unwrap(),
        lease_token_digest: digest('0'),
        run_value_id: id("val_0198f1c3-9a00-7c3e-b1f3-773c2836ae02"),
        request_digest: digest('7'),
        maximum_bytes: artifact_value_bytes.len(),
        deadline: Utc::now() + Duration::milliseconds(400),
    };
    let run_value_read = repository
        .resolve_run_value_read(run_value_lease.clone())
        .await
        .unwrap();
    assert_eq!(run_value_read.schema_digest, agent_schema().canonical_digest);
    assert_eq!(run_value_read.classification, DataClassification::Internal);
    assert_eq!(
        run_value_read.artifact.artifact_id(),
        &id("art_0198f1c3-9a00-7c3e-b1f3-773c2836ae01")
    );
    let authorized_value = repository
        .authorize_object_read(&run_value_read)
        .await
        .unwrap();
    assert_eq!(
        authorized_value.blob_id,
        id("blb_0198f1c3-9a00-7c3e-b1f3-773c2836ae00")
    );
    let mut wrong_value = run_value_lease;
    wrong_value.run_value_id = id("val_0198f1c3-9a00-7c3e-b1f3-773c2836ae04");
    assert!(matches!(
        repository.resolve_run_value_read(wrong_value).await,
        Err(ArtifactObjectReadAuthorityError::Denied)
    ));
    tokio::time::sleep(std::time::Duration::from_millis(550)).await;
    assert!(matches!(
        repository.authorize_object_read(&typed_plan_read).await,
        Err(ArtifactObjectReadAuthorityError::Denied)
    ));
    assert!(matches!(
        repository.authorize_object_read(&run_value_read).await,
        Err(ArtifactObjectReadAuthorityError::Denied)
    ));
    let running_recovery = DriveExpiredOrchestrationJobs {
        shard: SafetyScanShard::whole(),
        after: None,
        limit: 1,
        slots: vec![ExpiredOrchestrationRecoverySlot {
            quota_entry_ids: ["7a20", "7a21", "7a22", "7a23"]
                .map(|suffix| id(&format!("qle_0198f1c3-9a00-7c3e-b1f3-773c2836{suffix}")))
                .to_vec(),
            run_event_id: id("evt_0198f1c3-9a00-7c3e-b1f3-773c28367a24"),
            run_outbox_id: id("obx_0198f1c3-9a00-7c3e-b1f3-773c28367a24"),
            node_event_id: id("evt_0198f1c3-9a00-7c3e-b1f3-773c28367a25"),
            node_outbox_id: id("obx_0198f1c3-9a00-7c3e-b1f3-773c28367a25"),
            job_event_id: id("evt_0198f1c3-9a00-7c3e-b1f3-773c28367a26"),
            job_outbox_id: id("obx_0198f1c3-9a00-7c3e-b1f3-773c28367a26"),
        }],
    };
    let mut scheduler = repository.begin_scheduler_transaction().await.unwrap();
    let recovered_running = scheduler
        .drive_expired_orchestration_jobs(running_recovery)
        .await
        .unwrap();
    scheduler.commit().await.unwrap();
    assert_eq!(recovered_running.len(), 1);
    assert_eq!(recovered_running[0].job.state, "retry_scheduled");
    assert_eq!(recovered_running[0].job.attempt_no, 1);
    assert_eq!(recovered_running[0].run.active_work_count, 0);
    let recovery_due = DriveDueOrchestrationRetries {
        shard: SafetyScanShard::whole(),
        after: None,
        limit: 1,
        slots: vec![DueOrchestrationRetrySlot {
            node_event_id: id("evt_0198f1c3-9a00-7c3e-b1f3-773c28367a27"),
            node_outbox_id: id("obx_0198f1c3-9a00-7c3e-b1f3-773c28367a27"),
            job_event_id: id("evt_0198f1c3-9a00-7c3e-b1f3-773c28367a28"),
            job_outbox_id: id("obx_0198f1c3-9a00-7c3e-b1f3-773c28367a28"),
        }],
    };
    let mut scheduler = repository.begin_scheduler_transaction().await.unwrap();
    assert!(scheduler
        .drive_due_orchestration_retries(recovery_due.clone())
        .await
        .unwrap()
        .is_empty());
    scheduler.commit().await.unwrap();
    tokio::time::sleep(std::time::Duration::from_millis(110)).await;
    let mut scheduler = repository.begin_scheduler_transaction().await.unwrap();
    let recovered_ready = scheduler
        .drive_due_orchestration_retries(recovery_due)
        .await
        .unwrap();
    scheduler.commit().await.unwrap();
    assert_eq!(recovered_ready.len(), 1);
    assert_eq!(recovered_ready[0].job.state, "ready");
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT reserved_value FROM insight_platform.quota_accounts WHERE tenant_id = $1 AND quota_account_id = $2",
        )
        .bind(TENANT_ID)
        .bind(QUOTA_ACCOUNT_ID)
        .fetch_one(&pool)
        .await
        .unwrap(),
        3
    );

    let terminal_command = admission(
        AdmissionIds {
            run: "run_0198f1c3-9a00-7c3e-b1f3-773c28367800",
            scope: "scp_0198f1c3-9a00-7c3e-b1f3-773c28367801",
            node: "nod_0198f1c3-9a00-7c3e-b1f3-773c28367802",
            job: "job_0198f1c3-9a00-7c3e-b1f3-773c28367803",
            value: "val_0198f1c3-9a00-7c3e-b1f3-773c28367804",
        },
        audit(TENANT_ID, PRINCIPAL_ID, "7805", 'a', 'b'),
        bindings.clone(),
        Utc::now() + Duration::minutes(5),
    );
    applied(run_command!(repository, admit_run, terminal_command.clone()).unwrap());
    let terminal_claim = orchestration_claim_with_ids(
        WORKER_D_ID,
        'c',
        "7806",
        ["7807", "7808", "7809", "780a"],
        ["780b", "780c", "780d"],
    );
    let mut scheduler = repository.begin_scheduler_transaction().await.unwrap();
    let claimed_terminal = scheduler
        .claim_orchestration_jobs(terminal_claim)
        .await
        .unwrap();
    scheduler.commit().await.unwrap();
    assert_eq!(claimed_terminal.len(), 1);
    assert_eq!(
        claimed_terminal[0].job.job_id,
        terminal_command.orchestration_job_id.to_string()
    );
    let start = StartOrchestrationJob {
        fence: JobFence {
            tenant_id: TENANT_ID.to_owned(),
            job_id: claimed_terminal[0].job.job_id.clone(),
            worker_id: id(WORKER_D_ID),
            lease_epoch: claimed_terminal[0].job.lease_epoch,
            expected_job_version: claimed_terminal[0].job.version,
            lease_token_digest: digest('c'),
        },
        receipt_id: id("rcp_0198f1c3-9a00-7c3e-b1f3-773c28367810"),
        idempotency_key_digest: digest('d'),
        request_digest: digest('e'),
        receipt_expires_at: Utc::now() + Duration::hours(1),
        job_event_id: id("evt_0198f1c3-9a00-7c3e-b1f3-773c28367811"),
        job_outbox_id: id("obx_0198f1c3-9a00-7c3e-b1f3-773c28367811"),
        node_event_id: id("evt_0198f1c3-9a00-7c3e-b1f3-773c28367812"),
        node_outbox_id: id("obx_0198f1c3-9a00-7c3e-b1f3-773c28367812"),
    };
    let mut scheduler = repository.begin_scheduler_transaction().await.unwrap();
    assert!(matches!(
        scheduler
            .start_orchestration_job(start.clone())
            .await
            .unwrap(),
        CommandOutcome::Applied(_)
    ));
    scheduler.rollback().await.unwrap();
    assert_eq!(
        sqlx::query_scalar::<_, String>(
            "SELECT state FROM insight_platform.jobs WHERE tenant_id = $1 AND job_id = $2",
        )
        .bind(TENANT_ID)
        .bind(&start.fence.job_id)
        .fetch_one(&pool)
        .await
        .unwrap(),
        "leased"
    );
    let mut scheduler = repository.begin_scheduler_transaction().await.unwrap();
    let started = match scheduler
        .start_orchestration_job(start.clone())
        .await
        .unwrap()
    {
        CommandOutcome::Applied(record) => record,
        CommandOutcome::Replayed(_) => panic!("first committed start must apply"),
    };
    scheduler.commit().await.unwrap();
    assert_eq!(started.state, "running");
    let mut scheduler = repository.begin_scheduler_transaction().await.unwrap();
    assert!(matches!(
        scheduler.start_orchestration_job(start.clone()).await.unwrap(),
        CommandOutcome::Replayed(record) if record.state == "running"
    ));
    scheduler.commit().await.unwrap();

    let mut scheduler = repository.begin_scheduler_transaction().await.unwrap();
    let heartbeat = scheduler
        .heartbeat_orchestration_job(HeartbeatJob {
            fence: JobFence {
                expected_job_version: started.version,
                ..start.fence.clone()
            },
            lease_milliseconds: 30_000,
        })
        .await
        .unwrap();
    scheduler.commit().await.unwrap();
    assert!(heartbeat.version > started.version);

    sqlx::query(
        r#"
        UPDATE insight_platform.run_nodes
        SET plan_node_key = 'input_otherwise', node_kind = 'return'
        WHERE tenant_id = $1 AND node_id = $2
          AND record_kind = 'node_execution' AND state = 'running'
        "#,
    )
    .bind(TENANT_ID)
    .bind(terminal_command.entry_node_execution_id.to_string())
    .execute(&pool)
    .await
    .unwrap();
    let output_json = match &terminal_command.input.value {
        ValueRef::Inline { value } => value.clone(),
        ValueRef::Artifact { .. } => panic!("terminal fixture input must be Inline"),
    };
    let complete = CommitPlanTerminal {
        fence: JobFence {
            expected_job_version: heartbeat.version,
            ..start.fence.clone()
        },
        plan: runtime_plan(),
        value: MaterializedTerminalValue {
            value_id: terminal_command.input.value_id.clone(),
            classification: terminal_command.input.classification,
            schema_digest: terminal_command.input.schema_digest.clone(),
            content_digest: terminal_command.input.content_digest.clone(),
            body: output_json,
        },
        idempotency_key_digest: digest('0'),
        request_digest: digest('1'),
        receipt_expires_at: Utc::now() + Duration::hours(1),
        mutations: OrchestrationTerminalMutationIds {
            receipt_id: id("rcp_0198f1c3-9a00-7c3e-b1f3-773c28367821"),
            quota_entry_ids: ["7822", "7823", "7824", "7825"]
                .map(|suffix| id(&format!("qle_0198f1c3-9a00-7c3e-b1f3-773c2836{suffix}")))
                .to_vec(),
            run_event_id: id("evt_0198f1c3-9a00-7c3e-b1f3-773c28367826"),
            run_outbox_id: id("obx_0198f1c3-9a00-7c3e-b1f3-773c28367826"),
            node_event_id: id("evt_0198f1c3-9a00-7c3e-b1f3-773c28367827"),
            node_outbox_id: id("obx_0198f1c3-9a00-7c3e-b1f3-773c28367827"),
            scope_closing_event_id: id("evt_0198f1c3-9a00-7c3e-b1f3-773c28367828"),
            scope_closing_outbox_id: id("obx_0198f1c3-9a00-7c3e-b1f3-773c28367828"),
            scope_terminal_event_id: id("evt_0198f1c3-9a00-7c3e-b1f3-773c28367829"),
            scope_terminal_outbox_id: id("obx_0198f1c3-9a00-7c3e-b1f3-773c28367829"),
            job_event_id: id("evt_0198f1c3-9a00-7c3e-b1f3-773c2836782a"),
            job_outbox_id: id("obx_0198f1c3-9a00-7c3e-b1f3-773c2836782a"),
        },
    };
    let mut scheduler = repository.begin_scheduler_transaction().await.unwrap();
    assert!(matches!(
        scheduler
            .commit_plan_terminal(complete.clone())
            .await
            .unwrap(),
        CommandOutcome::Applied(_)
    ));
    scheduler.rollback().await.unwrap();
    assert_eq!(
        sqlx::query_scalar::<_, String>(
            "SELECT state FROM insight_platform.jobs WHERE tenant_id = $1 AND job_id = $2",
        )
        .bind(TENANT_ID)
        .bind(&complete.fence.job_id)
        .fetch_one(&pool)
        .await
        .unwrap(),
        "running"
    );
    let mut scheduler = repository.begin_scheduler_transaction().await.unwrap();
    let completed = match scheduler
        .commit_plan_terminal(complete.clone())
        .await
        .unwrap()
    {
        CommandOutcome::Applied(record) => record,
        CommandOutcome::Replayed(_) => panic!("first terminal winner must apply"),
    };
    scheduler.commit().await.unwrap();
    assert_eq!(completed.run.state, "succeeded");
    assert_eq!(completed.run.active_work_count, 0);
    assert_eq!(completed.job.state, "succeeded");
    assert_eq!(completed.settled_quota_account_ids, vec![QUOTA_ACCOUNT_ID]);
    let public_result = repository
        .read_run_result_for_principal(
            &id(TENANT_ID),
            &id(DENIED_PRINCIPAL_ID),
            PrincipalKind::AgentRunner,
            &terminal_command.run_id,
        )
        .await
        .unwrap();
    assert_eq!(public_result.run_id, terminal_command.run_id);
    assert_eq!(public_result.value_id, complete.value.value_id);
    assert_eq!(
        public_result.value,
        ValueRef::Inline {
            value: complete.value.body.clone()
        }
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT reserved_value FROM insight_platform.quota_accounts WHERE tenant_id = $1 AND quota_account_id = $2",
        )
        .bind(TENANT_ID)
        .bind(QUOTA_ACCOUNT_ID)
        .fetch_one(&pool)
        .await
        .unwrap(),
        3
    );
    let mut scheduler = repository.begin_scheduler_transaction().await.unwrap();
    assert!(matches!(
        scheduler.commit_plan_terminal(complete.clone()).await.unwrap(),
        CommandOutcome::Replayed(record) if record.job.state == "succeeded"
    ));
    scheduler.commit().await.unwrap();
    let mut conflicting_complete = complete;
    conflicting_complete.request_digest = digest('2');
    let mut scheduler = repository.begin_scheduler_transaction().await.unwrap();
    assert!(matches!(
        scheduler
            .commit_plan_terminal(conflicting_complete)
            .await,
        Err(RepositoryError::IdempotencyConflict)
    ));
    scheduler.rollback().await.unwrap();

    let wait_command = admission(
        AdmissionIds {
            run: "run_0198f1c3-9a00-7c3e-b1f3-773c28367900",
            scope: "scp_0198f1c3-9a00-7c3e-b1f3-773c28367901",
            node: "nod_0198f1c3-9a00-7c3e-b1f3-773c28367902",
            job: "job_0198f1c3-9a00-7c3e-b1f3-773c28367903",
            value: "val_0198f1c3-9a00-7c3e-b1f3-773c28367904",
        },
        audit(TENANT_ID, PRINCIPAL_ID, "7905", '3', '4'),
        bindings.clone(),
        Utc::now() + Duration::minutes(5),
    );
    applied(run_command!(repository, admit_run, wait_command.clone()).unwrap());
    sqlx::query(
        r#"
        UPDATE insight_platform.run_nodes
        SET plan_node_key = 'timer_wait', node_kind = 'timer_wait'
        WHERE tenant_id = $1 AND node_id = $2
          AND record_kind = 'node_execution' AND state = 'ready'
        "#,
    )
    .bind(TENANT_ID)
    .bind(wait_command.entry_node_execution_id.to_string())
    .execute(&pool)
    .await
    .unwrap();
    let mut scheduler = repository.begin_scheduler_transaction().await.unwrap();
    let wait_claimed = scheduler
        .claim_orchestration_jobs(orchestration_claim_with_ids(
            WORKER_D_ID,
            'd',
            "7906",
            ["7907", "7908", "7909", "790a"],
            ["790b", "790c", "790d"],
        ))
        .await
        .unwrap();
    scheduler.commit().await.unwrap();
    assert_eq!(wait_claimed.len(), 1);
    assert_eq!(
        wait_claimed[0].job.job_id,
        wait_command.orchestration_job_id.to_string()
    );
    let wait_start = StartOrchestrationJob {
        fence: JobFence {
            tenant_id: TENANT_ID.to_owned(),
            job_id: wait_claimed[0].job.job_id.clone(),
            worker_id: id(WORKER_D_ID),
            lease_epoch: wait_claimed[0].job.lease_epoch,
            expected_job_version: wait_claimed[0].job.version,
            lease_token_digest: digest('d'),
        },
        receipt_id: id("rcp_0198f1c3-9a00-7c3e-b1f3-773c28367910"),
        idempotency_key_digest: digest('5'),
        request_digest: digest('6'),
        receipt_expires_at: Utc::now() + Duration::hours(1),
        job_event_id: id("evt_0198f1c3-9a00-7c3e-b1f3-773c28367911"),
        job_outbox_id: id("obx_0198f1c3-9a00-7c3e-b1f3-773c28367911"),
        node_event_id: id("evt_0198f1c3-9a00-7c3e-b1f3-773c28367934"),
        node_outbox_id: id("obx_0198f1c3-9a00-7c3e-b1f3-773c28367934"),
    };
    let mut scheduler = repository.begin_scheduler_transaction().await.unwrap();
    let wait_started = match scheduler
        .start_orchestration_job(wait_start.clone())
        .await
        .unwrap()
    {
        CommandOutcome::Applied(record) => record,
        CommandOutcome::Replayed(_) => panic!("first wait Job start must apply"),
    };
    scheduler.commit().await.unwrap();

    let wait_yield = YieldOrchestrationJob {
        fence: JobFence {
            expected_job_version: wait_started.version,
            ..wait_start.fence.clone()
        },
        outcome: OrchestrationYield::TimerWait {
            plan: runtime_plan(),
        },
        idempotency_key_digest: digest('8'),
        request_digest: digest('9'),
        receipt_expires_at: Utc::now() + Duration::hours(1),
        mutations: OrchestrationYieldMutationIds {
            receipt_id: id("rcp_0198f1c3-9a00-7c3e-b1f3-773c28367912"),
            quota_entry_ids: ["7913", "7914", "7915", "7916"]
                .map(|suffix| id(&format!("qle_0198f1c3-9a00-7c3e-b1f3-773c2836{suffix}")))
                .to_vec(),
            run_event_id: id("evt_0198f1c3-9a00-7c3e-b1f3-773c28367917"),
            run_outbox_id: id("obx_0198f1c3-9a00-7c3e-b1f3-773c28367917"),
            node_event_id: id("evt_0198f1c3-9a00-7c3e-b1f3-773c28367918"),
            node_outbox_id: id("obx_0198f1c3-9a00-7c3e-b1f3-773c28367918"),
            job_event_id: id("evt_0198f1c3-9a00-7c3e-b1f3-773c28367919"),
            job_outbox_id: id("obx_0198f1c3-9a00-7c3e-b1f3-773c28367919"),
        },
    };
    let mut scheduler = repository.begin_scheduler_transaction().await.unwrap();
    assert!(matches!(
        scheduler
            .yield_orchestration_job(wait_yield.clone())
            .await
            .unwrap(),
        CommandOutcome::Applied(_)
    ));
    scheduler.rollback().await.unwrap();
    assert_eq!(
        sqlx::query_scalar::<_, String>(
            "SELECT state FROM insight_platform.jobs WHERE tenant_id = $1 AND job_id = $2",
        )
        .bind(TENANT_ID)
        .bind(&wait_yield.fence.job_id)
        .fetch_one(&pool)
        .await
        .unwrap(),
        "running"
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT reserved_value FROM insight_platform.quota_accounts WHERE tenant_id = $1 AND quota_account_id = $2",
        )
        .bind(TENANT_ID)
        .bind(QUOTA_ACCOUNT_ID)
        .fetch_one(&pool)
        .await
        .unwrap(),
        4
    );
    let mut forged_timer = wait_yield.clone();
    let OrchestrationYield::TimerWait { plan } = &mut forged_timer.outcome else {
        unreachable!("fixture uses a Timer wait")
    };
    let RuntimeNode::TimerWait {
        delay_milliseconds,
        ..
    } = plan
        .nodes
        .get_mut(&PlanNodeKey::new("timer_wait".to_owned()).unwrap())
        .unwrap()
    else {
        unreachable!("fixture Timer node exists")
    };
    *delay_milliseconds += 1;
    let mut scheduler = repository.begin_scheduler_transaction().await.unwrap();
    assert!(matches!(
        scheduler.yield_orchestration_job(forged_timer).await,
        Err(RepositoryError::Conflict("exact typed Plan digest"))
    ));
    scheduler.rollback().await.unwrap();
    let mut scheduler = repository.begin_scheduler_transaction().await.unwrap();
    let waited = match scheduler
        .yield_orchestration_job(wait_yield.clone())
        .await
        .unwrap()
    {
        CommandOutcome::Applied(record) => record,
        CommandOutcome::Replayed(_) => panic!("first committed wait must apply"),
    };
    scheduler.commit().await.unwrap();
    assert_eq!(waited.job.state, "waiting");
    assert_eq!(waited.run.active_work_count, 0);
    assert_eq!(waited.settled_quota_account_ids, vec![QUOTA_ACCOUNT_ID]);
    assert_eq!(waited.job.wake_kind.as_deref(), Some("timer"));
    let mut scheduler = repository.begin_scheduler_transaction().await.unwrap();
    assert!(matches!(
        scheduler
            .yield_orchestration_job(wait_yield.clone())
            .await
            .unwrap(),
        CommandOutcome::Replayed(record) if record.job.state == "waiting"
    ));
    scheduler.commit().await.unwrap();

    let wake = WakeOrchestrationJob {
        tenant_id: id(TENANT_ID),
        job_id: id(&waited.job.job_id),
        expected_job_version: waited.job.version,
        expected_wake_generation: u64::try_from(waited.job.wake_generation).unwrap(),
        source: WakeSource::Timer,
        signal_key: None,
        signal_payload: None,
        idempotency_key_digest: digest('a'),
        request_digest: digest('b'),
        receipt_expires_at: Utc::now() + Duration::hours(1),
        mutations: OrchestrationWakeMutationIds {
            receipt_id: id("rcp_0198f1c3-9a00-7c3e-b1f3-773c2836791a"),
            node_event_id: id("evt_0198f1c3-9a00-7c3e-b1f3-773c2836791b"),
            node_outbox_id: id("obx_0198f1c3-9a00-7c3e-b1f3-773c2836791b"),
            job_event_id: id("evt_0198f1c3-9a00-7c3e-b1f3-773c2836791c"),
            job_outbox_id: id("obx_0198f1c3-9a00-7c3e-b1f3-773c2836791c"),
        },
    };
    let mut scheduler = repository.begin_scheduler_transaction().await.unwrap();
    assert!(matches!(
        scheduler.wake_orchestration_job(wake.clone()).await,
        Err(RepositoryError::Conflict("Timer is not due"))
    ));
    scheduler.rollback().await.unwrap();
    tokio::time::sleep(std::time::Duration::from_millis(250)).await;
    let due_wait = DriveDueOrchestrationWaits {
        shard: SafetyScanShard::whole(),
        after: None,
        limit: 1,
        slots: vec![DueOrchestrationWaitSlot {
            idempotency_key_digest: digest('a'),
            request_digest: digest('b'),
            mutations: OrchestrationWakeMutationIds {
                receipt_id: id("rcp_0198f1c3-9a00-7c3e-b1f3-773c28367940"),
                node_event_id: id("evt_0198f1c3-9a00-7c3e-b1f3-773c28367941"),
                node_outbox_id: id("obx_0198f1c3-9a00-7c3e-b1f3-773c28367941"),
                job_event_id: id("evt_0198f1c3-9a00-7c3e-b1f3-773c28367942"),
                job_outbox_id: id("obx_0198f1c3-9a00-7c3e-b1f3-773c28367942"),
            },
        }],
    };
    let mut scheduler = repository.begin_scheduler_transaction().await.unwrap();
    let due_woken = scheduler
        .drive_due_orchestration_waits(due_wait.clone())
        .await
        .unwrap();
    scheduler.commit().await.unwrap();
    assert_eq!(due_woken.len(), 1);
    let woken = &due_woken[0];
    assert_eq!(woken.job.state, "ready");
    assert!(woken.job.wake_kind.is_none());
    assert_eq!(
        sqlx::query_scalar::<_, Value>(
            "SELECT payload FROM insight_platform.run_nodes WHERE tenant_id = $1 AND node_id = $2",
        )
        .bind(TENANT_ID)
        .bind(&woken.node_id)
        .fetch_one(&pool)
        .await
        .unwrap()
        .pointer("/resolution"),
        Some(&json!("succeeded"))
    );
    let mut scheduler = repository.begin_scheduler_transaction().await.unwrap();
    assert!(scheduler
        .drive_due_orchestration_waits(due_wait)
        .await
        .unwrap()
        .is_empty());
    scheduler.commit().await.unwrap();
    let mut losing_wake = wake;
    losing_wake.idempotency_key_digest = digest('c');
    losing_wake.request_digest = digest('d');
    losing_wake.mutations.receipt_id = id("rcp_0198f1c3-9a00-7c3e-b1f3-773c28367933");
    let mut scheduler = repository.begin_scheduler_transaction().await.unwrap();
    assert!(matches!(
        scheduler.wake_orchestration_job(losing_wake).await,
        Err(RepositoryError::Conflict("orchestration wake first-winner"))
    ));
    scheduler.rollback().await.unwrap();

    let mut scheduler = repository.begin_scheduler_transaction().await.unwrap();
    let retry_claimed = scheduler
        .claim_orchestration_jobs(orchestration_claim_with_ids(
            WORKER_D_ID,
            'e',
            "791d",
            ["791e", "791f", "7920", "7921"],
            ["7922", "7923", "7924"],
        ))
        .await
        .unwrap();
    scheduler.commit().await.unwrap();
    assert_eq!(retry_claimed.len(), 1);
    assert_eq!(retry_claimed[0].job.job_id, waited.job.job_id);
    assert_eq!(retry_claimed[0].job.attempt_no, 1);
    let retry_start = StartOrchestrationJob {
        fence: JobFence {
            tenant_id: TENANT_ID.to_owned(),
            job_id: retry_claimed[0].job.job_id.clone(),
            worker_id: id(WORKER_D_ID),
            lease_epoch: retry_claimed[0].job.lease_epoch,
            expected_job_version: retry_claimed[0].job.version,
            lease_token_digest: digest('e'),
        },
        receipt_id: id("rcp_0198f1c3-9a00-7c3e-b1f3-773c28367925"),
        idempotency_key_digest: digest('e'),
        request_digest: digest('f'),
        receipt_expires_at: Utc::now() + Duration::hours(1),
        job_event_id: id("evt_0198f1c3-9a00-7c3e-b1f3-773c28367926"),
        job_outbox_id: id("obx_0198f1c3-9a00-7c3e-b1f3-773c28367926"),
        node_event_id: id("evt_0198f1c3-9a00-7c3e-b1f3-773c28367935"),
        node_outbox_id: id("obx_0198f1c3-9a00-7c3e-b1f3-773c28367935"),
    };
    let mut scheduler = repository.begin_scheduler_transaction().await.unwrap();
    let retry_started = match scheduler
        .start_orchestration_job(retry_start.clone())
        .await
        .unwrap()
    {
        CommandOutcome::Applied(record) => record,
        CommandOutcome::Replayed(_) => panic!("retry Job start must apply"),
    };
    scheduler.commit().await.unwrap();
    assert_eq!(retry_started.attempt_no, 2);
    let retry_at = Utc::now() + Duration::milliseconds(500);
    let retry_yield = YieldOrchestrationJob {
        fence: JobFence {
            expected_job_version: retry_started.version,
            ..retry_start.fence
        },
        outcome: OrchestrationYield::Retry { retry_at },
        idempotency_key_digest: digest('0'),
        request_digest: digest('1'),
        receipt_expires_at: Utc::now() + Duration::hours(1),
        mutations: OrchestrationYieldMutationIds {
            receipt_id: id("rcp_0198f1c3-9a00-7c3e-b1f3-773c28367927"),
            quota_entry_ids: ["7928", "7929", "792a", "792b"]
                .map(|suffix| id(&format!("qle_0198f1c3-9a00-7c3e-b1f3-773c2836{suffix}")))
                .to_vec(),
            run_event_id: id("evt_0198f1c3-9a00-7c3e-b1f3-773c2836792c"),
            run_outbox_id: id("obx_0198f1c3-9a00-7c3e-b1f3-773c2836792c"),
            node_event_id: id("evt_0198f1c3-9a00-7c3e-b1f3-773c2836792d"),
            node_outbox_id: id("obx_0198f1c3-9a00-7c3e-b1f3-773c2836792d"),
            job_event_id: id("evt_0198f1c3-9a00-7c3e-b1f3-773c2836792e"),
            job_outbox_id: id("obx_0198f1c3-9a00-7c3e-b1f3-773c2836792e"),
        },
    };
    let mut scheduler = repository.begin_scheduler_transaction().await.unwrap();
    let retried = match scheduler
        .yield_orchestration_job(retry_yield)
        .await
        .unwrap()
    {
        CommandOutcome::Applied(record) => record,
        CommandOutcome::Replayed(_) => panic!("retry schedule must apply"),
    };
    scheduler.commit().await.unwrap();
    assert_eq!(retried.job.state, "retry_scheduled");
    assert_eq!(retried.run.active_work_count, 0);
    let due_retry = DriveDueOrchestrationRetries {
        shard: SafetyScanShard::whole(),
        after: None,
        limit: 1,
        slots: vec![DueOrchestrationRetrySlot {
            node_event_id: id("evt_0198f1c3-9a00-7c3e-b1f3-773c28367931"),
            node_outbox_id: id("obx_0198f1c3-9a00-7c3e-b1f3-773c28367931"),
            job_event_id: id("evt_0198f1c3-9a00-7c3e-b1f3-773c28367932"),
            job_outbox_id: id("obx_0198f1c3-9a00-7c3e-b1f3-773c28367932"),
        }],
    };
    let mut scheduler = repository.begin_scheduler_transaction().await.unwrap();
    assert!(scheduler
        .drive_due_orchestration_retries(due_retry.clone())
        .await
        .unwrap()
        .is_empty());
    scheduler.commit().await.unwrap();
    tokio::time::sleep(std::time::Duration::from_millis(600)).await;
    let mut scheduler = repository.begin_scheduler_transaction().await.unwrap();
    assert_eq!(
        scheduler
            .drive_due_orchestration_retries(due_retry.clone())
            .await
            .unwrap()
            .len(),
        1
    );
    scheduler.rollback().await.unwrap();
    assert_eq!(
        sqlx::query_scalar::<_, String>(
            "SELECT state FROM insight_platform.jobs WHERE tenant_id = $1 AND job_id = $2",
        )
        .bind(TENANT_ID)
        .bind(&retried.job.job_id)
        .fetch_one(&pool)
        .await
        .unwrap(),
        "retry_scheduled"
    );
    let mut scheduler = repository.begin_scheduler_transaction().await.unwrap();
    let promoted = scheduler
        .drive_due_orchestration_retries(due_retry.clone())
        .await
        .unwrap();
    scheduler.commit().await.unwrap();
    assert_eq!(promoted.len(), 1);
    assert_eq!(promoted[0].job.state, "ready");
    let mut scheduler = repository.begin_scheduler_transaction().await.unwrap();
    assert!(scheduler
        .drive_due_orchestration_retries(due_retry)
        .await
        .unwrap()
        .is_empty());
    scheduler.commit().await.unwrap();
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT reserved_value FROM insight_platform.quota_accounts WHERE tenant_id = $1 AND quota_account_id = $2",
        )
        .bind(TENANT_ID)
        .bind(QUOTA_ACCOUNT_ID)
        .fetch_one(&pool)
        .await
        .unwrap(),
        3
    );

    let signal_command = admission(
        AdmissionIds {
            run: "run_0198f1c3-9a00-7c3e-b1f3-773c28368100",
            scope: "scp_0198f1c3-9a00-7c3e-b1f3-773c28368101",
            node: "nod_0198f1c3-9a00-7c3e-b1f3-773c28368102",
            job: "job_0198f1c3-9a00-7c3e-b1f3-773c28368103",
            value: "val_0198f1c3-9a00-7c3e-b1f3-773c28368104",
        },
        audit(TENANT_ID, PRINCIPAL_ID, "8105", '2', '3'),
        bindings.clone(),
        Utc::now() + Duration::minutes(5),
    );
    applied(run_command!(repository, admit_run, signal_command.clone()).unwrap());
    sqlx::query(
        r#"
        UPDATE insight_platform.run_nodes
        SET plan_node_key = 'signal_wait', node_kind = 'signal_wait'
        WHERE tenant_id = $1 AND node_id = $2
          AND record_kind = 'node_execution' AND state = 'ready'
        "#,
    )
    .bind(TENANT_ID)
    .bind(signal_command.entry_node_execution_id.to_string())
    .execute(&pool)
    .await
    .unwrap();
    // The monolithic fixture keeps earlier ready jobs alive for later assertions. Park them while
    // this isolated Signal lifecycle claims its own ready work item.
    sqlx::query(
        r#"
        UPDATE insight_platform.run_nodes AS node
        SET state = 'retry_scheduled', retry_at = clock_timestamp() + interval '1 hour'
        WHERE node.tenant_id = $1 AND node.state = 'ready'
          AND EXISTS (
              SELECT 1 FROM insight_platform.jobs AS job
              WHERE job.tenant_id = node.tenant_id AND job.node_id = node.node_id
                AND job.state = 'ready' AND job.job_id <> $2
          )
        "#,
    )
    .bind(TENANT_ID)
    .bind(signal_command.orchestration_job_id.to_string())
    .execute(&pool)
    .await
    .unwrap();
    sqlx::query(
        r#"
        UPDATE insight_platform.jobs
        SET state = 'retry_scheduled', retry_at = clock_timestamp() + interval '1 hour'
        WHERE tenant_id = $1 AND state = 'ready' AND job_id <> $2
          AND work_class = 'orchestration'
        "#,
    )
    .bind(TENANT_ID)
    .bind(signal_command.orchestration_job_id.to_string())
    .execute(&pool)
    .await
    .unwrap();
    let mut scheduler = repository.begin_scheduler_transaction().await.unwrap();
    let signal_claimed = scheduler
        .claim_orchestration_jobs(orchestration_claim_with_ids(
            WORKER_D_ID,
            '6',
            "8106",
            ["8107", "8108", "8109", "810a"],
            ["810b", "810c", "810d"],
        ))
        .await
        .unwrap();
    scheduler.commit().await.unwrap();
    assert_eq!(signal_claimed.len(), 1);
    let signal_start = StartOrchestrationJob {
        fence: JobFence {
            tenant_id: TENANT_ID.to_owned(),
            job_id: signal_claimed[0].job.job_id.clone(),
            worker_id: id(WORKER_D_ID),
            lease_epoch: signal_claimed[0].job.lease_epoch,
            expected_job_version: signal_claimed[0].job.version,
            lease_token_digest: digest('6'),
        },
        receipt_id: id("rcp_0198f1c3-9a00-7c3e-b1f3-773c2836810e"),
        idempotency_key_digest: digest('4'),
        request_digest: digest('5'),
        receipt_expires_at: Utc::now() + Duration::hours(1),
        job_event_id: id("evt_0198f1c3-9a00-7c3e-b1f3-773c2836810f"),
        job_outbox_id: id("obx_0198f1c3-9a00-7c3e-b1f3-773c2836810f"),
        node_event_id: id("evt_0198f1c3-9a00-7c3e-b1f3-773c28368110"),
        node_outbox_id: id("obx_0198f1c3-9a00-7c3e-b1f3-773c28368110"),
    };
    let mut scheduler = repository.begin_scheduler_transaction().await.unwrap();
    let signal_started = match scheduler
        .start_orchestration_job(signal_start.clone())
        .await
        .unwrap()
    {
        CommandOutcome::Applied(record) => record,
        CommandOutcome::Replayed(_) => panic!("Signal producer start must apply"),
    };
    scheduler.commit().await.unwrap();
    let signal_yield = YieldOrchestrationJob {
        fence: JobFence {
            expected_job_version: signal_started.version,
            ..signal_start.fence
        },
        outcome: OrchestrationYield::SignalWait {
            plan: runtime_plan(),
        },
        idempotency_key_digest: digest('6'),
        request_digest: digest('7'),
        receipt_expires_at: Utc::now() + Duration::hours(1),
        mutations: OrchestrationYieldMutationIds {
            receipt_id: id("rcp_0198f1c3-9a00-7c3e-b1f3-773c28368111"),
            quota_entry_ids: ["8112", "8113", "8114", "8115"]
                .map(|suffix| id(&format!("qle_0198f1c3-9a00-7c3e-b1f3-773c2836{suffix}")))
                .to_vec(),
            run_event_id: id("evt_0198f1c3-9a00-7c3e-b1f3-773c28368116"),
            run_outbox_id: id("obx_0198f1c3-9a00-7c3e-b1f3-773c28368116"),
            node_event_id: id("evt_0198f1c3-9a00-7c3e-b1f3-773c28368117"),
            node_outbox_id: id("obx_0198f1c3-9a00-7c3e-b1f3-773c28368117"),
            job_event_id: id("evt_0198f1c3-9a00-7c3e-b1f3-773c28368118"),
            job_outbox_id: id("obx_0198f1c3-9a00-7c3e-b1f3-773c28368118"),
        },
    };
    let mut scheduler = repository.begin_scheduler_transaction().await.unwrap();
    let signal_waited = match scheduler
        .yield_orchestration_job(signal_yield)
        .await
        .unwrap()
    {
        CommandOutcome::Applied(record) => record,
        CommandOutcome::Replayed(_) => panic!("Signal wait must apply"),
    };
    scheduler.commit().await.unwrap();
    assert_eq!(signal_waited.job.wake_kind.as_deref(), Some("signal"));
    let signal_body = json!({"released": true});
    let signal_payload = RunInputValue {
        value_id: id("val_0198f1c3-9a00-7c3e-b1f3-773c28368119"),
        classification: DataClassification::Internal,
        schema_digest: digest('7'),
        content_digest: canonical_digest(&signal_body).unwrap().parse().unwrap(),
        value: ValueRef::Inline { value: signal_body },
    };
    let mut signal_wake = WakeOrchestrationJob {
        tenant_id: id(TENANT_ID),
        job_id: id(&signal_waited.job.job_id),
        expected_job_version: signal_waited.job.version,
        expected_wake_generation: u64::try_from(signal_waited.job.wake_generation).unwrap(),
        source: WakeSource::Signal,
        signal_key: Some("forged".to_owned()),
        signal_payload: Some(signal_payload.clone()),
        idempotency_key_digest: digest('8'),
        request_digest: digest('9'),
        receipt_expires_at: Utc::now() + Duration::hours(1),
        mutations: OrchestrationWakeMutationIds {
            receipt_id: id("rcp_0198f1c3-9a00-7c3e-b1f3-773c2836811a"),
            node_event_id: id("evt_0198f1c3-9a00-7c3e-b1f3-773c2836811b"),
            node_outbox_id: id("obx_0198f1c3-9a00-7c3e-b1f3-773c2836811b"),
            job_event_id: id("evt_0198f1c3-9a00-7c3e-b1f3-773c2836811c"),
            job_outbox_id: id("obx_0198f1c3-9a00-7c3e-b1f3-773c2836811c"),
        },
    };
    let mut corrupt_signal_wake = signal_wake.clone();
    corrupt_signal_wake.signal_key = Some("release".to_owned());
    corrupt_signal_wake
        .signal_payload
        .as_mut()
        .unwrap()
        .content_digest = digest('0');
    let mut scheduler = repository.begin_scheduler_transaction().await.unwrap();
    assert!(matches!(
        scheduler
            .wake_orchestration_job(corrupt_signal_wake)
            .await,
        Err(RepositoryError::InvalidInput(_))
    ));
    scheduler.rollback().await.unwrap();
    let mut scheduler = repository.begin_scheduler_transaction().await.unwrap();
    assert!(matches!(
        scheduler.wake_orchestration_job(signal_wake.clone()).await,
        Err(RepositoryError::Conflict("Signal wake owner evidence"))
    ));
    scheduler.rollback().await.unwrap();
    signal_wake.signal_key = Some("release".to_owned());
    let mut scheduler = repository.begin_scheduler_transaction().await.unwrap();
    let signal_woken = match scheduler
        .wake_orchestration_job(signal_wake.clone())
        .await
        .unwrap()
    {
        CommandOutcome::Applied(record) => record,
        CommandOutcome::Replayed(_) => panic!("Signal wake must apply"),
    };
    scheduler.commit().await.unwrap();
    assert_eq!(signal_woken.job.state, "ready");
    let signal_node_payload: Value = sqlx::query_scalar(
        "SELECT payload FROM insight_platform.run_nodes WHERE tenant_id = $1 AND node_id = $2",
    )
    .bind(TENANT_ID)
    .bind(&signal_woken.node_id)
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(
        signal_node_payload.pointer("/resolution/outcome"),
        Some(&json!("succeeded"))
    );
    assert_eq!(
        signal_node_payload.pointer("/resolution/payload/value_id"),
        Some(&json!(signal_payload.value_id))
    );
    let signal_scope_payload: Value = sqlx::query_scalar(
        r#"
        SELECT scope.payload
        FROM insight_platform.run_nodes AS node
        JOIN insight_platform.run_nodes AS scope
          ON scope.tenant_id = node.tenant_id AND scope.node_id = node.scope_id
        WHERE node.tenant_id = $1 AND node.node_id = $2
        "#,
    )
    .bind(TENANT_ID)
    .bind(&signal_woken.node_id)
    .fetch_one(&pool)
    .await
    .unwrap();
    assert!(signal_scope_payload
        .pointer("/environment/bindings")
        .and_then(Value::as_array)
        .unwrap()
        .iter()
        .any(|binding| binding.pointer("/value/value_id") == Some(&json!(signal_payload.value_id))));
    let mut scheduler = repository.begin_scheduler_transaction().await.unwrap();
    assert!(matches!(
        scheduler
            .wake_orchestration_job(signal_wake)
            .await
            .unwrap(),
        CommandOutcome::Replayed(record) if record.job.state == "ready"
    ));
    scheduler.commit().await.unwrap();
    // Keep the completed Signal callback work out of the timeout fixture's claim.
    sqlx::query(
        r#"
        UPDATE insight_platform.jobs
        SET state = 'retry_scheduled', retry_at = clock_timestamp() + interval '1 hour'
        WHERE tenant_id = $1 AND job_id = $2 AND state = 'ready'
        "#,
    )
    .bind(TENANT_ID)
    .bind(&signal_woken.job.job_id)
    .execute(&pool)
    .await
    .unwrap();
    sqlx::query(
        r#"
        UPDATE insight_platform.run_nodes
        SET state = 'retry_scheduled', retry_at = clock_timestamp() + interval '1 hour'
        WHERE tenant_id = $1 AND node_id = $2 AND state = 'ready'
        "#,
    )
    .bind(TENANT_ID)
    .bind(&signal_woken.node_id)
    .execute(&pool)
    .await
    .unwrap();

    let timeout_command = admission(
        AdmissionIds {
            run: "run_0198f1c3-9a00-7c3e-b1f3-773c28368200",
            scope: "scp_0198f1c3-9a00-7c3e-b1f3-773c28368201",
            node: "nod_0198f1c3-9a00-7c3e-b1f3-773c28368202",
            job: "job_0198f1c3-9a00-7c3e-b1f3-773c28368203",
            value: "val_0198f1c3-9a00-7c3e-b1f3-773c28368204",
        },
        audit(TENANT_ID, PRINCIPAL_ID, "8205", '3', '4'),
        bindings.clone(),
        Utc::now() + Duration::minutes(5),
    );
    applied(run_command!(repository, admit_run, timeout_command.clone()).unwrap());
    sqlx::query(
        r#"
        UPDATE insight_platform.run_nodes
        SET plan_node_key = 'signal_timeout', node_kind = 'signal_wait'
        WHERE tenant_id = $1 AND node_id = $2
          AND record_kind = 'node_execution' AND state = 'ready'
        "#,
    )
    .bind(TENANT_ID)
    .bind(timeout_command.entry_node_execution_id.to_string())
    .execute(&pool)
    .await
    .unwrap();
    let mut scheduler = repository.begin_scheduler_transaction().await.unwrap();
    let timeout_claimed = scheduler
        .claim_orchestration_jobs(orchestration_claim_with_ids(
            WORKER_D_ID,
            '7',
            "8206",
            ["8207", "8208", "8209", "820a"],
            ["820b", "820c", "820d"],
        ))
        .await
        .unwrap();
    scheduler.commit().await.unwrap();
    assert_eq!(timeout_claimed.len(), 1);
    assert_eq!(
        timeout_claimed[0].job.job_id,
        timeout_command.orchestration_job_id.to_string()
    );
    let timeout_start = StartOrchestrationJob {
        fence: JobFence {
            tenant_id: TENANT_ID.to_owned(),
            job_id: timeout_claimed[0].job.job_id.clone(),
            worker_id: id(WORKER_D_ID),
            lease_epoch: timeout_claimed[0].job.lease_epoch,
            expected_job_version: timeout_claimed[0].job.version,
            lease_token_digest: digest('7'),
        },
        receipt_id: id("rcp_0198f1c3-9a00-7c3e-b1f3-773c2836820e"),
        idempotency_key_digest: digest('8'),
        request_digest: digest('9'),
        receipt_expires_at: Utc::now() + Duration::hours(1),
        job_event_id: id("evt_0198f1c3-9a00-7c3e-b1f3-773c2836820f"),
        job_outbox_id: id("obx_0198f1c3-9a00-7c3e-b1f3-773c2836820f"),
        node_event_id: id("evt_0198f1c3-9a00-7c3e-b1f3-773c28368210"),
        node_outbox_id: id("obx_0198f1c3-9a00-7c3e-b1f3-773c28368210"),
    };
    let mut scheduler = repository.begin_scheduler_transaction().await.unwrap();
    let timeout_started = match scheduler
        .start_orchestration_job(timeout_start.clone())
        .await
        .unwrap()
    {
        CommandOutcome::Applied(record) => record,
        CommandOutcome::Replayed(_) => panic!("Signal timeout start must apply"),
    };
    scheduler.commit().await.unwrap();
    let timeout_yield = YieldOrchestrationJob {
        fence: JobFence {
            expected_job_version: timeout_started.version,
            ..timeout_start.fence
        },
        outcome: OrchestrationYield::SignalWait {
            plan: runtime_plan(),
        },
        idempotency_key_digest: digest('a'),
        request_digest: digest('b'),
        receipt_expires_at: Utc::now() + Duration::hours(1),
        mutations: OrchestrationYieldMutationIds {
            receipt_id: id("rcp_0198f1c3-9a00-7c3e-b1f3-773c28368211"),
            quota_entry_ids: ["8212", "8213", "8214", "8215"]
                .map(|suffix| id(&format!("qle_0198f1c3-9a00-7c3e-b1f3-773c2836{suffix}")))
                .to_vec(),
            run_event_id: id("evt_0198f1c3-9a00-7c3e-b1f3-773c28368216"),
            run_outbox_id: id("obx_0198f1c3-9a00-7c3e-b1f3-773c28368216"),
            node_event_id: id("evt_0198f1c3-9a00-7c3e-b1f3-773c28368217"),
            node_outbox_id: id("obx_0198f1c3-9a00-7c3e-b1f3-773c28368217"),
            job_event_id: id("evt_0198f1c3-9a00-7c3e-b1f3-773c28368218"),
            job_outbox_id: id("obx_0198f1c3-9a00-7c3e-b1f3-773c28368218"),
        },
    };
    let mut scheduler = repository.begin_scheduler_transaction().await.unwrap();
    let timeout_waited = match scheduler
        .yield_orchestration_job(timeout_yield)
        .await
        .unwrap()
    {
        CommandOutcome::Applied(record) => record,
        CommandOutcome::Replayed(_) => panic!("Signal timeout wait must apply"),
    };
    scheduler.commit().await.unwrap();
    let due_timeout = DriveDueOrchestrationWaits {
        shard: SafetyScanShard::whole(),
        after: None,
        limit: 1,
        slots: vec![DueOrchestrationWaitSlot {
            idempotency_key_digest: digest('c'),
            request_digest: digest('d'),
            mutations: OrchestrationWakeMutationIds {
                receipt_id: id("rcp_0198f1c3-9a00-7c3e-b1f3-773c28368219"),
                node_event_id: id("evt_0198f1c3-9a00-7c3e-b1f3-773c2836821a"),
                node_outbox_id: id("obx_0198f1c3-9a00-7c3e-b1f3-773c2836821a"),
                job_event_id: id("evt_0198f1c3-9a00-7c3e-b1f3-773c2836821b"),
                job_outbox_id: id("obx_0198f1c3-9a00-7c3e-b1f3-773c2836821b"),
            },
        }],
    };
    let mut scheduler = repository.begin_scheduler_transaction().await.unwrap();
    assert!(scheduler
        .drive_due_orchestration_waits(due_timeout.clone())
        .await
        .unwrap()
        .is_empty());
    scheduler.commit().await.unwrap();
    tokio::time::sleep(std::time::Duration::from_millis(250)).await;
    let mut scheduler = repository.begin_scheduler_transaction().await.unwrap();
    let timeout_woken = scheduler
        .drive_due_orchestration_waits(due_timeout)
        .await
        .unwrap();
    scheduler.commit().await.unwrap();
    assert_eq!(timeout_woken.len(), 1);
    assert_eq!(timeout_woken[0].job.job_id, timeout_waited.job.job_id);
    let timeout_node_payload: Value = sqlx::query_scalar(
        "SELECT payload FROM insight_platform.run_nodes WHERE tenant_id = $1 AND node_id = $2",
    )
    .bind(TENANT_ID)
    .bind(&timeout_woken[0].node_id)
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(
        timeout_node_payload.pointer("/resolution/outcome"),
        Some(&json!("timed_out"))
    );
    assert_eq!(
        timeout_node_payload.pointer("/resolution/payload"),
        Some(&Value::Null)
    );
    sqlx::query(
        r#"
        UPDATE insight_platform.jobs
        SET state = 'ready', retry_at = NULL
        WHERE tenant_id = $1 AND state = 'retry_scheduled'
          AND retry_at > clock_timestamp() + interval '50 minutes'
        "#,
    )
    .bind(TENANT_ID)
    .execute(&pool)
    .await
    .unwrap();
    sqlx::query(
        r#"
        UPDATE insight_platform.run_nodes
        SET state = 'ready', retry_at = NULL
        WHERE tenant_id = $1 AND state = 'retry_scheduled'
          AND retry_at > clock_timestamp() + interval '50 minutes'
        "#,
    )
    .bind(TENANT_ID)
    .execute(&pool)
    .await
    .unwrap();
    // The Signal lifecycles are complete; keep resumed work out of unrelated later fixtures.
    sqlx::query(
        r#"
        UPDATE insight_platform.jobs
        SET state = 'retry_scheduled', retry_at = clock_timestamp() + interval '1 hour'
        WHERE tenant_id = $1 AND job_id IN ($2, $3) AND state = 'ready'
        "#,
    )
    .bind(TENANT_ID)
    .bind(&signal_woken.job.job_id)
    .bind(&timeout_woken[0].job.job_id)
    .execute(&pool)
    .await
    .unwrap();
    sqlx::query(
        r#"
        UPDATE insight_platform.run_nodes
        SET state = 'retry_scheduled', retry_at = clock_timestamp() + interval '1 hour'
        WHERE tenant_id = $1 AND node_id IN ($2, $3) AND state = 'ready'
        "#,
    )
    .bind(TENANT_ID)
    .bind(&signal_woken.node_id)
    .bind(&timeout_woken[0].node_id)
    .execute(&pool)
    .await
    .unwrap();

    let rollback = admission(
        AdmissionIds {
            run: "run_0198f1c3-9a00-7c3e-b1f3-773c28367100",
            scope: "scp_0198f1c3-9a00-7c3e-b1f3-773c28367101",
            node: "nod_0198f1c3-9a00-7c3e-b1f3-773c28367102",
            job: "job_0198f1c3-9a00-7c3e-b1f3-773c28367103",
            value: "val_0198f1c3-9a00-7c3e-b1f3-773c28367104",
        },
        audit(TENANT_ID, PRINCIPAL_ID, "7105", '1', '2'),
        bindings.clone(),
        Utc::now() + Duration::minutes(5),
    );
    let mut transaction = repository.begin_run_transaction().await.unwrap();
    assert!(matches!(
        transaction.admit_run(rollback.clone()).await.unwrap(),
        CommandOutcome::Applied(_)
    ));
    transaction.rollback().await.unwrap();
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT count(*) FROM insight_platform.runs WHERE tenant_id = $1 AND run_id = $2",
        )
        .bind(TENANT_ID)
        .bind(rollback.run_id.to_string())
        .fetch_one(&pool)
        .await
        .unwrap(),
        0
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT count(*) FROM insight_platform.receipts WHERE tenant_id = $1 AND receipt_id = $2",
        )
        .bind(TENANT_ID)
        .bind(rollback.audit.receipt_id.to_string())
        .fetch_one(&pool)
        .await
        .unwrap(),
        0
    );

    let command = admission(
        AdmissionIds {
            run: "run_0198f1c3-9a00-7c3e-b1f3-773c28367200",
            scope: "scp_0198f1c3-9a00-7c3e-b1f3-773c28367201",
            node: "nod_0198f1c3-9a00-7c3e-b1f3-773c28367202",
            job: "job_0198f1c3-9a00-7c3e-b1f3-773c28367203",
            value: "val_0198f1c3-9a00-7c3e-b1f3-773c28367204",
        },
        audit(TENANT_ID, PRINCIPAL_ID, "7205", '3', '4'),
        bindings.clone(),
        Utc::now() + Duration::minutes(5),
    );
    let admitted = applied(run_command!(repository, admit_run, command.clone()).unwrap());
    assert_eq!(admitted.state, "queued");
    assert_eq!(admitted.version, 1);
    assert_eq!(admitted.bindings, bindings);
    assert_eq!(
        repository
            .read_run_for_principal(
                &id(TENANT_ID),
                &id(DENIED_PRINCIPAL_ID),
                PrincipalKind::AgentRunner,
                &command.run_id,
            )
            .await
            .unwrap(),
        admitted
    );
    assert!(matches!(
        repository
            .read_run_for_principal(
                &id(TENANT_B_ID),
                &id(DENIED_PRINCIPAL_ID),
                PrincipalKind::AgentRunner,
                &command.run_id,
            )
            .await,
        Err(RepositoryError::PermissionDenied)
    ));
    assert_eq!(
        admitted.input_value_id.as_deref(),
        Some(command.input.value_id.to_string().as_str())
    );

    let atomic_counts = sqlx::query(
        r#"
        SELECT
          (SELECT count(*) FROM insight_platform.run_nodes WHERE tenant_id = $1 AND run_id = $2) AS nodes,
          (SELECT count(*) FROM insight_platform.run_values WHERE tenant_id = $1 AND run_id = $2) AS values,
          (SELECT count(*) FROM insight_platform.jobs WHERE tenant_id = $1 AND run_id = $2) AS jobs,
          (SELECT count(*) FROM insight_platform.events WHERE tenant_id = $1 AND aggregate_kind = 'run' AND aggregate_id = $2) AS events,
          (SELECT count(*) FROM insight_platform.receipts WHERE tenant_id = $1 AND scope_kind = 'run_admission' AND scope_id = $3 AND receipt_id = $4) AS receipts
        "#,
    )
    .bind(TENANT_ID)
    .bind(command.run_id.to_string())
    .bind(command.admission_scope_id.to_string())
    .bind(command.audit.receipt_id.to_string())
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(atomic_counts.try_get::<i64, _>("nodes").unwrap(), 2);
    assert_eq!(atomic_counts.try_get::<i64, _>("values").unwrap(), 1);
    assert_eq!(atomic_counts.try_get::<i64, _>("jobs").unwrap(), 1);
    assert_eq!(atomic_counts.try_get::<i64, _>("events").unwrap(), 1);
    assert_eq!(atomic_counts.try_get::<i64, _>("receipts").unwrap(), 1);

    assert!(matches!(
        run_command!(repository, admit_run, command.clone()).unwrap(),
        CommandOutcome::Replayed(record) if record.run_id == command.run_id.to_string()
    ));
    let mut conflicting = command.clone();
    conflicting.audit.request_digest = digest('5');
    assert!(matches!(
        run_command!(repository, admit_run, conflicting),
        Err(RepositoryError::IdempotencyConflict)
    ));

    let denied = admission(
        AdmissionIds {
            run: "run_0198f1c3-9a00-7c3e-b1f3-773c28367300",
            scope: "scp_0198f1c3-9a00-7c3e-b1f3-773c28367301",
            node: "nod_0198f1c3-9a00-7c3e-b1f3-773c28367302",
            job: "job_0198f1c3-9a00-7c3e-b1f3-773c28367303",
            value: "val_0198f1c3-9a00-7c3e-b1f3-773c28367304",
        },
        audit(TENANT_ID, DENIED_PRINCIPAL_ID, "7305", '6', '7'),
        bindings.clone(),
        Utc::now() + Duration::minutes(5),
    );
    assert!(matches!(
        run_command!(repository, admit_run, denied.clone()),
        Err(RepositoryError::PermissionDenied)
    ));
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT count(*) FROM insight_platform.receipts WHERE tenant_id = $1 AND receipt_id = $2",
        )
        .bind(TENANT_ID)
        .bind(denied.audit.receipt_id.to_string())
        .fetch_one(&pool)
        .await
        .unwrap(),
        0
    );

    let pause = SetRunPause {
        audit: audit(TENANT_ID, PRINCIPAL_ID, "7401", '8', '9'),
        run_id: command.run_id.clone(),
        expected_run_version: 1,
        expected_pause_generation: 0,
        requested: true,
    };
    let paused = applied(run_command!(repository, set_run_pause, pause.clone()).unwrap());
    assert_eq!(paused.version, 2);
    assert_eq!(paused.pause_generation, 1);
    assert!(paused.current.control.pause_requested);
    assert!(matches!(
        run_command!(repository, set_run_pause, pause).unwrap(),
        CommandOutcome::Replayed(record) if record.version == 2
    ));

    let noop_pause = SetRunPause {
        audit: audit(TENANT_ID, PRINCIPAL_ID, "7402", 'a', 'b'),
        run_id: command.run_id.clone(),
        expected_run_version: 2,
        expected_pause_generation: 1,
        requested: true,
    };
    let unchanged = applied(run_command!(repository, set_run_pause, noop_pause.clone()).unwrap());
    assert_eq!(unchanged.version, 2);
    assert_eq!(unchanged.pause_generation, 1);
    let aggregate_version: Option<i64> = sqlx::query_scalar(
        "SELECT aggregate_version FROM insight_platform.events WHERE tenant_id = $1 AND event_id = $2",
    )
    .bind(TENANT_ID)
    .bind(noop_pause.audit.event_id.to_string())
    .fetch_one(&pool)
    .await
    .unwrap();
    assert!(aggregate_version.is_none());

    let stale_pause = SetRunPause {
        audit: audit(TENANT_ID, PRINCIPAL_ID, "7403", 'c', 'd'),
        run_id: command.run_id.clone(),
        expected_run_version: 2,
        expected_pause_generation: 0,
        requested: false,
    };
    assert!(matches!(
        run_command!(repository, set_run_pause, stale_pause.clone()),
        Err(RepositoryError::Conflict("run control generation"))
    ));
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT count(*) FROM insight_platform.receipts WHERE tenant_id = $1 AND receipt_id = $2",
        )
        .bind(TENANT_ID)
        .bind(stale_pause.audit.receipt_id.to_string())
        .fetch_one(&pool)
        .await
        .unwrap(),
        0
    );

    let resumed = applied(
        run_command!(
            repository,
            set_run_pause,
            SetRunPause {
                audit: audit(TENANT_ID, PRINCIPAL_ID, "7404", 'e', 'f'),
                run_id: command.run_id.clone(),
                expected_run_version: 2,
                expected_pause_generation: 1,
                requested: false,
            }
        )
        .unwrap(),
    );
    assert_eq!(resumed.version, 3);
    assert_eq!(resumed.pause_generation, 2);
    assert!(!resumed.current.control.pause_requested);

    let cancelled = applied(
        run_command!(
            repository,
            request_run_cancel,
            RequestRunCancel {
                audit: audit(TENANT_ID, PRINCIPAL_ID, "7405", '0', '1'),
                run_id: command.run_id.clone(),
                expected_run_version: 3,
                expected_cancel_generation: 0,
                reason_code: "operator_request".to_owned(),
            }
        )
        .unwrap(),
    );
    assert_eq!(cancelled.state, "cancelling");
    assert_eq!(cancelled.version, 4);
    assert_eq!(cancelled.cancel_generation, 1);
    let public_events = repository
        .read_public_run_events_for_principal(
            &id(TENANT_ID),
            &id(DENIED_PRINCIPAL_ID),
            PrincipalKind::AgentRunner,
            &command.run_id,
            0,
            16,
        )
        .await
        .unwrap();
    assert_eq!(
        public_events
            .iter()
            .map(|event| (event.sequence, event.event_type))
            .collect::<Vec<_>>(),
        vec![
            (1, PublicRunEventType::RunQueued),
            (2, PublicRunEventType::RunPaused),
            (3, PublicRunEventType::RunResumed),
            (4, PublicRunEventType::RunCancelling),
        ]
    );
    assert_eq!(
        repository
            .read_public_run_events_for_principal(
                &id(TENANT_ID),
                &id(DENIED_PRINCIPAL_ID),
                PrincipalKind::AgentRunner,
                &command.run_id,
                2,
                1,
            )
            .await
            .unwrap()[0]
            .event_type,
        PublicRunEventType::RunResumed
    );
    assert!(matches!(
        repository
            .read_public_run_events_for_principal(
                &id(TENANT_B_ID),
                &id(DENIED_PRINCIPAL_ID),
                PrincipalKind::AgentRunner,
                &command.run_id,
                0,
                16,
            )
            .await,
        Err(RepositoryError::PermissionDenied)
    ));
    let public_replay = repository
        .read_root_run_admission_replay(
            &command.audit.tenant_id,
            &command.audit.principal_id,
            command.audit.principal_kind,
            &id(AGENT_ID),
            &command.audit.idempotency_key_digest,
            &command.audit.request_digest,
        )
        .await
        .unwrap()
        .unwrap();
    assert_eq!(public_replay.run_id, command.run_id.to_string());
    assert_eq!(public_replay.version, 1);
    assert_eq!(public_replay.state, "queued");

    let mut late_admission_replay = command.clone();
    late_admission_replay.run_id = id("run_0198f1c3-9a00-7c3e-b1f3-773c28367210");
    late_admission_replay.root_scope_id = id("scp_0198f1c3-9a00-7c3e-b1f3-773c28367211");
    late_admission_replay.entry_node_execution_id =
        id("nod_0198f1c3-9a00-7c3e-b1f3-773c28367212");
    late_admission_replay.orchestration_job_id =
        id("job_0198f1c3-9a00-7c3e-b1f3-773c28367213");
    late_admission_replay.input.value_id =
        id("val_0198f1c3-9a00-7c3e-b1f3-773c28367214");
    let unused_candidate_run_id = late_admission_replay.run_id.to_string();
    assert!(matches!(
        run_command!(repository, admit_run, late_admission_replay).unwrap(),
        CommandOutcome::Replayed(record)
            if record.run_id == command.run_id.to_string()
                && record.version == 1
                && record.state == "queued"
    ));
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT count(*) FROM insight_platform.runs WHERE tenant_id = $1 AND run_id = $2",
        )
        .bind(TENANT_ID)
        .bind(unused_candidate_run_id)
        .fetch_one(&pool)
        .await
        .unwrap(),
        0
    );

    let timeout_command = admission(
        AdmissionIds {
            run: "run_0198f1c3-9a00-7c3e-b1f3-773c28367500",
            scope: "scp_0198f1c3-9a00-7c3e-b1f3-773c28367501",
            node: "nod_0198f1c3-9a00-7c3e-b1f3-773c28367502",
            job: "job_0198f1c3-9a00-7c3e-b1f3-773c28367503",
            value: "val_0198f1c3-9a00-7c3e-b1f3-773c28367504",
        },
        audit(TENANT_ID, PRINCIPAL_ID, "7505", '2', '3'),
        bindings.clone(),
        Utc::now() + Duration::seconds(2),
    );
    applied(run_command!(repository, admit_run, timeout_command.clone()).unwrap());
    tokio::time::sleep(std::time::Duration::from_millis(2_100)).await;
    let timed_out = applied(
        run_command!(
            repository,
            observe_run_timeout,
            ObserveRunTimeout {
                audit: audit(TENANT_ID, PRINCIPAL_ID, "7506", '4', '5'),
                run_id: timeout_command.run_id,
                expected_run_version: 1,
                expected_timeout_generation: 0,
            }
        )
        .unwrap(),
    );
    assert_eq!(timed_out.state, "queued");
    assert_eq!(timed_out.version, 2);
    assert_eq!(timed_out.timeout_generation, 1);
    assert!(timed_out.terminal_at.is_none());
    assert!(timed_out.current.control.timeout_requested_at.unwrap() >= timed_out.deadline);

    let timeout_convergence = orchestration_convergence(
        ["7b00", "7b01", "7b02", "7b03"],
        ["7b04", "7b05", "7b06", "7b07", "7b08", "7bf0"],
    );
    let mut scheduler = repository.begin_scheduler_transaction().await.unwrap();
    let timed_out = scheduler
        .drive_orchestration_convergence(timeout_convergence)
        .await
        .unwrap();
    scheduler.commit().await.unwrap();
    assert_eq!(timed_out.len(), 1);
    assert_eq!(timed_out[0].completed.run.state, "timed_out");
    assert_eq!(timed_out[0].completed.job.state, "timed_out");
    assert_eq!(timed_out[0].completed.run.active_work_count, 0);
    assert!(timed_out[0].completed.settled_quota_account_ids.is_empty());
    let ready_cancel_convergence = orchestration_convergence(
        ["7b09", "7b0a", "7b0b", "7b0c"],
        ["7b0d", "7b0e", "7b0f", "7b10", "7b11", "7bf1"],
    );
    let mut scheduler = repository.begin_scheduler_transaction().await.unwrap();
    let ready_cancelled = scheduler
        .drive_orchestration_convergence(ready_cancel_convergence)
        .await
        .unwrap();
    scheduler.commit().await.unwrap();
    assert_eq!(ready_cancelled.len(), 1);
    assert_eq!(ready_cancelled[0].completed.run.state, "cancelled");
    assert_eq!(ready_cancelled[0].completed.job.state, "cancelled");
    assert!(ready_cancelled[0]
        .completed
        .settled_quota_account_ids
        .is_empty());

    let mut scheduler = repository.begin_scheduler_transaction().await.unwrap();
    let active_cancel_claimed = scheduler
        .claim_orchestration_jobs(orchestration_claim_with_ids(
            WORKER_D_ID,
            '3',
            "7b12",
            ["7b13", "7b14", "7b15", "7b16"],
            ["7b17", "7b18", "7b19"],
        ))
        .await
        .unwrap();
    scheduler.commit().await.unwrap();
    assert_eq!(active_cancel_claimed.len(), 1);
    assert_eq!(
        active_cancel_claimed[0].job.job_id,
        wait_command.orchestration_job_id.to_string()
    );
    let active_cancel_start = StartOrchestrationJob {
        fence: JobFence {
            tenant_id: TENANT_ID.to_owned(),
            job_id: active_cancel_claimed[0].job.job_id.clone(),
            worker_id: id(WORKER_D_ID),
            lease_epoch: active_cancel_claimed[0].job.lease_epoch,
            expected_job_version: active_cancel_claimed[0].job.version,
            lease_token_digest: digest('3'),
        },
        receipt_id: id("rcp_0198f1c3-9a00-7c3e-b1f3-773c28367b1a"),
        idempotency_key_digest: digest('4'),
        request_digest: digest('5'),
        receipt_expires_at: Utc::now() + Duration::hours(1),
        job_event_id: id("evt_0198f1c3-9a00-7c3e-b1f3-773c28367b1b"),
        job_outbox_id: id("obx_0198f1c3-9a00-7c3e-b1f3-773c28367b1b"),
        node_event_id: id("evt_0198f1c3-9a00-7c3e-b1f3-773c28367b1c"),
        node_outbox_id: id("obx_0198f1c3-9a00-7c3e-b1f3-773c28367b1c"),
    };
    let mut scheduler = repository.begin_scheduler_transaction().await.unwrap();
    let active_cancel_started = match scheduler
        .start_orchestration_job(active_cancel_start.clone())
        .await
        .unwrap()
    {
        CommandOutcome::Applied(record) => record,
        CommandOutcome::Replayed(_) => panic!("active cancellation start must apply"),
    };
    scheduler.commit().await.unwrap();
    assert_eq!(active_cancel_started.attempt_no, 3);
    let active_cancel_run_version: i64 = sqlx::query_scalar(
        "SELECT version FROM insight_platform.runs WHERE tenant_id = $1 AND run_id = $2",
    )
    .bind(TENANT_ID)
    .bind(wait_command.run_id.to_string())
    .fetch_one(&pool)
    .await
    .unwrap();
    let active_cancelled = applied(
        run_command!(
            repository,
            request_run_cancel,
            RequestRunCancel {
                audit: audit(TENANT_ID, PRINCIPAL_ID, "7b1d", '6', '7'),
                run_id: wait_command.run_id.clone(),
                expected_run_version: active_cancel_run_version,
                expected_cancel_generation: 0,
                reason_code: "operator_request".to_owned(),
            }
        )
        .unwrap(),
    );
    assert_eq!(active_cancelled.state, "cancelling");

    let losing_body = match &wait_command.input.value {
        ValueRef::Inline { value } => value.clone(),
        ValueRef::Artifact { .. } => panic!("cancellation fixture input must be Inline"),
    };
    let losing_result = CommitPlanTerminal {
        fence: JobFence {
            expected_job_version: active_cancel_started.version,
            ..active_cancel_start.fence
        },
        plan: runtime_plan(),
        value: MaterializedTerminalValue {
            value_id: wait_command.input.value_id.clone(),
            classification: wait_command.input.classification,
            schema_digest: wait_command.input.schema_digest.clone(),
            content_digest: wait_command.input.content_digest.clone(),
            body: losing_body,
        },
        idempotency_key_digest: digest('8'),
        request_digest: digest('9'),
        receipt_expires_at: Utc::now() + Duration::hours(1),
        mutations: OrchestrationTerminalMutationIds {
            receipt_id: id("rcp_0198f1c3-9a00-7c3e-b1f3-773c28367b1e"),
            quota_entry_ids: ["7b1f", "7b20", "7b21", "7b22"]
                .map(|suffix| id(&format!("qle_0198f1c3-9a00-7c3e-b1f3-773c2836{suffix}")))
                .to_vec(),
            run_event_id: id("evt_0198f1c3-9a00-7c3e-b1f3-773c28367b23"),
            run_outbox_id: id("obx_0198f1c3-9a00-7c3e-b1f3-773c28367b23"),
            node_event_id: id("evt_0198f1c3-9a00-7c3e-b1f3-773c28367b24"),
            node_outbox_id: id("obx_0198f1c3-9a00-7c3e-b1f3-773c28367b24"),
            scope_closing_event_id: id("evt_0198f1c3-9a00-7c3e-b1f3-773c28367b25"),
            scope_closing_outbox_id: id("obx_0198f1c3-9a00-7c3e-b1f3-773c28367b25"),
            scope_terminal_event_id: id("evt_0198f1c3-9a00-7c3e-b1f3-773c28367b26"),
            scope_terminal_outbox_id: id("obx_0198f1c3-9a00-7c3e-b1f3-773c28367b26"),
            job_event_id: id("evt_0198f1c3-9a00-7c3e-b1f3-773c28367b27"),
            job_outbox_id: id("obx_0198f1c3-9a00-7c3e-b1f3-773c28367b27"),
        },
    };
    let mut scheduler = repository.begin_scheduler_transaction().await.unwrap();
    assert!(matches!(
        scheduler.commit_plan_terminal(losing_result).await,
        Err(RepositoryError::Conflict("orchestration parent closure"))
    ));
    scheduler.rollback().await.unwrap();

    let mut scheduler = repository.begin_scheduler_transaction().await.unwrap();
    let converged_cancel = scheduler
        .drive_orchestration_convergence(orchestration_convergence(
            ["7b28", "7b29", "7b2a", "7b2b"],
            ["7b2c", "7b2d", "7b2e", "7b2f", "7b30", "7bf2"],
        ))
        .await
        .unwrap();
    scheduler.commit().await.unwrap();
    assert_eq!(converged_cancel.len(), 1);
    assert_eq!(converged_cancel[0].completed.run.state, "cancelled");
    assert_eq!(converged_cancel[0].completed.job.state, "cancelled");
    assert_eq!(
        converged_cancel[0].completed.settled_quota_account_ids,
        vec![QUOTA_ACCOUNT_ID]
    );

    let mut exhausted_command = admission(
        AdmissionIds {
            run: "run_0198f1c3-9a00-7c3e-b1f3-773c28367110",
            scope: "scp_0198f1c3-9a00-7c3e-b1f3-773c28367111",
            node: "nod_0198f1c3-9a00-7c3e-b1f3-773c28367112",
            job: "job_0198f1c3-9a00-7c3e-b1f3-773c28367113",
            value: "val_0198f1c3-9a00-7c3e-b1f3-773c28367114",
        },
        audit(TENANT_ID, PRINCIPAL_ID, "7c00", 'a', 'b'),
        wait_command.bindings.clone(),
        Utc::now() + Duration::minutes(2),
    );
    exhausted_command.attempt_limit = 1;
    applied(run_command!(repository, admit_run, exhausted_command.clone()).unwrap());
    let mut exhausted_claim = orchestration_claim_with_ids(
        WORKER_D_ID,
        'c',
        "7c01",
        ["7c02", "7c03", "7c04", "7c05"],
        ["7c06", "7c07", "7c08"],
    );
    exhausted_claim.lease_milliseconds = 100;
    let mut scheduler = repository.begin_scheduler_transaction().await.unwrap();
    let exhausted_claimed = scheduler
        .claim_orchestration_jobs(exhausted_claim)
        .await
        .unwrap();
    scheduler.commit().await.unwrap();
    assert_eq!(exhausted_claimed.len(), 1);
    assert_eq!(
        exhausted_claimed[0].job.job_id,
        exhausted_command.orchestration_job_id.to_string()
    );
    let exhausted_start = StartOrchestrationJob {
        fence: JobFence {
            tenant_id: TENANT_ID.to_owned(),
            job_id: exhausted_claimed[0].job.job_id.clone(),
            worker_id: id(WORKER_D_ID),
            lease_epoch: exhausted_claimed[0].job.lease_epoch,
            expected_job_version: exhausted_claimed[0].job.version,
            lease_token_digest: digest('c'),
        },
        receipt_id: id("rcp_0198f1c3-9a00-7c3e-b1f3-773c28367c09"),
        idempotency_key_digest: digest('d'),
        request_digest: digest('e'),
        receipt_expires_at: Utc::now() + Duration::hours(1),
        job_event_id: id("evt_0198f1c3-9a00-7c3e-b1f3-773c28367c0a"),
        job_outbox_id: id("obx_0198f1c3-9a00-7c3e-b1f3-773c28367c0a"),
        node_event_id: id("evt_0198f1c3-9a00-7c3e-b1f3-773c28367c0b"),
        node_outbox_id: id("obx_0198f1c3-9a00-7c3e-b1f3-773c28367c0b"),
    };
    let mut scheduler = repository.begin_scheduler_transaction().await.unwrap();
    let exhausted_started = match scheduler
        .start_orchestration_job(exhausted_start)
        .await
        .unwrap()
    {
        CommandOutcome::Applied(record) => record,
        CommandOutcome::Replayed(_) => panic!("exhausted generation start must apply"),
    };
    scheduler.commit().await.unwrap();
    assert_eq!(exhausted_started.attempt_no, 1);
    tokio::time::sleep(std::time::Duration::from_millis(150)).await;
    let mut scheduler = repository.begin_scheduler_transaction().await.unwrap();
    let exhausted = scheduler
        .drive_orchestration_convergence(orchestration_convergence(
            ["7c0c", "7c0d", "7c0e", "7c0f"],
            ["7c10", "7c11", "7c12", "7c13", "7c14", "7bf3"],
        ))
        .await
        .unwrap();
    scheduler.commit().await.unwrap();
    assert_eq!(exhausted.len(), 1);
    assert_eq!(exhausted[0].completed.run.state, "failed");
    assert_eq!(exhausted[0].completed.job.state, "failed");
    assert_eq!(exhausted[0].completed.job.attempt_no, 1);
    assert_eq!(
        exhausted[0].completed.settled_quota_account_ids,
        vec![QUOTA_ACCOUNT_ID]
    );

        macro_rules! task_first_winner_fixture {
            ($fixture_repository:ident, $fixture_pool:ident, $fixture_bindings:ident) => {{
                let repository: PgRepository = $fixture_repository;
                let pool: PgPool = $fixture_pool;
                let bindings: RunBindingsSnapshot = $fixture_bindings;
    let reserved_before_task: i64 = sqlx::query_scalar(
        "SELECT reserved_value FROM insight_platform.quota_accounts WHERE tenant_id = $1 AND quota_account_id = $2",
    )
    .bind(TENANT_ID)
    .bind(QUOTA_ACCOUNT_ID)
    .fetch_one(&pool)
    .await
    .unwrap();
    let mut scheduler = repository.begin_scheduler_transaction().await.unwrap();
    let task_claimed = scheduler
        .claim_orchestration_jobs(orchestration_claim_with_ids(
            WORKER_D_ID,
            '2',
            "7d06",
            ["7d07", "7d08", "7d09", "7d0a"],
            ["7d0b", "7d0c", "7d0d"],
        ))
        .await
        .unwrap();
    scheduler.commit().await.unwrap();
    assert_eq!(task_claimed.len(), 1);
    let selected_node_id = task_claimed[0]
        .job
        .node_id
        .clone()
        .expect("claimed orchestration Job has a Node");
    sqlx::query(
        r#"
        UPDATE insight_platform.run_nodes
        SET plan_node_key = 'human_task', node_kind = 'human_task'
        WHERE tenant_id = $1 AND node_id = $2
          AND record_kind = 'node_execution' AND state = 'ready'
        "#,
    )
    .bind(TENANT_ID)
    .bind(&selected_node_id)
    .execute(&pool)
    .await
    .unwrap();
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT reserved_value FROM insight_platform.quota_accounts WHERE tenant_id = $1 AND quota_account_id = $2",
        )
        .bind(TENANT_ID)
        .bind(QUOTA_ACCOUNT_ID)
        .fetch_one(&pool)
        .await
        .unwrap(),
        reserved_before_task + 1
    );
    let task_start = StartOrchestrationJob {
        fence: JobFence {
            tenant_id: TENANT_ID.to_owned(),
            job_id: task_claimed[0].job.job_id.clone(),
            worker_id: id(WORKER_D_ID),
            lease_epoch: task_claimed[0].job.lease_epoch,
            expected_job_version: task_claimed[0].job.version,
            lease_token_digest: digest('2'),
        },
        receipt_id: id("rcp_0198f1c3-9a00-7c3e-b1f3-773c28367d0e"),
        idempotency_key_digest: digest('3'),
        request_digest: digest('4'),
        receipt_expires_at: Utc::now() + Duration::hours(1),
        job_event_id: id("evt_0198f1c3-9a00-7c3e-b1f3-773c28367d0f"),
        job_outbox_id: id("obx_0198f1c3-9a00-7c3e-b1f3-773c28367d0f"),
        node_event_id: id("evt_0198f1c3-9a00-7c3e-b1f3-773c28367d10"),
        node_outbox_id: id("obx_0198f1c3-9a00-7c3e-b1f3-773c28367d10"),
    };
    let mut scheduler = repository.begin_scheduler_transaction().await.unwrap();
    let task_started = match scheduler
        .start_orchestration_job(task_start.clone())
        .await
        .unwrap()
    {
        CommandOutcome::Applied(record) => record,
        CommandOutcome::Replayed(_) => panic!("Task producer start must apply"),
    };
    scheduler.commit().await.unwrap();

    let task_id = id("int_0198f1c3-9a00-7c3e-b1f3-773c28367d11");
    let response_schema_digest = digest('5');
    let defer_task = DeferOrchestrationToTask {
        fence: JobFence {
            expected_job_version: task_started.version,
            ..task_start.fence
        },
        plan: runtime_plan(),
        task_id: task_id.clone(),
        definition: TaskDefinition::Interaction {
            interaction_kind: InteractionKind::Form,
            eligible_principal_rule_digest: digest('6'),
            safe_prompt_key: "provide_input".to_owned(),
        },
        response_schema_digest: Some(response_schema_digest.clone()),
        task_deadline: Utc::now() + Duration::seconds(30),
        idempotency_key_digest: digest('7'),
        request_digest: digest('8'),
        receipt_expires_at: Utc::now() + Duration::hours(1),
        mutations: DeferOrchestrationTaskMutationIds {
            receipt_id: id("rcp_0198f1c3-9a00-7c3e-b1f3-773c28367d12"),
            quota_entry_ids: ["7d13", "7d14", "7d15", "7d16"]
                .map(|suffix| id(&format!("qle_0198f1c3-9a00-7c3e-b1f3-773c2836{suffix}")))
                .to_vec(),
            run_event_id: id("evt_0198f1c3-9a00-7c3e-b1f3-773c28367d17"),
            run_outbox_id: id("obx_0198f1c3-9a00-7c3e-b1f3-773c28367d17"),
            node_event_id: id("evt_0198f1c3-9a00-7c3e-b1f3-773c28367d18"),
            node_outbox_id: id("obx_0198f1c3-9a00-7c3e-b1f3-773c28367d18"),
            task_event_id: id("evt_0198f1c3-9a00-7c3e-b1f3-773c28367d19"),
            task_outbox_id: id("obx_0198f1c3-9a00-7c3e-b1f3-773c28367d19"),
            job_event_id: id("evt_0198f1c3-9a00-7c3e-b1f3-773c28367d1a"),
            job_outbox_id: id("obx_0198f1c3-9a00-7c3e-b1f3-773c28367d1a"),
        },
    };
    let mut scheduler = repository.begin_scheduler_transaction().await.unwrap();
    let rolled_back_task = scheduler
        .defer_orchestration_to_task(defer_task.clone())
        .await
        .unwrap();
    assert!(matches!(rolled_back_task, CommandOutcome::Applied(_)));
    scheduler.rollback().await.unwrap();
    assert_eq!(
        sqlx::query_scalar::<_, String>(
            "SELECT state FROM insight_platform.jobs WHERE tenant_id = $1 AND job_id = $2",
        )
        .bind(TENANT_ID)
        .bind(&task_claimed[0].job.job_id)
        .fetch_one(&pool)
        .await
        .unwrap(),
        "running"
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT count(*) FROM insight_platform.tasks WHERE tenant_id = $1 AND task_id = $2",
        )
        .bind(TENANT_ID)
        .bind(task_id.to_string())
        .fetch_one(&pool)
        .await
        .unwrap(),
        0
    );
    let mut scheduler = repository.begin_scheduler_transaction().await.unwrap();
    let deferred = match scheduler
        .defer_orchestration_to_task(defer_task.clone())
        .await
        .unwrap()
    {
        CommandOutcome::Applied(record) => record,
        CommandOutcome::Replayed(_) => panic!("Task deferral must apply after rollback"),
    };
    scheduler.commit().await.unwrap();
    assert_eq!(deferred.job.state, "succeeded");
    assert_eq!(deferred.task.state, TaskState::Pending);
    assert_eq!(deferred.task.version, 1);
    let public_task = repository
        .read_task_for_principal(
            &id(TENANT_ID),
            &id(PRINCIPAL_ID),
            PrincipalKind::AgentRunner,
            &task_id,
        )
        .await
        .unwrap();
    assert_eq!(public_task.task_id, task_id.to_string());
    assert_eq!(public_task.state, TaskState::Pending);
    assert_eq!(deferred.run.state, "waiting");
    assert_eq!(deferred.run.active_work_count, 0);
    assert_eq!(deferred.node_id, selected_node_id.clone());
    assert_eq!(deferred.settled_quota_account_ids, vec![QUOTA_ACCOUNT_ID]);
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT reserved_value FROM insight_platform.quota_accounts WHERE tenant_id = $1 AND quota_account_id = $2",
        )
        .bind(TENANT_ID)
        .bind(QUOTA_ACCOUNT_ID)
        .fetch_one(&pool)
        .await
        .unwrap(),
        reserved_before_task
    );
    let mut scheduler = repository.begin_scheduler_transaction().await.unwrap();
    assert!(matches!(
        scheduler
            .defer_orchestration_to_task(defer_task.clone())
            .await
            .unwrap(),
        CommandOutcome::Replayed(record) if record.task.task_id == task_id.to_string()
    ));
    scheduler.commit().await.unwrap();

    let response_value: Value = json!({"answer": "approved"});
    let response = RunInputValue {
        value_id: id("val_0198f1c3-9a00-7c3e-b1f3-773c28367d1c"),
        classification: DataClassification::Internal,
        schema_digest: response_schema_digest.clone(),
        content_digest: canonical_digest(&response_value).unwrap().parse().unwrap(),
        value: ValueRef::Inline {
            value: response_value,
        },
    };
    let resolve_task = ResolveOrchestrationTask {
        audit: audit(TENANT_ID, PRINCIPAL_ID, "7d1b", '9', 'a'),
        task_id: task_id.clone(),
        expected_generation: 1,
        expected_task_version: 1,
        target: TaskState::Responded,
        response: Some(response.clone()),
        resume_job_id: id("job_0198f1c3-9a00-7c3e-b1f3-773c28367d1d"),
        resume_request_digest: digest('b'),
        mutations: ResolveOrchestrationTaskMutationIds {
            run_event_id: id("evt_0198f1c3-9a00-7c3e-b1f3-773c28367d1e"),
            run_outbox_id: id("obx_0198f1c3-9a00-7c3e-b1f3-773c28367d1e"),
            node_event_id: id("evt_0198f1c3-9a00-7c3e-b1f3-773c28367d1f"),
            node_outbox_id: id("obx_0198f1c3-9a00-7c3e-b1f3-773c28367d1f"),
            job_event_id: id("evt_0198f1c3-9a00-7c3e-b1f3-773c28367d20"),
            job_outbox_id: id("obx_0198f1c3-9a00-7c3e-b1f3-773c28367d20"),
        },
    };
    let mut scheduler = repository.begin_scheduler_transaction().await.unwrap();
    let resolved = match scheduler
        .resolve_orchestration_task(resolve_task.clone())
        .await
        .unwrap()
    {
        CommandOutcome::Applied(record) => record,
        CommandOutcome::Replayed(_) => panic!("Task response must apply"),
    };
    scheduler.commit().await.unwrap();
    assert_eq!(resolved.task.state, TaskState::Responded);
    assert_eq!(resolved.task.version, 2);
    assert_eq!(
        resolved.task.response_value_id.as_deref(),
        Some(response.value_id.to_string().as_str())
    );
    assert_eq!(resolved.run.state, "running");
    assert_eq!(resolved.run.active_work_count, 0);
    assert_eq!(resolved.job.state, "ready");
    assert_eq!(resolved.job.attempt_no, 0);
    assert_eq!(
        sqlx::query_scalar::<_, String>(
            "SELECT state FROM insight_platform.run_nodes WHERE tenant_id = $1 AND node_id = $2",
        )
        .bind(TENANT_ID)
        .bind(&selected_node_id)
        .fetch_one(&pool)
        .await
        .unwrap(),
        "ready"
    );
    let resolved_node_payload: Value = sqlx::query_scalar(
        "SELECT payload FROM insight_platform.run_nodes WHERE tenant_id = $1 AND node_id = $2",
    )
    .bind(TENANT_ID)
    .bind(&selected_node_id)
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(
        resolved_node_payload.pointer("/resolution/outcome"),
        Some(&json!("succeeded"))
    );
    assert_eq!(
        resolved_node_payload.pointer("/resolution/response/value_id"),
        Some(&json!(response.value_id))
    );
    let resolved_scope_payload: Value = sqlx::query_scalar(
        r#"
        SELECT scope.payload
        FROM insight_platform.run_nodes AS node
        JOIN insight_platform.run_nodes AS scope
          ON scope.tenant_id = node.tenant_id AND scope.node_id = node.scope_id
        WHERE node.tenant_id = $1 AND node.node_id = $2
        "#,
    )
    .bind(TENANT_ID)
    .bind(&selected_node_id)
    .fetch_one(&pool)
    .await
    .unwrap();
    assert!(resolved_scope_payload
        .pointer("/environment/bindings")
        .and_then(Value::as_array)
        .unwrap()
        .iter()
        .any(|binding| binding.pointer("/value/value_id") == Some(&json!(response.value_id))));
    let mut scheduler = repository.begin_scheduler_transaction().await.unwrap();
    assert!(matches!(
        scheduler
            .resolve_orchestration_task(resolve_task.clone())
            .await
            .unwrap(),
        CommandOutcome::Replayed(record) if record.task.state == TaskState::Responded
    ));
    scheduler.commit().await.unwrap();
    let mut scheduler = repository.begin_scheduler_transaction().await.unwrap();
    assert!(matches!(
        scheduler
            .defer_orchestration_to_task(defer_task.clone())
            .await
            .unwrap(),
        CommandOutcome::Replayed(record) if record.task.state == TaskState::Responded
    ));
    scheduler.commit().await.unwrap();

    let losing_task_response = ResolveOrchestrationTask {
        audit: audit(TENANT_ID, PRINCIPAL_ID, "7d21", 'c', 'd'),
        task_id: task_id.clone(),
        expected_generation: 1,
        expected_task_version: 1,
        target: TaskState::Declined,
        response: None,
        resume_job_id: id("job_0198f1c3-9a00-7c3e-b1f3-773c28367d22"),
        resume_request_digest: digest('e'),
        mutations: ResolveOrchestrationTaskMutationIds {
            run_event_id: id("evt_0198f1c3-9a00-7c3e-b1f3-773c28367d23"),
            run_outbox_id: id("obx_0198f1c3-9a00-7c3e-b1f3-773c28367d23"),
            node_event_id: id("evt_0198f1c3-9a00-7c3e-b1f3-773c28367d24"),
            node_outbox_id: id("obx_0198f1c3-9a00-7c3e-b1f3-773c28367d24"),
            job_event_id: id("evt_0198f1c3-9a00-7c3e-b1f3-773c28367d25"),
            job_outbox_id: id("obx_0198f1c3-9a00-7c3e-b1f3-773c28367d25"),
        },
    };
    let losing_receipt_id = losing_task_response.audit.receipt_id.to_string();
    let mut scheduler = repository.begin_scheduler_transaction().await.unwrap();
    assert!(matches!(
        scheduler
            .resolve_orchestration_task(losing_task_response)
            .await,
        Err(RepositoryError::Conflict("Task first-winner"))
    ));
    scheduler.rollback().await.unwrap();
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT count(*) FROM insight_platform.receipts WHERE tenant_id = $1 AND receipt_id = $2",
        )
        .bind(TENANT_ID)
        .bind(losing_receipt_id)
        .fetch_one(&pool)
        .await
        .unwrap(),
        0
    );

    sqlx::query(
        r#"
        UPDATE insight_platform.run_nodes
        SET plan_node_key = 'expiring_task', node_kind = 'human_task'
        WHERE tenant_id = $1 AND node_id = $2
          AND record_kind = 'node_execution' AND state = 'ready'
        "#,
    )
    .bind(TENANT_ID)
    .bind(&selected_node_id)
    .execute(&pool)
    .await
    .unwrap();
    let mut scheduler = repository.begin_scheduler_transaction().await.unwrap();
    let expiry_claimed = scheduler
        .claim_orchestration_jobs(orchestration_claim_with_ids(
            WORKER_D_ID,
            'f',
            "7d26",
            ["7d27", "7d28", "7d29", "7d2a"],
            ["7d2b", "7d2c", "7d2d"],
        ))
        .await
        .unwrap();
    scheduler.commit().await.unwrap();
    assert_eq!(expiry_claimed.len(), 1);
    let expiry_node_id = expiry_claimed[0]
        .job
        .node_id
        .clone()
        .expect("claimed orchestration Job has a Node");
    let expiry_start = StartOrchestrationJob {
        fence: JobFence {
            tenant_id: TENANT_ID.to_owned(),
            job_id: expiry_claimed[0].job.job_id.clone(),
            worker_id: id(WORKER_D_ID),
            lease_epoch: expiry_claimed[0].job.lease_epoch,
            expected_job_version: expiry_claimed[0].job.version,
            lease_token_digest: digest('f'),
        },
        receipt_id: id("rcp_0198f1c3-9a00-7c3e-b1f3-773c28367d2e"),
        idempotency_key_digest: digest('0'),
        request_digest: digest('1'),
        receipt_expires_at: Utc::now() + Duration::hours(1),
        job_event_id: id("evt_0198f1c3-9a00-7c3e-b1f3-773c28367d2f"),
        job_outbox_id: id("obx_0198f1c3-9a00-7c3e-b1f3-773c28367d2f"),
        node_event_id: id("evt_0198f1c3-9a00-7c3e-b1f3-773c28367d30"),
        node_outbox_id: id("obx_0198f1c3-9a00-7c3e-b1f3-773c28367d30"),
    };
    let mut scheduler = repository.begin_scheduler_transaction().await.unwrap();
    let expiry_started = match scheduler
        .start_orchestration_job(expiry_start.clone())
        .await
        .unwrap()
    {
        CommandOutcome::Applied(record) => record,
        CommandOutcome::Replayed(_) => panic!("expiry Task producer start must apply"),
    };
    scheduler.commit().await.unwrap();
    let expiring_task_id = id("int_0198f1c3-9a00-7c3e-b1f3-773c28367d31");
    let expiring_task = DeferOrchestrationToTask {
        fence: JobFence {
            expected_job_version: expiry_started.version,
            ..expiry_start.fence
        },
        plan: runtime_plan(),
        task_id: expiring_task_id.clone(),
        definition: TaskDefinition::Interaction {
            interaction_kind: InteractionKind::Form,
            eligible_principal_rule_digest: digest('2'),
            safe_prompt_key: "short_lived_input".to_owned(),
        },
        response_schema_digest: Some(digest('3')),
        task_deadline: Utc::now() + Duration::seconds(2),
        idempotency_key_digest: digest('4'),
        request_digest: digest('5'),
        receipt_expires_at: Utc::now() + Duration::hours(1),
        mutations: DeferOrchestrationTaskMutationIds {
            receipt_id: id("rcp_0198f1c3-9a00-7c3e-b1f3-773c28367d32"),
            quota_entry_ids: ["7d33", "7d34", "7d35", "7d36"]
                .map(|suffix| id(&format!("qle_0198f1c3-9a00-7c3e-b1f3-773c2836{suffix}")))
                .to_vec(),
            run_event_id: id("evt_0198f1c3-9a00-7c3e-b1f3-773c28367d37"),
            run_outbox_id: id("obx_0198f1c3-9a00-7c3e-b1f3-773c28367d37"),
            node_event_id: id("evt_0198f1c3-9a00-7c3e-b1f3-773c28367d38"),
            node_outbox_id: id("obx_0198f1c3-9a00-7c3e-b1f3-773c28367d38"),
            task_event_id: id("evt_0198f1c3-9a00-7c3e-b1f3-773c28367d39"),
            task_outbox_id: id("obx_0198f1c3-9a00-7c3e-b1f3-773c28367d39"),
            job_event_id: id("evt_0198f1c3-9a00-7c3e-b1f3-773c28367d3a"),
            job_outbox_id: id("obx_0198f1c3-9a00-7c3e-b1f3-773c28367d3a"),
        },
    };
    let mut scheduler = repository.begin_scheduler_transaction().await.unwrap();
    let expiring = match scheduler
        .defer_orchestration_to_task(expiring_task.clone())
        .await
        .unwrap()
    {
        CommandOutcome::Applied(record) => record,
        CommandOutcome::Replayed(_) => panic!("expiring Task deferral must apply"),
    };
    scheduler.commit().await.unwrap();
    assert_eq!(expiring.task.state, TaskState::Pending);
    assert_eq!(expiring.node_id, expiry_node_id);

    let mut scheduler = repository.begin_scheduler_transaction().await.unwrap();
    assert!(scheduler
        .drive_expired_orchestration_tasks(expired_task_scan(
            "7d3b",
            '6',
            ["7d3c", "7d3d", "7d3e", "7d3f"],
        ))
        .await
        .unwrap()
        .is_empty());
    scheduler.commit().await.unwrap();
    tokio::time::sleep(std::time::Duration::from_millis(2_100)).await;
    let mut scheduler = repository.begin_scheduler_transaction().await.unwrap();
    let expired = scheduler
        .drive_expired_orchestration_tasks(expired_task_scan(
            "7d40",
            '7',
            ["7d41", "7d42", "7d43", "7d44"],
        ))
        .await
        .unwrap();
    scheduler.commit().await.unwrap();
    assert_eq!(expired.len(), 1);
    assert_eq!(expired[0].task.state, TaskState::Expired);
    assert_eq!(expired[0].task.version, 2);
    assert_eq!(expired[0].run.state, "running");
    assert_eq!(expired[0].run.active_work_count, 0);
    assert_eq!(expired[0].job.state, "ready");
    assert_eq!(expired[0].node_id, expiry_node_id);

    let mut scheduler = repository.begin_scheduler_transaction().await.unwrap();
    assert!(scheduler
        .drive_expired_orchestration_tasks(expired_task_scan(
            "7d4a",
            '8',
            ["7d4b", "7d4c", "7d4d", "7d4e"],
        ))
        .await
        .unwrap()
        .is_empty());
    scheduler.commit().await.unwrap();
    let late_expiry_response = ResolveOrchestrationTask {
        audit: audit(TENANT_ID, PRINCIPAL_ID, "7d45", '9', 'a'),
        task_id: expiring_task_id.clone(),
        expected_generation: 1,
        expected_task_version: 1,
        target: TaskState::Declined,
        response: None,
        resume_job_id: id("job_0198f1c3-9a00-7c3e-b1f3-773c28367d46"),
        resume_request_digest: digest('b'),
        mutations: ResolveOrchestrationTaskMutationIds {
            run_event_id: id("evt_0198f1c3-9a00-7c3e-b1f3-773c28367d47"),
            run_outbox_id: id("obx_0198f1c3-9a00-7c3e-b1f3-773c28367d47"),
            node_event_id: id("evt_0198f1c3-9a00-7c3e-b1f3-773c28367d48"),
            node_outbox_id: id("obx_0198f1c3-9a00-7c3e-b1f3-773c28367d48"),
            job_event_id: id("evt_0198f1c3-9a00-7c3e-b1f3-773c28367d49"),
            job_outbox_id: id("obx_0198f1c3-9a00-7c3e-b1f3-773c28367d49"),
        },
    };
    let late_expiry_receipt_id = late_expiry_response.audit.receipt_id.to_string();
    let mut scheduler = repository.begin_scheduler_transaction().await.unwrap();
    assert!(matches!(
        scheduler
            .resolve_orchestration_task(late_expiry_response)
            .await,
        Err(RepositoryError::Conflict("Task first-winner"))
    ));
    scheduler.rollback().await.unwrap();
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT count(*) FROM insight_platform.receipts WHERE tenant_id = $1 AND receipt_id = $2",
        )
        .bind(TENANT_ID)
        .bind(late_expiry_receipt_id)
        .fetch_one(&pool)
        .await
        .unwrap(),
        0
    );
    let mut scheduler = repository.begin_scheduler_transaction().await.unwrap();
    assert!(matches!(
        scheduler
            .defer_orchestration_to_task(expiring_task)
            .await
            .unwrap(),
        CommandOutcome::Replayed(record) if record.task.state == TaskState::Expired
    ));
    scheduler.commit().await.unwrap();

    // This repository test exercises the child owner transaction in isolation. Production Plan
    // driving creates a distinct ChildAgentCall Node; move the recycled fixture Node onto that
    // exact Plan identity before claiming its next orchestration Job.
    sqlx::query(
        r#"
        UPDATE insight_platform.run_nodes
        SET plan_node_key = 'child_call', node_kind = 'child_agent_call'
        WHERE tenant_id = $1 AND node_id = $2
          AND record_kind = 'node_execution' AND state = 'ready'
        "#,
    )
    .bind(TENANT_ID)
    .bind(&expiry_node_id)
    .execute(&pool)
    .await
    .unwrap();

    let reserved_before_child: i64 = sqlx::query_scalar(
        "SELECT reserved_value FROM insight_platform.quota_accounts WHERE tenant_id = $1 AND quota_account_id = $2",
    )
    .bind(TENANT_ID)
    .bind(QUOTA_ACCOUNT_ID)
    .fetch_one(&pool)
    .await
    .unwrap();
    let mut scheduler = repository.begin_scheduler_transaction().await.unwrap();
    let child_claimed = scheduler
        .claim_orchestration_jobs(orchestration_claim_with_ids(
            WORKER_D_ID,
            'a',
            "7e30",
            ["7e31", "7e32", "7e33", "7e34"],
            ["7e35", "7e36", "7e37"],
        ))
        .await
        .unwrap();
    scheduler.commit().await.unwrap();
    assert_eq!(child_claimed.len(), 1);
    assert_eq!(child_claimed[0].job.job_id, expired[0].job.job_id);
    let child_start = StartOrchestrationJob {
        fence: JobFence {
            tenant_id: TENANT_ID.to_owned(),
            job_id: child_claimed[0].job.job_id.clone(),
            worker_id: id(WORKER_D_ID),
            lease_epoch: child_claimed[0].job.lease_epoch,
            expected_job_version: child_claimed[0].job.version,
            lease_token_digest: digest('a'),
        },
        receipt_id: id("rcp_0198f1c3-9a00-7c3e-b1f3-773c28367e38"),
        idempotency_key_digest: digest('b'),
        request_digest: digest('c'),
        receipt_expires_at: Utc::now() + Duration::hours(1),
        job_event_id: id("evt_0198f1c3-9a00-7c3e-b1f3-773c28367e39"),
        job_outbox_id: id("obx_0198f1c3-9a00-7c3e-b1f3-773c28367e39"),
        node_event_id: id("evt_0198f1c3-9a00-7c3e-b1f3-773c28367e3a"),
        node_outbox_id: id("obx_0198f1c3-9a00-7c3e-b1f3-773c28367e3a"),
    };
    let mut scheduler = repository.begin_scheduler_transaction().await.unwrap();
    let child_started = match scheduler
        .start_orchestration_job(child_start.clone())
        .await
        .unwrap()
    {
        CommandOutcome::Applied(record) => record,
        CommandOutcome::Replayed(_) => panic!("child producer start must apply"),
    };
    scheduler.commit().await.unwrap();

    let child_facts_fence = JobFence {
        expected_job_version: child_started.version,
        ..child_start.fence.clone()
    };
    let child_dispatch_facts = repository
        .load_controller_facts(&child_facts_fence, &runtime_plan())
        .await
        .unwrap();
    assert!(matches!(
        child_dispatch_facts,
        ControllerFacts::ChildAgentDispatch(ref facts)
            if facts.input.run_value_id.to_string()
                == expired[0].run.input_value_id.as_deref().unwrap()
                && facts.route.is_none()
                && facts.candidates.len() == 1
    ));

    let child_deployment_digest: String = sqlx::query_scalar(
        "SELECT bindings_digest FROM insight_platform.deployments WHERE tenant_id = $1 AND deployment_id = $2",
    )
    .bind(TENANT_ID)
    .bind(CHILD_AGENT_DEPLOYMENT_ID)
    .fetch_one(&pool)
    .await
    .unwrap();
    let child_input: Value = json!({"question": "select one"});
    let child_input_digest: Sha256Digest =
        canonical_digest(&child_input).unwrap().parse().unwrap();
    let (selection_policy, selection_candidates) = match &bindings
        .slots
        .iter()
        .find(|slot| slot.slot_id == "child_worker")
        .unwrap()
        .target
    {
        FrozenSlotTarget::ChildAgent {
            candidates,
            selection_policy,
        } => (selection_policy, candidates),
        _ => panic!("child_worker must be a child Agent slot"),
    };
    let selected_child_deployment = ExactDeploymentRef::new(
        id(CHILD_AGENT_DEPLOYMENT_ID),
        child_deployment_digest.parse().unwrap(),
    )
    .unwrap();
    let selection_evidence = derive_candidate_selection(
        "child_worker",
        selection_policy,
        &CandidateSelectionPolicyDocument {
            schema_version: 1,
            mode: CandidateSelectionMode::OnlyCandidate,
            route_schema_digest: None,
        },
        selection_candidates,
        None,
    )
    .unwrap();
    let parent_input_value_id: ResourceId = expired[0]
        .run
        .input_value_id
        .as_deref()
        .unwrap()
        .parse()
        .unwrap();
    let mutation_ids = |receipt_suffix: &str,
                        quota_suffixes: [&str; 4],
                        event_suffixes: [&str; 7]| {
        let platform_id = |prefix: &str, suffix: &str| {
            id(&format!(
                "{prefix}_0198f1c3-9a00-7c3e-b1f3-773c2836{suffix}"
            ))
        };
        DeferOrchestrationChildMutationIds {
            receipt_id: platform_id("rcp", receipt_suffix),
            quota_entry_ids: quota_suffixes
                .map(|suffix| platform_id("qle", suffix))
                .to_vec(),
            root_run_event_id: platform_id("evt", event_suffixes[0]),
            root_run_outbox_id: platform_id("obx", event_suffixes[0]),
            parent_run_event_id: platform_id("evt", event_suffixes[1]),
            parent_run_outbox_id: platform_id("obx", event_suffixes[1]),
            parent_node_event_id: platform_id("evt", event_suffixes[2]),
            parent_node_outbox_id: platform_id("obx", event_suffixes[2]),
            parent_job_event_id: platform_id("evt", event_suffixes[3]),
            parent_job_outbox_id: platform_id("obx", event_suffixes[3]),
            child_link_event_id: platform_id("evt", event_suffixes[4]),
            child_link_outbox_id: platform_id("obx", event_suffixes[4]),
            child_run_event_id: platform_id("evt", event_suffixes[5]),
            child_run_outbox_id: platform_id("obx", event_suffixes[5]),
            child_job_event_id: platform_id("evt", event_suffixes[6]),
            child_job_outbox_id: platform_id("obx", event_suffixes[6]),
        }
    };
    let defer_child = DeferOrchestrationToChildRun {
        fence: JobFence {
            expected_job_version: child_started.version,
            ..child_start.fence
        },
        plan: runtime_plan(),
        slot_id: "child_worker".to_owned(),
        selected_child_deployment,
        selection_evidence,
        materialized_route: None,
        child_link_id: id("crun_0198f1c3-9a00-7c3e-b1f3-773c28367e01"),
        child_run_id: id("run_0198f1c3-9a00-7c3e-b1f3-773c28367e02"),
        child_root_scope_id: id("scp_0198f1c3-9a00-7c3e-b1f3-773c28367e03"),
        child_entry_node_execution_id: id("nod_0198f1c3-9a00-7c3e-b1f3-773c28367e04"),
        child_orchestration_job_id: id("job_0198f1c3-9a00-7c3e-b1f3-773c28367e05"),
        input: RunInputValue {
            value_id: id("val_0198f1c3-9a00-7c3e-b1f3-773c28367e06"),
            classification: DataClassification::Internal,
            schema_digest: agent_schema().canonical_digest,
            content_digest: child_input_digest.clone(),
            value: ValueRef::Inline { value: child_input },
        },
        source_value_ids: vec![parent_input_value_id],
        budget: ChildBudget {
            deadline: Utc::now() + Duration::seconds(30),
            maximum_model_tokens: 1_000,
            maximum_capability_calls: 10,
            maximum_artifact_bytes: 1_048_576,
            maximum_descendant_runs: 8,
        },
        cancellation_policy: ChildCancellationPolicy::CascadeAndWait,
        logical_key: "child:expiry-node:attempt-1".to_owned(),
        child_attempt_limit: 3,
        child_retry_backoff_milliseconds: 100,
        idempotency_key_digest: digest('d'),
        request_digest: digest('e'),
        receipt_expires_at: Utc::now() + Duration::hours(1),
        mutations: mutation_ids(
            "7e3b",
            ["7e3c", "7e3d", "7e3e", "7e3f"],
            ["7e40", "7e41", "7e42", "7e43", "7e44", "7e45", "7e46"],
        ),
    };

    let mut invalid_child = defer_child.clone();
    invalid_child.input.schema_digest = digest('0');
    invalid_child.mutations = mutation_ids(
        "7e20",
        ["7e21", "7e22", "7e23", "7e24"],
        ["7e25", "7e26", "7e27", "7e28", "7e29", "7e2a", "7e2b"],
    );
    invalid_child.idempotency_key_digest = digest('f');
    invalid_child.request_digest = digest('0');
    let invalid_receipt_id = invalid_child.mutations.receipt_id.to_string();
    let mut scheduler = repository.begin_scheduler_transaction().await.unwrap();
    let invalid_child_error = scheduler
        .defer_orchestration_to_child_run(invalid_child)
        .await
        .unwrap_err();
    assert!(
        matches!(
            invalid_child_error,
            RepositoryError::Conflict("child input RunValue evidence")
        ),
        "unexpected invalid child error: {invalid_child_error:?}"
    );
    scheduler.rollback().await.unwrap();
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT count(*) FROM insight_platform.receipts WHERE tenant_id = $1 AND receipt_id = $2",
        )
        .bind(TENANT_ID)
        .bind(invalid_receipt_id)
        .fetch_one(&pool)
        .await
        .unwrap(),
        0
    );

    let parent_deployment_digest: String = sqlx::query_scalar(
        "SELECT bindings_digest FROM insight_platform.deployments WHERE tenant_id = $1 AND deployment_id = $2",
    )
    .bind(TENANT_ID)
    .bind(AGENT_DEPLOYMENT_ID)
    .fetch_one(&pool)
    .await
    .unwrap();
    let mut invalid_candidate = defer_child.clone();
    invalid_candidate.selected_child_deployment = ExactDeploymentRef::new(
        id(AGENT_DEPLOYMENT_ID),
        parent_deployment_digest.parse().unwrap(),
    )
    .unwrap();
    invalid_candidate.selection_evidence.selected_deployment =
        invalid_candidate.selected_child_deployment.clone();
    invalid_candidate.idempotency_key_digest = digest('1');
    invalid_candidate.request_digest = digest('2');
    let mut scheduler = repository.begin_scheduler_transaction().await.unwrap();
    let invalid_candidate_error = scheduler
        .defer_orchestration_to_child_run(invalid_candidate)
        .await
        .unwrap_err();
    assert!(
        matches!(
            invalid_candidate_error,
            RepositoryError::Conflict("exact child candidate selection")
        ),
        "unexpected invalid candidate error: {invalid_candidate_error:?}"
    );
    scheduler.rollback().await.unwrap();

    let mut declassified_child = defer_child.clone();
    declassified_child.input.classification = DataClassification::Public;
    declassified_child.idempotency_key_digest = digest('3');
    declassified_child.request_digest = digest('4');
    let mut scheduler = repository.begin_scheduler_transaction().await.unwrap();
    assert!(matches!(
        scheduler
            .defer_orchestration_to_child_run(declassified_child)
            .await,
        Err(RepositoryError::Conflict("child input RunValue evidence"))
    ));
    scheduler.rollback().await.unwrap();

    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT count(*) FROM insight_platform.runs WHERE tenant_id = $1 AND run_id = $2",
        )
        .bind(TENANT_ID)
        .bind(defer_child.child_run_id.to_string())
        .fetch_one(&pool)
        .await
        .unwrap(),
        0
    );

    let mut scheduler = repository.begin_scheduler_transaction().await.unwrap();
    let deferred_child = match scheduler
        .defer_orchestration_to_child_run(defer_child.clone())
        .await
        .unwrap()
    {
        CommandOutcome::Applied(record) => record,
        CommandOutcome::Replayed(_) => panic!("child deferral must apply"),
    };
    scheduler.commit().await.unwrap();
    assert_eq!(deferred_child.parent_run.state, "waiting");
    assert_eq!(deferred_child.parent_run.active_work_count, 0);
    assert_eq!(deferred_child.root_run.descendant_count, 1);
    assert_eq!(deferred_child.parent_job.state, "succeeded");
    assert_eq!(deferred_child.child_link.state.as_str(), "running");
    assert_eq!(deferred_child.child_run.state, "queued");
    assert_eq!(deferred_child.child_run.depth, 1);
    assert_eq!(
        deferred_child.child_run.parent_run_id.as_deref(),
        Some(deferred_child.parent_run.run_id.as_str())
    );
    assert_eq!(deferred_child.child_job.state, "ready");
    assert_eq!(
        deferred_child.settled_quota_account_ids,
        vec![QUOTA_ACCOUNT_ID]
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT reserved_value FROM insight_platform.quota_accounts WHERE tenant_id = $1 AND quota_account_id = $2",
        )
        .bind(TENANT_ID)
        .bind(QUOTA_ACCOUNT_ID)
        .fetch_one(&pool)
        .await
        .unwrap(),
        reserved_before_child
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT count(*) FROM insight_platform.events WHERE tenant_id = $1 AND event_id = ANY($2)",
        )
        .bind(TENANT_ID)
        .bind(
            ["7e41", "7e42", "7e43", "7e44", "7e45", "7e46"]
                .map(|suffix| {
                    format!("evt_0198f1c3-9a00-7c3e-b1f3-773c2836{suffix}")
                })
                .to_vec(),
        )
        .fetch_one(&pool)
        .await
        .unwrap(),
        6
    );

    let mut scheduler = repository.begin_scheduler_transaction().await.unwrap();
    assert!(matches!(
        scheduler
            .defer_orchestration_to_child_run(defer_child.clone())
            .await
            .unwrap(),
        CommandOutcome::Replayed(record)
            if record.child_link.child_link_id == deferred_child.child_link.child_link_id
    ));
    scheduler.commit().await.unwrap();

    let mut logical_replay = defer_child.clone();
    logical_replay.child_link_id = id("crun_0198f1c3-9a00-7c3e-b1f3-773c28367e53");
    logical_replay.child_run_id = id("run_0198f1c3-9a00-7c3e-b1f3-773c28367e54");
    logical_replay.child_root_scope_id = id("scp_0198f1c3-9a00-7c3e-b1f3-773c28367e55");
    logical_replay.child_entry_node_execution_id =
        id("nod_0198f1c3-9a00-7c3e-b1f3-773c28367e56");
    logical_replay.child_orchestration_job_id =
        id("job_0198f1c3-9a00-7c3e-b1f3-773c28367e57");
    logical_replay.input.value_id = id("val_0198f1c3-9a00-7c3e-b1f3-773c28367e58");
    logical_replay.idempotency_key_digest = digest('1');
    logical_replay.request_digest = digest('2');
    logical_replay.mutations = mutation_ids(
        "7e47",
        ["7e48", "7e49", "7e4a", "7e4b"],
        ["7e4c", "7e4d", "7e4e", "7e4f", "7e50", "7e51", "7e52"],
    );
    let unused_child_run_id = logical_replay.child_run_id.to_string();
    let mut scheduler = repository.begin_scheduler_transaction().await.unwrap();
    assert!(matches!(
        scheduler
            .defer_orchestration_to_child_run(logical_replay)
            .await
            .unwrap(),
        CommandOutcome::Replayed(record)
            if record.child_link.child_link_id == deferred_child.child_link.child_link_id
    ));
    scheduler.commit().await.unwrap();
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT count(*) FROM insight_platform.runs WHERE tenant_id = $1 AND run_id = $2",
        )
        .bind(TENANT_ID)
        .bind(unused_child_run_id)
        .fetch_one(&pool)
        .await
        .unwrap(),
        0
    );

    let mut scheduler = repository.begin_scheduler_transaction().await.unwrap();
    let claimed_child_job = scheduler
        .claim_orchestration_jobs(orchestration_claim_with_ids(
            WORKER_C_ID,
            '5',
            "7f00",
            ["7f01", "7f02", "7f03", "7f04"],
            ["7f05", "7f06", "7f07"],
        ))
        .await
        .unwrap();
    scheduler.commit().await.unwrap();
    assert_eq!(claimed_child_job.len(), 1);
    assert_eq!(
        claimed_child_job[0].job.job_id,
        deferred_child.child_job.job_id
    );
    let child_job_start = StartOrchestrationJob {
        fence: JobFence {
            tenant_id: TENANT_ID.to_owned(),
            job_id: claimed_child_job[0].job.job_id.clone(),
            worker_id: id(WORKER_C_ID),
            lease_epoch: claimed_child_job[0].job.lease_epoch,
            expected_job_version: claimed_child_job[0].job.version,
            lease_token_digest: digest('5'),
        },
        receipt_id: id("rcp_0198f1c3-9a00-7c3e-b1f3-773c28367f08"),
        idempotency_key_digest: digest('6'),
        request_digest: digest('7'),
        receipt_expires_at: Utc::now() + Duration::hours(1),
        job_event_id: id("evt_0198f1c3-9a00-7c3e-b1f3-773c28367f09"),
        job_outbox_id: id("obx_0198f1c3-9a00-7c3e-b1f3-773c28367f09"),
        node_event_id: id("evt_0198f1c3-9a00-7c3e-b1f3-773c28367f0a"),
        node_outbox_id: id("obx_0198f1c3-9a00-7c3e-b1f3-773c28367f0a"),
    };
    let mut scheduler = repository.begin_scheduler_transaction().await.unwrap();
    let child_job_started = match scheduler
        .start_orchestration_job(child_job_start.clone())
        .await
        .unwrap()
    {
        CommandOutcome::Applied(record) => record,
        CommandOutcome::Replayed(_) => panic!("child Job start must apply"),
    };
    scheduler.commit().await.unwrap();

    sqlx::query(
        r#"
        UPDATE insight_platform.run_nodes
        SET plan_node_key = 'finish', node_kind = 'return'
        WHERE tenant_id = $1 AND node_id = $2
          AND record_kind = 'node_execution' AND state = 'running'
        "#,
    )
    .bind(TENANT_ID)
    .bind(child_job_started.node_id.as_deref().unwrap())
    .execute(&pool)
    .await
    .unwrap();
    let child_terminal_body = match &defer_child.input.value {
        ValueRef::Inline { value } => value.clone(),
        ValueRef::Artifact { .. } => panic!("child fixture input must be Inline"),
    };
    let complete_child = CommitPlanTerminal {
        fence: JobFence {
            expected_job_version: child_job_started.version,
            ..child_job_start.fence
        },
        plan: child_runtime_plan(),
        value: MaterializedTerminalValue {
            value_id: defer_child.input.value_id.clone(),
            classification: defer_child.input.classification,
            schema_digest: defer_child.input.schema_digest.clone(),
            content_digest: defer_child.input.content_digest.clone(),
            body: child_terminal_body,
        },
        idempotency_key_digest: digest('9'),
        request_digest: digest('a'),
        receipt_expires_at: Utc::now() + Duration::hours(1),
        mutations: OrchestrationTerminalMutationIds {
            receipt_id: id("rcp_0198f1c3-9a00-7c3e-b1f3-773c28367f0c"),
            quota_entry_ids: ["7f0d", "7f0e", "7f0f", "7f10"]
                .map(|suffix| id(&format!("qle_0198f1c3-9a00-7c3e-b1f3-773c2836{suffix}")))
                .to_vec(),
            run_event_id: id("evt_0198f1c3-9a00-7c3e-b1f3-773c28367f11"),
            run_outbox_id: id("obx_0198f1c3-9a00-7c3e-b1f3-773c28367f11"),
            node_event_id: id("evt_0198f1c3-9a00-7c3e-b1f3-773c28367f12"),
            node_outbox_id: id("obx_0198f1c3-9a00-7c3e-b1f3-773c28367f12"),
            scope_closing_event_id: id("evt_0198f1c3-9a00-7c3e-b1f3-773c28367f13"),
            scope_closing_outbox_id: id("obx_0198f1c3-9a00-7c3e-b1f3-773c28367f13"),
            scope_terminal_event_id: id("evt_0198f1c3-9a00-7c3e-b1f3-773c28367f14"),
            scope_terminal_outbox_id: id("obx_0198f1c3-9a00-7c3e-b1f3-773c28367f14"),
            job_event_id: id("evt_0198f1c3-9a00-7c3e-b1f3-773c28367f15"),
            job_outbox_id: id("obx_0198f1c3-9a00-7c3e-b1f3-773c28367f15"),
        },
    };
    let mut scheduler = repository.begin_scheduler_transaction().await.unwrap();
    let completed_child = match scheduler
        .commit_plan_terminal(complete_child)
        .await
        .unwrap()
    {
        CommandOutcome::Applied(record) => record,
        CommandOutcome::Replayed(_) => panic!("child Run completion must apply"),
    };
    scheduler.commit().await.unwrap();
    assert_eq!(completed_child.run.state, "succeeded");

    let terminal_child_slot = TerminalChildRunSlot {
        parent_output_value_id: id("val_0198f1c3-9a00-7c3e-b1f3-773c28366f16"),
        continuation_node_execution_id: id("nod_0198f1c3-9a00-7c3e-b1f3-773c28366f17"),
        resume_job_id: id("job_0198f1c3-9a00-7c3e-b1f3-773c28367f16"),
        resume_request_digest: digest('b'),
        child_link_event_id: id("evt_0198f1c3-9a00-7c3e-b1f3-773c28367f17"),
        child_link_outbox_id: id("obx_0198f1c3-9a00-7c3e-b1f3-773c28367f17"),
        parent_run_event_id: id("evt_0198f1c3-9a00-7c3e-b1f3-773c28367f18"),
        parent_run_outbox_id: id("obx_0198f1c3-9a00-7c3e-b1f3-773c28367f18"),
        parent_node_event_id: id("evt_0198f1c3-9a00-7c3e-b1f3-773c28367f19"),
        parent_node_outbox_id: id("obx_0198f1c3-9a00-7c3e-b1f3-773c28367f19"),
        continuation_node_event_id: id("evt_0198f1c3-9a00-7c3e-b1f3-773c28366f18"),
        continuation_node_outbox_id: id("obx_0198f1c3-9a00-7c3e-b1f3-773c28366f18"),
        resume_job_event_id: id("evt_0198f1c3-9a00-7c3e-b1f3-773c28367f1a"),
        resume_job_outbox_id: id("obx_0198f1c3-9a00-7c3e-b1f3-773c28367f1a"),
    };
    let mut scheduler = repository.begin_scheduler_transaction().await.unwrap();
    let terminal_child = scheduler
        .drive_terminal_child_runs(DriveTerminalChildRuns {
            limit: 1,
            slots: vec![terminal_child_slot],
        })
        .await
        .unwrap();
    scheduler.commit().await.unwrap();
    assert_eq!(terminal_child.len(), 1);
    assert_eq!(terminal_child[0].child_link.state.as_str(), "succeeded");
    assert_eq!(terminal_child[0].child_run.state, "succeeded");
    assert_eq!(terminal_child[0].parent_run.state, "running");
    assert_eq!(terminal_child[0].resume_job.state, "ready");
    assert_eq!(
        terminal_child[0].parent_output_value_id,
        Some(id("val_0198f1c3-9a00-7c3e-b1f3-773c28366f16"))
    );
    assert_eq!(
        sqlx::query_scalar::<_, String>(
            "SELECT state FROM insight_platform.run_nodes WHERE tenant_id = $1 AND node_id = $2",
        )
        .bind(TENANT_ID)
        .bind(&terminal_child[0].parent_node_id)
        .fetch_one(&pool)
        .await
        .unwrap(),
        "succeeded"
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            r#"
            SELECT count(*) FROM insight_platform.run_values
            WHERE tenant_id = $1 AND value_id = $2 AND run_id = $3 AND node_id = $4
              AND value_kind = 'run_output' AND classification = 'internal'
              AND schema_digest = $5 AND content_digest = $6
              AND inline_value = $7 AND artifact_id IS NULL
            "#,
        )
        .bind(TENANT_ID)
        .bind("val_0198f1c3-9a00-7c3e-b1f3-773c28366f16")
        .bind(&terminal_child[0].parent_run.run_id)
        .bind(&terminal_child[0].parent_node_id)
        .bind(agent_schema().canonical_digest.to_string())
        .bind(child_input_digest.to_string())
        .bind(json!({"question": "select one"}))
        .fetch_one(&pool)
        .await
        .unwrap(),
        1
    );
    assert_eq!(
        sqlx::query_scalar::<_, String>(
            r#"
            SELECT plan_node_key FROM insight_platform.run_nodes
            WHERE tenant_id = $1 AND node_id = $2 AND run_id = $3
              AND record_kind = 'node_execution' AND state = 'ready'
            "#,
        )
        .bind(TENANT_ID)
        .bind("nod_0198f1c3-9a00-7c3e-b1f3-773c28366f17")
        .bind(&terminal_child[0].parent_run.run_id)
        .fetch_one(&pool)
        .await
        .unwrap(),
        "child_call_2"
    );
    assert_eq!(
        terminal_child[0].resume_job.node_id.as_deref(),
        Some("nod_0198f1c3-9a00-7c3e-b1f3-773c28366f17")
    );

    let mut scheduler = repository.begin_scheduler_transaction().await.unwrap();
    assert!(scheduler
        .drive_terminal_child_runs(DriveTerminalChildRuns {
            limit: 1,
            slots: vec![TerminalChildRunSlot {
                parent_output_value_id: id("val_0198f1c3-9a00-7c3e-b1f3-773c28366f1b"),
                continuation_node_execution_id: id(
                    "nod_0198f1c3-9a00-7c3e-b1f3-773c28366f1c",
                ),
                resume_job_id: id("job_0198f1c3-9a00-7c3e-b1f3-773c28367f1b"),
                resume_request_digest: digest('c'),
                child_link_event_id: id("evt_0198f1c3-9a00-7c3e-b1f3-773c28367f1c"),
                child_link_outbox_id: id("obx_0198f1c3-9a00-7c3e-b1f3-773c28367f1c"),
                parent_run_event_id: id("evt_0198f1c3-9a00-7c3e-b1f3-773c28367f1d"),
                parent_run_outbox_id: id("obx_0198f1c3-9a00-7c3e-b1f3-773c28367f1d"),
                parent_node_event_id: id("evt_0198f1c3-9a00-7c3e-b1f3-773c28367f1e"),
                parent_node_outbox_id: id("obx_0198f1c3-9a00-7c3e-b1f3-773c28367f1e"),
                continuation_node_event_id: id(
                    "evt_0198f1c3-9a00-7c3e-b1f3-773c28366f1d",
                ),
                continuation_node_outbox_id: id(
                    "obx_0198f1c3-9a00-7c3e-b1f3-773c28366f1d",
                ),
                resume_job_event_id: id("evt_0198f1c3-9a00-7c3e-b1f3-773c28367f1f"),
                resume_job_outbox_id: id("obx_0198f1c3-9a00-7c3e-b1f3-773c28367f1f"),
            }],
        })
        .await
        .unwrap()
        .is_empty());
    scheduler.commit().await.unwrap();

    let mut scheduler = repository.begin_scheduler_transaction().await.unwrap();
    let cancel_parent_claim = scheduler
        .claim_orchestration_jobs(orchestration_claim_with_ids(
            WORKER_D_ID,
            'd',
            "8000",
            ["8001", "8002", "8003", "8004"],
            ["8005", "8006", "8007"],
        ))
        .await
        .unwrap();
    scheduler.commit().await.unwrap();
    assert_eq!(cancel_parent_claim.len(), 1);
    assert_eq!(
        cancel_parent_claim[0].job.job_id,
        terminal_child[0].resume_job.job_id
    );
    let cancel_parent_start = StartOrchestrationJob {
        fence: JobFence {
            tenant_id: TENANT_ID.to_owned(),
            job_id: cancel_parent_claim[0].job.job_id.clone(),
            worker_id: id(WORKER_D_ID),
            lease_epoch: cancel_parent_claim[0].job.lease_epoch,
            expected_job_version: cancel_parent_claim[0].job.version,
            lease_token_digest: digest('d'),
        },
        receipt_id: id("rcp_0198f1c3-9a00-7c3e-b1f3-773c28368008"),
        idempotency_key_digest: digest('e'),
        request_digest: digest('f'),
        receipt_expires_at: Utc::now() + Duration::hours(1),
        job_event_id: id("evt_0198f1c3-9a00-7c3e-b1f3-773c28368009"),
        job_outbox_id: id("obx_0198f1c3-9a00-7c3e-b1f3-773c28368009"),
        node_event_id: id("evt_0198f1c3-9a00-7c3e-b1f3-773c2836800a"),
        node_outbox_id: id("obx_0198f1c3-9a00-7c3e-b1f3-773c2836800a"),
    };
    let mut scheduler = repository.begin_scheduler_transaction().await.unwrap();
    let cancel_parent_started = match scheduler
        .start_orchestration_job(cancel_parent_start.clone())
        .await
        .unwrap()
    {
        CommandOutcome::Applied(record) => record,
        CommandOutcome::Replayed(_) => panic!("cancel parent start must apply"),
    };
    scheduler.commit().await.unwrap();

    let mut cancel_child = defer_child.clone();
    cancel_child.fence = JobFence {
        expected_job_version: cancel_parent_started.version,
        ..cancel_parent_start.fence
    };
    cancel_child.child_link_id = id("crun_0198f1c3-9a00-7c3e-b1f3-773c2836800b");
    cancel_child.child_run_id = id("run_0198f1c3-9a00-7c3e-b1f3-773c2836800c");
    cancel_child.child_root_scope_id = id("scp_0198f1c3-9a00-7c3e-b1f3-773c2836800d");
    cancel_child.child_entry_node_execution_id =
        id("nod_0198f1c3-9a00-7c3e-b1f3-773c2836800e");
    cancel_child.child_orchestration_job_id =
        id("job_0198f1c3-9a00-7c3e-b1f3-773c2836800f");
    cancel_child.input.value_id = id("val_0198f1c3-9a00-7c3e-b1f3-773c28368010");
    cancel_child.budget.deadline = Utc::now() + Duration::seconds(30);
    cancel_child.logical_key = "child:cancel-path:attempt-1".to_owned();
    cancel_child.idempotency_key_digest = digest('0');
    cancel_child.request_digest = digest('1');
    cancel_child.mutations = mutation_ids(
        "8011",
        ["8012", "8013", "8014", "8015"],
        ["8016", "8017", "8018", "8019", "801a", "801b", "801c"],
    );
    let mut scheduler = repository.begin_scheduler_transaction().await.unwrap();
    let cancelling_child = match scheduler
        .defer_orchestration_to_child_run(cancel_child)
        .await
        .unwrap()
    {
        CommandOutcome::Applied(record) => record,
        CommandOutcome::Replayed(_) => panic!("cancel-path child must apply"),
    };
    scheduler.commit().await.unwrap();

    let cancel_parent = RequestRunCancel {
        audit: audit(TENANT_ID, PRINCIPAL_ID, "8020", '2', '3'),
        run_id: cancelling_child
            .parent_run
            .run_id
            .parse()
            .unwrap(),
        expected_run_version: cancelling_child.parent_run.version,
        expected_cancel_generation: u64::try_from(cancelling_child.parent_run.cancel_generation)
            .unwrap(),
        reason_code: "user_requested".to_owned(),
    };
    let cancelled_parent = match run_command!(repository, request_run_cancel, cancel_parent).unwrap()
    {
        CommandOutcome::Applied(record) => record,
        CommandOutcome::Replayed(_) => panic!("parent cancel must apply"),
    };
    assert_eq!(cancelled_parent.state, "cancelling");

    let mut scheduler = repository.begin_scheduler_transaction().await.unwrap();
    let propagated = scheduler
        .drive_child_run_cancellations(DriveChildRunCancellations {
            limit: 1,
            slots: vec![ChildRunCancellationSlot {
                child_link_event_id: id("evt_0198f1c3-9a00-7c3e-b1f3-773c28368021"),
                child_link_outbox_id: id("obx_0198f1c3-9a00-7c3e-b1f3-773c28368021"),
                child_run_event_id: id("evt_0198f1c3-9a00-7c3e-b1f3-773c28368022"),
                child_run_outbox_id: id("obx_0198f1c3-9a00-7c3e-b1f3-773c28368022"),
            }],
        })
        .await
        .unwrap();
    scheduler.commit().await.unwrap();
    assert_eq!(propagated.len(), 1);
    assert_eq!(propagated[0].child_link.state.as_str(), "cancelling");
    assert_eq!(propagated[0].child_run.state, "cancelling");
    assert_eq!(propagated[0].child_run.cancel_generation, 1);

    let mut scheduler = repository.begin_scheduler_transaction().await.unwrap();
    assert!(scheduler
        .drive_child_run_cancellations(DriveChildRunCancellations {
            limit: 1,
            slots: vec![ChildRunCancellationSlot {
                child_link_event_id: id("evt_0198f1c3-9a00-7c3e-b1f3-773c28368023"),
                child_link_outbox_id: id("obx_0198f1c3-9a00-7c3e-b1f3-773c28368023"),
                child_run_event_id: id("evt_0198f1c3-9a00-7c3e-b1f3-773c28368024"),
                child_run_outbox_id: id("obx_0198f1c3-9a00-7c3e-b1f3-773c28368024"),
            }],
        })
        .await
        .unwrap()
        .is_empty());
    scheduler.commit().await.unwrap();

    let mut scheduler = repository.begin_scheduler_transaction().await.unwrap();
    let child_cancelled = scheduler
        .drive_orchestration_convergence(orchestration_convergence(
            ["8025", "8026", "8027", "8028"],
            ["8029", "802a", "802b", "802c", "802d", "802e"],
        ))
        .await
        .unwrap();
    scheduler.commit().await.unwrap();
    assert_eq!(child_cancelled.len(), 1);
    assert_eq!(child_cancelled[0].completed.run.state, "cancelled");
    assert_eq!(
        child_cancelled[0].completed.run.run_id,
        propagated[0].child_run.run_id
    );

    let mut scheduler = repository.begin_scheduler_transaction().await.unwrap();
    let cancel_terminal = scheduler
        .drive_terminal_child_runs(DriveTerminalChildRuns {
            limit: 1,
            slots: vec![TerminalChildRunSlot {
                parent_output_value_id: id("val_0198f1c3-9a00-7c3e-b1f3-773c2836602f"),
                continuation_node_execution_id: id(
                    "nod_0198f1c3-9a00-7c3e-b1f3-773c28366030",
                ),
                resume_job_id: id("job_0198f1c3-9a00-7c3e-b1f3-773c2836802f"),
                resume_request_digest: digest('4'),
                child_link_event_id: id("evt_0198f1c3-9a00-7c3e-b1f3-773c28368030"),
                child_link_outbox_id: id("obx_0198f1c3-9a00-7c3e-b1f3-773c28368030"),
                parent_run_event_id: id("evt_0198f1c3-9a00-7c3e-b1f3-773c28368031"),
                parent_run_outbox_id: id("obx_0198f1c3-9a00-7c3e-b1f3-773c28368031"),
                parent_node_event_id: id("evt_0198f1c3-9a00-7c3e-b1f3-773c28368032"),
                parent_node_outbox_id: id("obx_0198f1c3-9a00-7c3e-b1f3-773c28368032"),
                continuation_node_event_id: id(
                    "evt_0198f1c3-9a00-7c3e-b1f3-773c28366031",
                ),
                continuation_node_outbox_id: id(
                    "obx_0198f1c3-9a00-7c3e-b1f3-773c28366031",
                ),
                resume_job_event_id: id("evt_0198f1c3-9a00-7c3e-b1f3-773c28368033"),
                resume_job_outbox_id: id("obx_0198f1c3-9a00-7c3e-b1f3-773c28368033"),
            }],
        })
        .await
        .unwrap();
    scheduler.commit().await.unwrap();
    assert_eq!(cancel_terminal.len(), 1);
    assert_eq!(cancel_terminal[0].child_link.state.as_str(), "cancelled");
    assert_eq!(cancel_terminal[0].parent_run.state, "cancelling");
    assert_eq!(cancel_terminal[0].resume_job.state, "cancelling");

    let mut scheduler = repository.begin_scheduler_transaction().await.unwrap();
    let parent_cancelled = scheduler
        .drive_orchestration_convergence(orchestration_convergence(
            ["8034", "8035", "8036", "8037"],
            ["8038", "8039", "803a", "803b", "803c", "803d"],
        ))
        .await
        .unwrap();
    scheduler.commit().await.unwrap();
    assert_eq!(parent_cancelled.len(), 1);
    assert_eq!(parent_cancelled[0].completed.run.state, "cancelled");
    assert_eq!(
        parent_cancelled[0].completed.run.run_id,
        cancelling_child.parent_run.run_id
    );

    let reserved_before_controller: i64 = sqlx::query_scalar(
        "SELECT reserved_value FROM insight_platform.quota_accounts WHERE tenant_id = $1 AND quota_account_id = $2",
    )
    .bind(TENANT_ID)
    .bind(QUOTA_ACCOUNT_ID)
    .fetch_one(&pool)
    .await
    .unwrap();
    let controller_admission = admission(
        AdmissionIds {
            run: "run_0198f1c3-9a00-7c3e-b1f3-773c28369000",
            scope: "scp_0198f1c3-9a00-7c3e-b1f3-773c28369001",
            node: "nod_0198f1c3-9a00-7c3e-b1f3-773c28369002",
            job: "job_0198f1c3-9a00-7c3e-b1f3-773c28369003",
            value: "val_0198f1c3-9a00-7c3e-b1f3-773c28369004",
        },
        audit(TENANT_ID, PRINCIPAL_ID, "9005", '5', '6'),
        bindings.clone(),
        Utc::now() + Duration::minutes(5),
    );
    applied(run_command!(
        repository,
        admit_run,
        controller_admission.clone()
    )
    .unwrap());
    let mut scheduler = repository.begin_scheduler_transaction().await.unwrap();
    let controller_claimed = scheduler
        .claim_orchestration_jobs(orchestration_claim_with_ids(
            WORKER_D_ID,
            '7',
            "9010",
            ["9011", "9012", "9013", "9014"],
            ["9015", "9016", "9017"],
        ))
        .await
        .unwrap();
    scheduler.commit().await.unwrap();
    assert_eq!(controller_claimed.len(), 1);
    assert_eq!(
        controller_claimed[0].job.job_id,
        controller_admission.orchestration_job_id.to_string()
    );
    let controller_start = StartOrchestrationJob {
        fence: JobFence {
            tenant_id: TENANT_ID.to_owned(),
            job_id: controller_claimed[0].job.job_id.clone(),
            worker_id: id(WORKER_D_ID),
            lease_epoch: controller_claimed[0].job.lease_epoch,
            expected_job_version: controller_claimed[0].job.version,
            lease_token_digest: digest('7'),
        },
        receipt_id: id("rcp_0198f1c3-9a00-7c3e-b1f3-773c28369018"),
        idempotency_key_digest: digest('8'),
        request_digest: digest('9'),
        receipt_expires_at: Utc::now() + Duration::hours(1),
        job_event_id: id("evt_0198f1c3-9a00-7c3e-b1f3-773c28369019"),
        job_outbox_id: id("obx_0198f1c3-9a00-7c3e-b1f3-773c28369019"),
        node_event_id: id("evt_0198f1c3-9a00-7c3e-b1f3-773c2836901a"),
        node_outbox_id: id("obx_0198f1c3-9a00-7c3e-b1f3-773c2836901a"),
    };
    let mut scheduler = repository.begin_scheduler_transaction().await.unwrap();
    let controller_started = match scheduler
        .start_orchestration_job(controller_start.clone())
        .await
        .unwrap()
    {
        CommandOutcome::Applied(record) => record,
        CommandOutcome::Replayed(_) => panic!("controller start must apply"),
    };
    scheduler.commit().await.unwrap();

    let controller_mutations = ControllerStepMutationIds {
        receipt_id: id("rcp_0198f1c3-9a00-7c3e-b1f3-773c28369020"),
        quota_entry_ids: ["9021", "9022", "9023", "9024"]
            .map(|suffix| {
                id(&format!(
                    "qle_0198f1c3-9a00-7c3e-b1f3-773c2836{suffix}"
                ))
            })
            .to_vec(),
        run_event_id: id("evt_0198f1c3-9a00-7c3e-b1f3-773c28369025"),
        run_outbox_id: id("obx_0198f1c3-9a00-7c3e-b1f3-773c28369025"),
        node_event_id: id("evt_0198f1c3-9a00-7c3e-b1f3-773c28369026"),
        node_outbox_id: id("obx_0198f1c3-9a00-7c3e-b1f3-773c28369026"),
        job_event_id: id("evt_0198f1c3-9a00-7c3e-b1f3-773c28369027"),
        job_outbox_id: id("obx_0198f1c3-9a00-7c3e-b1f3-773c28369027"),
        activations: vec![ControllerActivationSlot {
            node_execution_id: id("nod_0198f1c3-9a00-7c3e-b1f3-773c28369028"),
            orchestration_job_id: id("job_0198f1c3-9a00-7c3e-b1f3-773c28369029"),
            scope: None,
            node_event_id: id("evt_0198f1c3-9a00-7c3e-b1f3-773c2836902a"),
            node_outbox_id: id("obx_0198f1c3-9a00-7c3e-b1f3-773c2836902a"),
            job_event_id: id("evt_0198f1c3-9a00-7c3e-b1f3-773c2836902b"),
            job_outbox_id: id("obx_0198f1c3-9a00-7c3e-b1f3-773c2836902b"),
        }],
        pending_nodes: vec![],
        structural_exit: None,
        pending_wake: None,
        remainder_cancellations: Vec::new(),
    };
    let controller_fence = JobFence {
        expected_job_version: controller_started.version,
        ..controller_start.fence.clone()
    };
    let plan = runtime_plan();
    let mut forged_plan = plan.clone();
    let alternate = PlanNodeKey::new("alternate".to_owned()).unwrap();
    forged_plan.nodes.insert(alternate.clone(), return_run_input());
    forged_plan.nodes.insert(
        forged_plan.entry_node_id.clone(),
        RuntimeNode::Start { next: alternate },
    );
    let controller_step = ApplyOrchestrationControllerStep {
        fence: controller_fence,
        plan: plan.clone(),
        observation: ControllerObservation::None,
        idempotency_key_digest: digest('a'),
        request_digest: digest('b'),
        receipt_expires_at: Utc::now() + Duration::hours(1),
        mutations: controller_mutations,
    };
    let mut scheduler = repository.begin_scheduler_transaction().await.unwrap();
    let forged_failure = scheduler
        .apply_orchestration_controller_step(ApplyOrchestrationControllerStep {
            plan: forged_plan,
            ..controller_step.clone()
        })
        .await
        .unwrap_err();
    assert!(matches!(
        forged_failure,
        RepositoryError::Conflict("exact typed Plan digest")
    ), "unexpected forged Plan failure: {forged_failure:?}");
    scheduler.rollback().await.unwrap();
    assert_eq!(
        sqlx::query_scalar::<_, String>(
            "SELECT state FROM insight_platform.jobs WHERE tenant_id = $1 AND job_id = $2",
        )
        .bind(TENANT_ID)
        .bind(&controller_step.fence.job_id)
        .fetch_one(&pool)
        .await
        .unwrap(),
        "running"
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT count(*) FROM insight_platform.run_nodes WHERE tenant_id = $1 AND node_id = $2",
        )
        .bind(TENANT_ID)
        .bind(controller_step.mutations.activations[0].node_execution_id.to_string())
        .fetch_one(&pool)
        .await
        .unwrap(),
        0
    );

    let mut scheduler = repository.begin_scheduler_transaction().await.unwrap();
    let controller_applied = match scheduler
        .apply_orchestration_controller_step(controller_step.clone())
        .await
        .unwrap()
    {
        CommandOutcome::Applied(record) => record,
        CommandOutcome::Replayed(_) => panic!("first controller step must apply"),
    };
    scheduler.commit().await.unwrap();
    assert_eq!(controller_applied.source_job.state, "succeeded");
    assert_eq!(controller_applied.run.active_work_count, 0);
    assert_eq!(controller_applied.activations.len(), 1);
    assert_eq!(
        controller_applied.activations[0].plan_node_key.as_str(),
        "finish"
    );
    assert_eq!(controller_applied.activations[0].job.state, "ready");
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT reserved_value FROM insight_platform.quota_accounts WHERE tenant_id = $1 AND quota_account_id = $2",
        )
        .bind(TENANT_ID)
        .bind(QUOTA_ACCOUNT_ID)
        .fetch_one(&pool)
        .await
        .unwrap(),
        reserved_before_controller
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            r#"
            SELECT count(*) FROM insight_platform.events
            WHERE tenant_id = $1 AND event_id = ANY($2)
            "#,
        )
        .bind(TENANT_ID)
        .bind(
            ["9025", "9026", "9027", "902a", "902b"]
                .map(|suffix| format!("evt_0198f1c3-9a00-7c3e-b1f3-773c2836{suffix}"))
                .to_vec(),
        )
        .fetch_one(&pool)
        .await
        .unwrap(),
        5
    );
    let mut scheduler = repository.begin_scheduler_transaction().await.unwrap();
    assert!(matches!(
        scheduler
            .apply_orchestration_controller_step(controller_step.clone())
            .await
            .unwrap(),
        CommandOutcome::Replayed(record)
            if record.activations.len() == 1
                && record.activations[0].node_id
                    == controller_step.mutations.activations[0].node_execution_id.to_string()
    ));
    scheduler.commit().await.unwrap();
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT count(*) FROM insight_platform.run_nodes WHERE tenant_id = $1 AND run_id = $2 AND plan_node_key = 'finish'",
        )
        .bind(TENANT_ID)
        .bind(controller_admission.run_id.to_string())
        .fetch_one(&pool)
        .await
        .unwrap(),
        1
    );

    let cancelled_controller = applied(
        run_command!(
            repository,
            request_run_cancel,
            RequestRunCancel {
                audit: audit(TENANT_ID, PRINCIPAL_ID, "9030", 'c', 'd'),
                run_id: controller_admission.run_id.clone(),
                expected_run_version: controller_applied.run.version,
                expected_cancel_generation: 0,
                reason_code: "fixture_cleanup".to_owned(),
            }
        )
        .unwrap(),
    );
    assert_eq!(cancelled_controller.state, "cancelling");
    let mut scheduler = repository.begin_scheduler_transaction().await.unwrap();
    let controller_cleanup = scheduler
        .drive_orchestration_convergence(orchestration_convergence(
            ["9031", "9032", "9033", "9034"],
            ["9035", "9036", "9037", "9038", "9039", "903a"],
        ))
        .await
        .unwrap();
    scheduler.commit().await.unwrap();
    assert_eq!(controller_cleanup.len(), 1);
    assert_eq!(controller_cleanup[0].completed.run.state, "cancelled");

    let reserved_before_fork: i64 = sqlx::query_scalar(
        "SELECT reserved_value FROM insight_platform.quota_accounts WHERE tenant_id = $1 AND quota_account_id = $2",
    )
    .bind(TENANT_ID)
    .bind(QUOTA_ACCOUNT_ID)
    .fetch_one(&pool)
    .await
    .unwrap();
    let mut fork_admission = admission(
        AdmissionIds {
            run: "run_0198f1c3-9a00-7c3e-b1f3-773c28369100",
            scope: "scp_0198f1c3-9a00-7c3e-b1f3-773c28369101",
            node: "nod_0198f1c3-9a00-7c3e-b1f3-773c28369102",
            job: "job_0198f1c3-9a00-7c3e-b1f3-773c28369103",
            value: "val_0198f1c3-9a00-7c3e-b1f3-773c28369104",
        },
        audit(TENANT_ID, PRINCIPAL_ID, "9105", 'e', 'f'),
        bindings.clone(),
        Utc::now() + Duration::minutes(5),
    );
    fork_admission.entry_plan_node_key = PlanNodeKey::new("fork".to_owned()).unwrap();
    fork_admission.entry_node_kind = PlanNodeKind::Fork;
    applied(run_command!(repository, admit_run, fork_admission.clone()).unwrap());
    let mut scheduler = repository.begin_scheduler_transaction().await.unwrap();
    let fork_claimed = scheduler
        .claim_orchestration_jobs(orchestration_claim_with_ids(
            WORKER_D_ID,
            '1',
            "9110",
            ["9111", "9112", "9113", "9114"],
            ["9115", "9116", "9117"],
        ))
        .await
        .unwrap();
    scheduler.commit().await.unwrap();
    assert_eq!(fork_claimed.len(), 1);
    assert_eq!(
        fork_claimed[0].job.job_id,
        fork_admission.orchestration_job_id.to_string()
    );
    let fork_start = StartOrchestrationJob {
        fence: JobFence {
            tenant_id: TENANT_ID.to_owned(),
            job_id: fork_claimed[0].job.job_id.clone(),
            worker_id: id(WORKER_D_ID),
            lease_epoch: fork_claimed[0].job.lease_epoch,
            expected_job_version: fork_claimed[0].job.version,
            lease_token_digest: digest('1'),
        },
        receipt_id: id("rcp_0198f1c3-9a00-7c3e-b1f3-773c28369118"),
        idempotency_key_digest: digest('2'),
        request_digest: digest('3'),
        receipt_expires_at: Utc::now() + Duration::hours(1),
        job_event_id: id("evt_0198f1c3-9a00-7c3e-b1f3-773c28369119"),
        job_outbox_id: id("obx_0198f1c3-9a00-7c3e-b1f3-773c28369119"),
        node_event_id: id("evt_0198f1c3-9a00-7c3e-b1f3-773c2836911a"),
        node_outbox_id: id("obx_0198f1c3-9a00-7c3e-b1f3-773c2836911a"),
    };
    let mut scheduler = repository.begin_scheduler_transaction().await.unwrap();
    let fork_started = match scheduler
        .start_orchestration_job(fork_start.clone())
        .await
        .unwrap()
    {
        CommandOutcome::Applied(record) => record,
        CommandOutcome::Replayed(_) => panic!("Fork start must apply"),
    };
    scheduler.commit().await.unwrap();

    let fork_step = ApplyOrchestrationControllerStep {
        fence: JobFence {
            expected_job_version: fork_started.version,
            ..fork_start.fence.clone()
        },
        plan: runtime_plan(),
        observation: ControllerObservation::None,
        idempotency_key_digest: digest('4'),
        request_digest: digest('5'),
        receipt_expires_at: Utc::now() + Duration::hours(1),
        mutations: ControllerStepMutationIds {
            receipt_id: id("rcp_0198f1c3-9a00-7c3e-b1f3-773c28369120"),
            quota_entry_ids: ["9121", "9122", "9123", "9124"]
                .map(|suffix| {
                    id(&format!(
                        "qle_0198f1c3-9a00-7c3e-b1f3-773c2836{suffix}"
                    ))
                })
                .to_vec(),
            run_event_id: id("evt_0198f1c3-9a00-7c3e-b1f3-773c28369125"),
            run_outbox_id: id("obx_0198f1c3-9a00-7c3e-b1f3-773c28369125"),
            node_event_id: id("evt_0198f1c3-9a00-7c3e-b1f3-773c28369126"),
            node_outbox_id: id("obx_0198f1c3-9a00-7c3e-b1f3-773c28369126"),
            job_event_id: id("evt_0198f1c3-9a00-7c3e-b1f3-773c28369127"),
            job_outbox_id: id("obx_0198f1c3-9a00-7c3e-b1f3-773c28369127"),
            activations: vec![
                ControllerActivationSlot {
                    node_execution_id: id(
                        "nod_0198f1c3-9a00-7c3e-b1f3-773c28369130",
                    ),
                    orchestration_job_id: id(
                        "job_0198f1c3-9a00-7c3e-b1f3-773c28369131",
                    ),
                    scope: Some(ControllerScopeSlot {
                        scope_instance_id: id(
                            "scp_0198f1c3-9a00-7c3e-b1f3-773c28369132",
                        ),
                        scope_event_id: id(
                            "evt_0198f1c3-9a00-7c3e-b1f3-773c28369135",
                        ),
                        scope_outbox_id: id(
                            "obx_0198f1c3-9a00-7c3e-b1f3-773c28369135",
                        ),
                    }),
                    node_event_id: id("evt_0198f1c3-9a00-7c3e-b1f3-773c28369133"),
                    node_outbox_id: id("obx_0198f1c3-9a00-7c3e-b1f3-773c28369133"),
                    job_event_id: id("evt_0198f1c3-9a00-7c3e-b1f3-773c28369134"),
                    job_outbox_id: id("obx_0198f1c3-9a00-7c3e-b1f3-773c28369134"),
                },
                ControllerActivationSlot {
                    node_execution_id: id(
                        "nod_0198f1c3-9a00-7c3e-b1f3-773c28369140",
                    ),
                    orchestration_job_id: id(
                        "job_0198f1c3-9a00-7c3e-b1f3-773c28369141",
                    ),
                    scope: Some(ControllerScopeSlot {
                        scope_instance_id: id(
                            "scp_0198f1c3-9a00-7c3e-b1f3-773c28369142",
                        ),
                        scope_event_id: id(
                            "evt_0198f1c3-9a00-7c3e-b1f3-773c28369145",
                        ),
                        scope_outbox_id: id(
                            "obx_0198f1c3-9a00-7c3e-b1f3-773c28369145",
                        ),
                    }),
                    node_event_id: id("evt_0198f1c3-9a00-7c3e-b1f3-773c28369143"),
                    node_outbox_id: id("obx_0198f1c3-9a00-7c3e-b1f3-773c28369143"),
                    job_event_id: id("evt_0198f1c3-9a00-7c3e-b1f3-773c28369144"),
                    job_outbox_id: id("obx_0198f1c3-9a00-7c3e-b1f3-773c28369144"),
                },
            ],
            pending_nodes: vec![ControllerPendingNodeSlot {
                node_execution_id: id("nod_0198f1c3-9a00-7c3e-b1f3-773c28369150"),
                node_event_id: id("evt_0198f1c3-9a00-7c3e-b1f3-773c28369151"),
                node_outbox_id: id("obx_0198f1c3-9a00-7c3e-b1f3-773c28369151"),
            }],
            structural_exit: None,
            pending_wake: None,
            remainder_cancellations: Vec::new(),
        },
    };
    let mut invalid_fork_step = fork_step.clone();
    invalid_fork_step.mutations.activations[0].scope = None;
    let mut scheduler = repository.begin_scheduler_transaction().await.unwrap();
    assert!(matches!(
        scheduler
            .apply_orchestration_controller_step(invalid_fork_step)
            .await
            .unwrap_err(),
        RepositoryError::InvalidInput(_)
    ));
    scheduler.rollback().await.unwrap();
    assert_eq!(
        sqlx::query_scalar::<_, String>(
            "SELECT state FROM insight_platform.jobs WHERE tenant_id = $1 AND job_id = $2",
        )
        .bind(TENANT_ID)
        .bind(&fork_step.fence.job_id)
        .fetch_one(&pool)
        .await
        .unwrap(),
        "running"
    );

    let mut scheduler = repository.begin_scheduler_transaction().await.unwrap();
    let opened_fork = match scheduler
        .apply_orchestration_controller_step(fork_step.clone())
        .await
        .unwrap()
    {
        CommandOutcome::Applied(record) => record,
        CommandOutcome::Replayed(_) => panic!("first Fork step must apply"),
    };
    scheduler.commit().await.unwrap();
    assert_eq!(opened_fork.activations.len(), 2);
    assert_eq!(opened_fork.created_scopes.len(), 2);
    assert_eq!(opened_fork.pending_nodes.len(), 1);
    assert!(opened_fork
        .activations
        .iter()
        .all(|activation| activation.job.state == "ready"));
    assert!(opened_fork
        .created_scopes
        .iter()
        .all(|scope| scope.scope_kind == "parallel_leg"));
    assert_eq!(opened_fork.pending_nodes[0].plan_node_key.as_str(), "join");
    assert_eq!(
        sqlx::query_scalar::<_, String>(
            "SELECT state FROM insight_platform.run_nodes WHERE tenant_id = $1 AND node_id = $2",
        )
        .bind(TENANT_ID)
        .bind(&opened_fork.pending_nodes[0].node_id)
        .fetch_one(&pool)
        .await
        .unwrap(),
        "pending"
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT reserved_value FROM insight_platform.quota_accounts WHERE tenant_id = $1 AND quota_account_id = $2",
        )
        .bind(TENANT_ID)
        .bind(QUOTA_ACCOUNT_ID)
        .fetch_one(&pool)
        .await
        .unwrap(),
        reserved_before_fork
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT count(*) FROM insight_platform.events WHERE tenant_id = $1 AND event_id = ANY($2)",
        )
        .bind(TENANT_ID)
        .bind(
            [
                "9125", "9126", "9127", "9133", "9134", "9135", "9143",
                "9144", "9145", "9151",
            ]
            .map(|suffix| format!("evt_0198f1c3-9a00-7c3e-b1f3-773c2836{suffix}"))
            .to_vec(),
        )
        .fetch_one(&pool)
        .await
        .unwrap(),
        10
    );
    let mut scheduler = repository.begin_scheduler_transaction().await.unwrap();
    assert!(matches!(
        scheduler
            .apply_orchestration_controller_step(fork_step.clone())
            .await
            .unwrap(),
        CommandOutcome::Replayed(record)
            if record.activations.len() == 2
                && record.created_scopes.len() == 2
                && record.pending_nodes.len() == 1
    ));
    scheduler.commit().await.unwrap();

    let mut scheduler = repository.begin_scheduler_transaction().await.unwrap();
    let left_claimed = scheduler
        .claim_orchestration_jobs(orchestration_claim_with_ids(
            WORKER_D_ID,
            '2',
            "9160",
            ["9161", "9162", "9163", "9164"],
            ["9165", "9166", "9167"],
        ))
        .await
        .unwrap();
    scheduler.commit().await.unwrap();
    assert_eq!(left_claimed.len(), 1);
    assert_eq!(
        left_claimed[0].job.node_id.as_deref(),
        Some(opened_fork.activations[0].node_id.as_str())
    );
    let left_start = StartOrchestrationJob {
        fence: JobFence {
            tenant_id: TENANT_ID.to_owned(),
            job_id: left_claimed[0].job.job_id.clone(),
            worker_id: id(WORKER_D_ID),
            lease_epoch: left_claimed[0].job.lease_epoch,
            expected_job_version: left_claimed[0].job.version,
            lease_token_digest: digest('2'),
        },
        receipt_id: id("rcp_0198f1c3-9a00-7c3e-b1f3-773c28369168"),
        idempotency_key_digest: digest('3'),
        request_digest: digest('4'),
        receipt_expires_at: Utc::now() + Duration::hours(1),
        job_event_id: id("evt_0198f1c3-9a00-7c3e-b1f3-773c28369169"),
        job_outbox_id: id("obx_0198f1c3-9a00-7c3e-b1f3-773c28369169"),
        node_event_id: id("evt_0198f1c3-9a00-7c3e-b1f3-773c2836916a"),
        node_outbox_id: id("obx_0198f1c3-9a00-7c3e-b1f3-773c2836916a"),
    };
    let mut scheduler = repository.begin_scheduler_transaction().await.unwrap();
    let left_started = match scheduler
        .start_orchestration_job(left_start.clone())
        .await
        .unwrap()
    {
        CommandOutcome::Applied(record) => record,
        CommandOutcome::Replayed(_) => panic!("left leg start must apply"),
    };
    scheduler.commit().await.unwrap();
    let left_exit = ApplyOrchestrationControllerStep {
        fence: JobFence {
            expected_job_version: left_started.version,
            ..left_start.fence.clone()
        },
        plan: runtime_plan(),
        observation: ControllerObservation::None,
        idempotency_key_digest: digest('5'),
        request_digest: digest('6'),
        receipt_expires_at: Utc::now() + Duration::hours(1),
        mutations: ControllerStepMutationIds {
            receipt_id: id("rcp_0198f1c3-9a00-7c3e-b1f3-773c2836916b"),
            quota_entry_ids: ["916c", "916d", "916e", "916f"]
                .map(|suffix| {
                    id(&format!(
                        "qle_0198f1c3-9a00-7c3e-b1f3-773c2836{suffix}"
                    ))
                })
                .to_vec(),
            run_event_id: id("evt_0198f1c3-9a00-7c3e-b1f3-773c28369170"),
            run_outbox_id: id("obx_0198f1c3-9a00-7c3e-b1f3-773c28369170"),
            node_event_id: id("evt_0198f1c3-9a00-7c3e-b1f3-773c28369171"),
            node_outbox_id: id("obx_0198f1c3-9a00-7c3e-b1f3-773c28369171"),
            job_event_id: id("evt_0198f1c3-9a00-7c3e-b1f3-773c28369172"),
            job_outbox_id: id("obx_0198f1c3-9a00-7c3e-b1f3-773c28369172"),
            activations: Vec::new(),
            pending_nodes: Vec::new(),
            structural_exit: Some(ControllerStructuralExitSlot {
                scope_closing_event_id: id(
                    "evt_0198f1c3-9a00-7c3e-b1f3-773c28369173",
                ),
                scope_closing_outbox_id: id(
                    "obx_0198f1c3-9a00-7c3e-b1f3-773c28369173",
                ),
                scope_terminal_event_id: id(
                    "evt_0198f1c3-9a00-7c3e-b1f3-773c28369174",
                ),
                scope_terminal_outbox_id: id(
                    "obx_0198f1c3-9a00-7c3e-b1f3-773c28369174",
                ),
                loop_rollover: None,
            }),
            pending_wake: Some(ControllerPendingWakeSlot {
                orchestration_job_id: id(
                    "job_0198f1c3-9a00-7c3e-b1f3-773c28369175",
                ),
                request_digest: digest('7'),
                node_event_id: id("evt_0198f1c3-9a00-7c3e-b1f3-773c28369176"),
                node_outbox_id: id("obx_0198f1c3-9a00-7c3e-b1f3-773c28369176"),
                job_event_id: id("evt_0198f1c3-9a00-7c3e-b1f3-773c28369177"),
                job_outbox_id: id("obx_0198f1c3-9a00-7c3e-b1f3-773c28369177"),
            }),
            remainder_cancellations: Vec::new(),
        },
    };
    let mut scheduler = repository.begin_scheduler_transaction().await.unwrap();
    let left_settled = match scheduler
        .apply_orchestration_controller_step(left_exit.clone())
        .await
        .unwrap()
    {
        CommandOutcome::Applied(record) => record,
        CommandOutcome::Replayed(_) => panic!("left leg exit must apply"),
    };
    scheduler.commit().await.unwrap();
    assert_eq!(left_settled.settled_scopes.len(), 1);
    assert_eq!(
        left_settled.settled_scopes[0].scope_id,
        opened_fork.created_scopes[0].scope_id
    );
    assert_eq!(left_settled.settled_scopes[0].state, "succeeded");
    assert!(left_settled.woken_nodes.is_empty());
    assert_eq!(
        sqlx::query_scalar::<_, String>(
            "SELECT state FROM insight_platform.run_nodes WHERE tenant_id = $1 AND node_id = $2",
        )
        .bind(TENANT_ID)
        .bind(&opened_fork.pending_nodes[0].node_id)
        .fetch_one(&pool)
        .await
        .unwrap(),
        "pending"
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT count(*) FROM insight_platform.events WHERE tenant_id = $1 AND event_id = ANY($2)",
        )
        .bind(TENANT_ID)
        .bind(
            ["9170", "9171", "9172", "9173", "9174"]
                .map(|suffix| format!("evt_0198f1c3-9a00-7c3e-b1f3-773c2836{suffix}"))
                .to_vec(),
        )
        .fetch_one(&pool)
        .await
        .unwrap(),
        5
    );
    let mut scheduler = repository.begin_scheduler_transaction().await.unwrap();
    assert!(matches!(
        scheduler
            .apply_orchestration_controller_step(left_exit.clone())
            .await
            .unwrap(),
        CommandOutcome::Replayed(record)
            if record.settled_scopes.len() == 1 && record.woken_nodes.is_empty()
    ));
    scheduler.commit().await.unwrap();

    let mut scheduler = repository.begin_scheduler_transaction().await.unwrap();
    let right_claimed = scheduler
        .claim_orchestration_jobs(orchestration_claim_with_ids(
            WORKER_D_ID,
            '8',
            "9180",
            ["9181", "9182", "9183", "9184"],
            ["9185", "9186", "9187"],
        ))
        .await
        .unwrap();
    scheduler.commit().await.unwrap();
    assert_eq!(right_claimed.len(), 1);
    assert_eq!(
        right_claimed[0].job.node_id.as_deref(),
        Some(opened_fork.activations[1].node_id.as_str())
    );
    let right_start = StartOrchestrationJob {
        fence: JobFence {
            tenant_id: TENANT_ID.to_owned(),
            job_id: right_claimed[0].job.job_id.clone(),
            worker_id: id(WORKER_D_ID),
            lease_epoch: right_claimed[0].job.lease_epoch,
            expected_job_version: right_claimed[0].job.version,
            lease_token_digest: digest('8'),
        },
        receipt_id: id("rcp_0198f1c3-9a00-7c3e-b1f3-773c28369188"),
        idempotency_key_digest: digest('9'),
        request_digest: digest('a'),
        receipt_expires_at: Utc::now() + Duration::hours(1),
        job_event_id: id("evt_0198f1c3-9a00-7c3e-b1f3-773c28369189"),
        job_outbox_id: id("obx_0198f1c3-9a00-7c3e-b1f3-773c28369189"),
        node_event_id: id("evt_0198f1c3-9a00-7c3e-b1f3-773c2836918a"),
        node_outbox_id: id("obx_0198f1c3-9a00-7c3e-b1f3-773c2836918a"),
    };
    let mut scheduler = repository.begin_scheduler_transaction().await.unwrap();
    let right_started = match scheduler
        .start_orchestration_job(right_start.clone())
        .await
        .unwrap()
    {
        CommandOutcome::Applied(record) => record,
        CommandOutcome::Replayed(_) => panic!("right leg start must apply"),
    };
    scheduler.commit().await.unwrap();
    let right_exit = ApplyOrchestrationControllerStep {
        fence: JobFence {
            expected_job_version: right_started.version,
            ..right_start.fence.clone()
        },
        plan: runtime_plan(),
        observation: ControllerObservation::None,
        idempotency_key_digest: digest('b'),
        request_digest: digest('c'),
        receipt_expires_at: Utc::now() + Duration::hours(1),
        mutations: ControllerStepMutationIds {
            receipt_id: id("rcp_0198f1c3-9a00-7c3e-b1f3-773c2836918b"),
            quota_entry_ids: ["918c", "918d", "918e", "918f"]
                .map(|suffix| {
                    id(&format!(
                        "qle_0198f1c3-9a00-7c3e-b1f3-773c2836{suffix}"
                    ))
                })
                .to_vec(),
            run_event_id: id("evt_0198f1c3-9a00-7c3e-b1f3-773c28369190"),
            run_outbox_id: id("obx_0198f1c3-9a00-7c3e-b1f3-773c28369190"),
            node_event_id: id("evt_0198f1c3-9a00-7c3e-b1f3-773c28369191"),
            node_outbox_id: id("obx_0198f1c3-9a00-7c3e-b1f3-773c28369191"),
            job_event_id: id("evt_0198f1c3-9a00-7c3e-b1f3-773c28369192"),
            job_outbox_id: id("obx_0198f1c3-9a00-7c3e-b1f3-773c28369192"),
            activations: Vec::new(),
            pending_nodes: Vec::new(),
            structural_exit: Some(ControllerStructuralExitSlot {
                scope_closing_event_id: id(
                    "evt_0198f1c3-9a00-7c3e-b1f3-773c28369193",
                ),
                scope_closing_outbox_id: id(
                    "obx_0198f1c3-9a00-7c3e-b1f3-773c28369193",
                ),
                scope_terminal_event_id: id(
                    "evt_0198f1c3-9a00-7c3e-b1f3-773c28369194",
                ),
                scope_terminal_outbox_id: id(
                    "obx_0198f1c3-9a00-7c3e-b1f3-773c28369194",
                ),
                loop_rollover: None,
            }),
            pending_wake: Some(ControllerPendingWakeSlot {
                orchestration_job_id: id(
                    "job_0198f1c3-9a00-7c3e-b1f3-773c28369195",
                ),
                request_digest: digest('d'),
                node_event_id: id("evt_0198f1c3-9a00-7c3e-b1f3-773c28369196"),
                node_outbox_id: id("obx_0198f1c3-9a00-7c3e-b1f3-773c28369196"),
                job_event_id: id("evt_0198f1c3-9a00-7c3e-b1f3-773c28369197"),
                job_outbox_id: id("obx_0198f1c3-9a00-7c3e-b1f3-773c28369197"),
            }),
            remainder_cancellations: Vec::new(),
        },
    };
    let mut scheduler = repository.begin_scheduler_transaction().await.unwrap();
    let right_settled = match scheduler
        .apply_orchestration_controller_step(right_exit.clone())
        .await
        .unwrap()
    {
        CommandOutcome::Applied(record) => record,
        CommandOutcome::Replayed(_) => panic!("right leg exit must apply"),
    };
    scheduler.commit().await.unwrap();
    assert_eq!(right_settled.settled_scopes.len(), 1);
    assert_eq!(right_settled.woken_nodes.len(), 1);
    assert_eq!(
        right_settled.woken_nodes[0].node_id,
        opened_fork.pending_nodes[0].node_id
    );
    assert_eq!(right_settled.woken_nodes[0].job.state, "ready");
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT count(*) FROM insight_platform.events WHERE tenant_id = $1 AND event_id = ANY($2)",
        )
        .bind(TENANT_ID)
        .bind(
            ["9190", "9191", "9192", "9193", "9194", "9196", "9197"]
                .map(|suffix| format!("evt_0198f1c3-9a00-7c3e-b1f3-773c2836{suffix}"))
                .to_vec(),
        )
        .fetch_one(&pool)
        .await
        .unwrap(),
        7
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT reserved_value FROM insight_platform.quota_accounts WHERE tenant_id = $1 AND quota_account_id = $2",
        )
        .bind(TENANT_ID)
        .bind(QUOTA_ACCOUNT_ID)
        .fetch_one(&pool)
        .await
        .unwrap(),
        reserved_before_fork
    );
    let mut scheduler = repository.begin_scheduler_transaction().await.unwrap();
    assert!(matches!(
        scheduler
            .apply_orchestration_controller_step(right_exit.clone())
            .await
            .unwrap(),
        CommandOutcome::Replayed(record)
            if record.settled_scopes.len() == 1 && record.woken_nodes.len() == 1
    ));
    scheduler.commit().await.unwrap();

    let mut scheduler = repository.begin_scheduler_transaction().await.unwrap();
    let join_claimed = scheduler
        .claim_orchestration_jobs(orchestration_claim_with_ids(
            WORKER_C_ID,
            'e',
            "91a0",
            ["91a1", "91a2", "91a3", "91a4"],
            ["91a5", "91a6", "91a7"],
        ))
        .await
        .unwrap();
    scheduler.commit().await.unwrap();
    assert_eq!(join_claimed.len(), 1);
    assert_eq!(
        join_claimed[0].job.node_id.as_deref(),
        Some(opened_fork.pending_nodes[0].node_id.as_str())
    );
    let join_start = StartOrchestrationJob {
        fence: JobFence {
            tenant_id: TENANT_ID.to_owned(),
            job_id: join_claimed[0].job.job_id.clone(),
            worker_id: id(WORKER_C_ID),
            lease_epoch: join_claimed[0].job.lease_epoch,
            expected_job_version: join_claimed[0].job.version,
            lease_token_digest: digest('e'),
        },
        receipt_id: id("rcp_0198f1c3-9a00-7c3e-b1f3-773c283691a8"),
        idempotency_key_digest: digest('f'),
        request_digest: digest('0'),
        receipt_expires_at: Utc::now() + Duration::hours(1),
        job_event_id: id("evt_0198f1c3-9a00-7c3e-b1f3-773c283691a9"),
        job_outbox_id: id("obx_0198f1c3-9a00-7c3e-b1f3-773c283691a9"),
        node_event_id: id("evt_0198f1c3-9a00-7c3e-b1f3-773c283691aa"),
        node_outbox_id: id("obx_0198f1c3-9a00-7c3e-b1f3-773c283691aa"),
    };
    let mut scheduler = repository.begin_scheduler_transaction().await.unwrap();
    let join_started = match scheduler
        .start_orchestration_job(join_start.clone())
        .await
        .unwrap()
    {
        CommandOutcome::Applied(record) => record,
        CommandOutcome::Replayed(_) => panic!("Join start must apply"),
    };
    scheduler.commit().await.unwrap();
    let join_step = ApplyOrchestrationControllerStep {
        fence: JobFence {
            expected_job_version: join_started.version,
            ..join_start.fence.clone()
        },
        plan: runtime_plan(),
        observation: ControllerObservation::Join {
            children: vec![ChildOutcome::Succeeded, ChildOutcome::Succeeded],
        },
        idempotency_key_digest: digest('1'),
        request_digest: digest('2'),
        receipt_expires_at: Utc::now() + Duration::hours(1),
        mutations: ControllerStepMutationIds {
            receipt_id: id("rcp_0198f1c3-9a00-7c3e-b1f3-773c283691ab"),
            quota_entry_ids: ["91ac", "91ad", "91ae", "91af"]
                .map(|suffix| {
                    id(&format!(
                        "qle_0198f1c3-9a00-7c3e-b1f3-773c2836{suffix}"
                    ))
                })
                .to_vec(),
            run_event_id: id("evt_0198f1c3-9a00-7c3e-b1f3-773c283691b0"),
            run_outbox_id: id("obx_0198f1c3-9a00-7c3e-b1f3-773c283691b0"),
            node_event_id: id("evt_0198f1c3-9a00-7c3e-b1f3-773c283691b1"),
            node_outbox_id: id("obx_0198f1c3-9a00-7c3e-b1f3-773c283691b1"),
            job_event_id: id("evt_0198f1c3-9a00-7c3e-b1f3-773c283691b2"),
            job_outbox_id: id("obx_0198f1c3-9a00-7c3e-b1f3-773c283691b2"),
            activations: vec![ControllerActivationSlot {
                node_execution_id: id("nod_0198f1c3-9a00-7c3e-b1f3-773c283691b3"),
                orchestration_job_id: id(
                    "job_0198f1c3-9a00-7c3e-b1f3-773c283691b4",
                ),
                scope: None,
                node_event_id: id("evt_0198f1c3-9a00-7c3e-b1f3-773c283691b5"),
                node_outbox_id: id("obx_0198f1c3-9a00-7c3e-b1f3-773c283691b5"),
                job_event_id: id("evt_0198f1c3-9a00-7c3e-b1f3-773c283691b6"),
                job_outbox_id: id("obx_0198f1c3-9a00-7c3e-b1f3-773c283691b6"),
            }],
            pending_nodes: Vec::new(),
            structural_exit: None,
            pending_wake: None,
            remainder_cancellations: Vec::new(),
        },
    };
    let mut forged_join_step = join_step.clone();
    forged_join_step.observation = ControllerObservation::Join {
        children: vec![
            ChildOutcome::Succeeded,
            ChildOutcome::Succeeded,
            ChildOutcome::Succeeded,
        ],
    };
    let mut scheduler = repository.begin_scheduler_transaction().await.unwrap();
    assert!(matches!(
        scheduler
            .apply_orchestration_controller_step(forged_join_step)
            .await
            .unwrap_err(),
        RepositoryError::Conflict("exact Join observation")
    ));
    scheduler.rollback().await.unwrap();
    assert_eq!(
        sqlx::query_scalar::<_, String>(
            "SELECT state FROM insight_platform.jobs WHERE tenant_id = $1 AND job_id = $2",
        )
        .bind(TENANT_ID)
        .bind(&join_step.fence.job_id)
        .fetch_one(&pool)
        .await
        .unwrap(),
        "running"
    );
    let mut scheduler = repository.begin_scheduler_transaction().await.unwrap();
    let joined = match scheduler
        .apply_orchestration_controller_step(join_step.clone())
        .await
        .unwrap()
    {
        CommandOutcome::Applied(record) => record,
        CommandOutcome::Replayed(_) => panic!("Join controller step must apply"),
    };
    scheduler.commit().await.unwrap();
    assert_eq!(joined.activations.len(), 1);
    assert_eq!(joined.activations[0].plan_node_key.as_str(), "finish");

    let cancelling_fork = applied(
        run_command!(
            repository,
            request_run_cancel,
            RequestRunCancel {
                audit: audit(TENANT_ID, PRINCIPAL_ID, "91c0", '3', '4'),
                run_id: fork_admission.run_id.clone(),
                expected_run_version: joined.run.version,
                expected_cancel_generation: 0,
                reason_code: "fixture_cleanup".to_owned(),
            }
        )
        .unwrap(),
    );
    assert_eq!(cancelling_fork.state, "cancelling");
    let mut scheduler = repository.begin_scheduler_transaction().await.unwrap();
    let fork_cleanup = scheduler
        .drive_orchestration_convergence(orchestration_convergence(
            ["91c1", "91c2", "91c3", "91c4"],
            ["91c5", "91c6", "91c7", "91c8", "91c9", "91ca"],
        ))
        .await
        .unwrap();
    scheduler.commit().await.unwrap();
    assert_eq!(fork_cleanup.len(), 1);
    assert_eq!(fork_cleanup[0].completed.run.state, "cancelled");

    for (run_id, audit_suffix, idempotency, request) in [
        (
            id("run_0198f1c3-9a00-7c3e-b1f3-773c28367010"),
            "9300",
            '0',
            '1',
        ),
        (
            id("run_0198f1c3-9a00-7c3e-b1f3-773c28367600"),
            "9310",
            '2',
            '3',
        ),
        (
            id("run_0198f1c3-9a00-7c3e-b1f3-773c28367700"),
            "9320",
            '4',
            '5',
        ),
    ] {
        let cancelled = applied(
            run_command!(
                repository,
                request_run_cancel,
                RequestRunCancel {
                    audit: audit(
                        TENANT_ID,
                        PRINCIPAL_ID,
                        audit_suffix,
                        idempotency,
                        request,
                    ),
                    run_id,
                    expected_run_version: 2,
                    expected_cancel_generation: 0,
                    reason_code: "fixture_cleanup".to_owned(),
                }
            )
            .unwrap(),
        );
        assert_eq!(cancelled.state, "cancelling");
    }
    for (quota_suffixes, event_suffixes) in [
        (
            ["9341", "9342", "9343", "9344"],
            ["9345", "9346", "9347", "9348", "9349", "934a"],
        ),
        (
            ["9351", "9352", "9353", "9354"],
            ["9355", "9356", "9357", "9358", "9359", "935a"],
        ),
        (
            ["9361", "9362", "9363", "9364"],
            ["9365", "9366", "9367", "9368", "9369", "936a"],
        ),
    ] {
        let mut scheduler = repository.begin_scheduler_transaction().await.unwrap();
        let cleaned = scheduler
            .drive_orchestration_convergence(orchestration_convergence(
                quota_suffixes,
                event_suffixes,
            ))
            .await
            .unwrap();
        scheduler.commit().await.unwrap();
        assert_eq!(cleaned.len(), 1);
        assert_eq!(cleaned[0].completed.run.state, "cancelled");
    }
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT reserved_value FROM insight_platform.quota_accounts WHERE tenant_id = $1 AND quota_account_id = $2",
        )
        .bind(TENANT_ID)
        .bind(QUOTA_ACCOUNT_ID)
        .fetch_one(&pool)
        .await
        .unwrap(),
        0
    );

    let mut quorum_admission = admission(
        AdmissionIds {
            run: "run_0198f1c3-9a00-7c3e-b1f3-773c28369200",
            scope: "scp_0198f1c3-9a00-7c3e-b1f3-773c28369201",
            node: "nod_0198f1c3-9a00-7c3e-b1f3-773c28369202",
            job: "job_0198f1c3-9a00-7c3e-b1f3-773c28369203",
            value: "val_0198f1c3-9a00-7c3e-b1f3-773c28369204",
        },
        audit(TENANT_ID, PRINCIPAL_ID, "9205", '5', '6'),
        bindings.clone(),
        Utc::now() + Duration::minutes(5),
    );
    quorum_admission.entry_plan_node_key =
        PlanNodeKey::new("quorum_fork".to_owned()).unwrap();
    quorum_admission.entry_node_kind = PlanNodeKind::Fork;
    applied(run_command!(repository, admit_run, quorum_admission.clone()).unwrap());
    let mut scheduler = repository.begin_scheduler_transaction().await.unwrap();
    let quorum_fork_claimed = scheduler
        .claim_orchestration_jobs(orchestration_claim_with_ids(
            WORKER_D_ID,
            '7',
            "9210",
            ["9211", "9212", "9213", "9214"],
            ["9215", "9216", "9217"],
        ))
        .await
        .unwrap();
    scheduler.commit().await.unwrap();
    assert_eq!(quorum_fork_claimed.len(), 1);
    let quorum_fork_start = start_claimed_orchestration(
        &quorum_fork_claimed[0],
        WORKER_D_ID,
        '7',
        "9218",
        "9219",
        "921a",
        '8',
        '9',
    );
    let mut scheduler = repository.begin_scheduler_transaction().await.unwrap();
    let quorum_fork_started = match scheduler
        .start_orchestration_job(quorum_fork_start.clone())
        .await
        .unwrap()
    {
        CommandOutcome::Applied(record) => record,
        CommandOutcome::Replayed(_) => panic!("Quorum Fork start must apply"),
    };
    scheduler.commit().await.unwrap();
    let quorum_fork_step = fork_controller_step(
        &quorum_fork_start,
        quorum_fork_started.version,
        "9220",
        ["9221", "9222", "9223", "9224"],
        ["9225", "9226", "9227"],
        [
            ["9230", "9231", "9232", "9233", "9234", "9235"],
            ["9240", "9241", "9242", "9243", "9244", "9245"],
        ],
        ["9250", "9251"],
        'a',
        'b',
    );
    let mut scheduler = repository.begin_scheduler_transaction().await.unwrap();
    let opened_quorum = match scheduler
        .apply_orchestration_controller_step(quorum_fork_step)
        .await
        .unwrap()
    {
        CommandOutcome::Applied(record) => record,
        CommandOutcome::Replayed(_) => panic!("Quorum Fork open must apply"),
    };
    scheduler.commit().await.unwrap();
    assert_eq!(opened_quorum.activations.len(), 2);

    let mut scheduler = repository.begin_scheduler_transaction().await.unwrap();
    let first_quorum_leg = scheduler
        .claim_orchestration_jobs(orchestration_claim_with_ids(
            WORKER_D_ID,
            'c',
            "9260",
            ["9261", "9262", "9263", "9264"],
            ["9265", "9266", "9267"],
        ))
        .await
        .expect("first Quorum leg claim must succeed");
    scheduler.commit().await.unwrap();
    let mut scheduler = repository.begin_scheduler_transaction().await.unwrap();
    let second_quorum_leg = scheduler
        .claim_orchestration_jobs(orchestration_claim_with_ids(
            WORKER_D_ID,
            'd',
            "9270",
            ["9271", "9272", "9273", "9274"],
            ["9275", "9276", "9277"],
        ))
        .await
        .expect("second Quorum leg claim must succeed");
    scheduler.commit().await.unwrap();
    let quorum_legs = first_quorum_leg
        .into_iter()
        .chain(second_quorum_leg)
        .collect::<Vec<_>>();
    assert_eq!(quorum_legs.len(), 2);
    let quorum_left_claimed = quorum_legs
        .iter()
        .find(|claimed| {
            claimed.job.node_id.as_deref() == Some(opened_quorum.activations[0].node_id.as_str())
        })
        .unwrap()
        .clone();
    let quorum_right_claimed = quorum_legs
        .iter()
        .find(|claimed| {
            claimed.job.node_id.as_deref() == Some(opened_quorum.activations[1].node_id.as_str())
        })
        .unwrap()
        .clone();
    let quorum_left_start = start_claimed_orchestration(
        &quorum_left_claimed,
        WORKER_D_ID,
        'c',
        "9280",
        "9281",
        "9282",
        'e',
        'f',
    );
    let quorum_right_start = start_claimed_orchestration(
        &quorum_right_claimed,
        WORKER_D_ID,
        'd',
        "9283",
        "9284",
        "9285",
        '0',
        '1',
    );
    let mut scheduler = repository.begin_scheduler_transaction().await.unwrap();
    let quorum_left_started = match scheduler
        .start_orchestration_job(quorum_left_start.clone())
        .await
        .unwrap()
    {
        CommandOutcome::Applied(record) => record,
        CommandOutcome::Replayed(_) => panic!("Quorum left start must apply"),
    };
    scheduler.commit().await.unwrap();
    let mut scheduler = repository.begin_scheduler_transaction().await.unwrap();
    let quorum_right_started = match scheduler
        .start_orchestration_job(quorum_right_start.clone())
        .await
        .unwrap()
    {
        CommandOutcome::Applied(record) => record,
        CommandOutcome::Replayed(_) => panic!("Quorum right start must apply"),
    };
    scheduler.commit().await.unwrap();

    let quorum_left_exit = parallel_leg_exit_step(
        &quorum_left_start,
        quorum_left_started.version,
        "9290",
        ["9291", "9292", "9293", "9294"],
        ["9295", "9296", "9297"],
        ["9298", "9299"],
        "929a",
        ["929b", "929c"],
        '2',
        '3',
        '4',
    );
    let mut scheduler = repository.begin_scheduler_transaction().await.unwrap();
    let quorum_reached = match scheduler
        .apply_orchestration_controller_step(quorum_left_exit)
        .await
        .unwrap()
    {
        CommandOutcome::Applied(record) => record,
        CommandOutcome::Replayed(_) => panic!("Quorum first leg exit must apply"),
    };
    scheduler.commit().await.unwrap();
    assert_eq!(quorum_reached.woken_nodes.len(), 1);
    assert_eq!(quorum_reached.woken_nodes[0].plan_node_key.as_str(), "quorum_join");
    assert_eq!(
        sqlx::query_scalar::<_, String>(
            "SELECT state FROM insight_platform.run_nodes WHERE tenant_id = $1 AND node_id = $2",
        )
        .bind(TENANT_ID)
        .bind(&opened_quorum.activations[1].node_id)
        .fetch_one(&pool)
        .await
        .unwrap(),
        "running"
    );

    let mut scheduler = repository.begin_scheduler_transaction().await.unwrap();
    let quorum_join_claimed = scheduler
        .claim_orchestration_jobs(orchestration_claim_with_ids(
            WORKER_C_ID,
            '5',
            "92a0",
            ["92a1", "92a2", "92a3", "92a4"],
            ["92a5", "92a6", "92a7"],
        ))
        .await
        .expect("Quorum Join claim must succeed");
    scheduler.commit().await.unwrap();
    assert_eq!(quorum_join_claimed.len(), 1);
    assert_eq!(
        quorum_join_claimed[0].job.node_id.as_deref(),
        Some(opened_quorum.pending_nodes[0].node_id.as_str())
    );
    let quorum_join_start = start_claimed_orchestration(
        &quorum_join_claimed[0],
        WORKER_C_ID,
        '5',
        "92a8",
        "92a9",
        "92aa",
        '6',
        '7',
    );
    let mut scheduler = repository.begin_scheduler_transaction().await.unwrap();
    let quorum_join_started = match scheduler
        .start_orchestration_job(quorum_join_start.clone())
        .await
        .unwrap()
    {
        CommandOutcome::Applied(record) => record,
        CommandOutcome::Replayed(_) => panic!("Quorum Join start must apply"),
    };
    scheduler.commit().await.unwrap();
    let quorum_join_step = ApplyOrchestrationControllerStep {
        fence: JobFence {
            expected_job_version: quorum_join_started.version,
            ..quorum_join_start.fence.clone()
        },
        plan: runtime_plan(),
        observation: ControllerObservation::Join {
            children: vec![ChildOutcome::Succeeded, ChildOutcome::Active],
        },
        idempotency_key_digest: digest('8'),
        request_digest: digest('9'),
        receipt_expires_at: Utc::now() + Duration::hours(1),
        mutations: ControllerStepMutationIds {
            receipt_id: fixture_platform_id("rcp", "92b0"),
            quota_entry_ids: ["92b1", "92b2", "92b3", "92b4"]
                .map(|suffix| fixture_platform_id("qle", suffix))
                .to_vec(),
            run_event_id: fixture_platform_id("evt", "92b5"),
            run_outbox_id: fixture_platform_id("obx", "92b5"),
            node_event_id: fixture_platform_id("evt", "92b6"),
            node_outbox_id: fixture_platform_id("obx", "92b6"),
            job_event_id: fixture_platform_id("evt", "92b7"),
            job_outbox_id: fixture_platform_id("obx", "92b7"),
            activations: vec![ControllerActivationSlot {
                node_execution_id: fixture_platform_id("nod", "92b8"),
                orchestration_job_id: fixture_platform_id("job", "92b9"),
                scope: None,
                node_event_id: fixture_platform_id("evt", "92ba"),
                node_outbox_id: fixture_platform_id("obx", "92ba"),
                job_event_id: fixture_platform_id("evt", "92bb"),
                job_outbox_id: fixture_platform_id("obx", "92bb"),
            }],
            pending_nodes: Vec::new(),
            structural_exit: None,
            pending_wake: None,
            remainder_cancellations: Vec::new(),
        },
    };
    let loaded_quorum_facts = repository
        .load_controller_facts(&quorum_join_step.fence, &quorum_join_step.plan)
        .await
        .unwrap();
    assert!(matches!(
        loaded_quorum_facts,
        ControllerFacts::Committed {
            observation: ControllerObservation::Join { ref children },
            ..
        } if children == &vec![ChildOutcome::Succeeded, ChildOutcome::Active]
    ));
    let mut scheduler = repository.begin_scheduler_transaction().await.unwrap();
    let quorum_joined = match scheduler
        .apply_orchestration_controller_step(quorum_join_step)
        .await
        .unwrap()
    {
        CommandOutcome::Applied(record) => record,
        CommandOutcome::Replayed(_) => panic!("Quorum Join step must apply"),
    };
    scheduler.commit().await.unwrap();
    assert_eq!(quorum_joined.activations[0].plan_node_key.as_str(), "finish");

    let quorum_right_exit = parallel_leg_exit_step(
        &quorum_right_start,
        quorum_right_started.version,
        "92c0",
        ["92c1", "92c2", "92c3", "92c4"],
        ["92c5", "92c6", "92c7"],
        ["92c8", "92c9"],
        "92ca",
        ["92cb", "92cc"],
        'a',
        'b',
        'c',
    );
    let mut scheduler = repository.begin_scheduler_transaction().await.unwrap();
    let drained_remainder = match scheduler
        .apply_orchestration_controller_step(quorum_right_exit.clone())
        .await
        .unwrap()
    {
        CommandOutcome::Applied(record) => record,
        CommandOutcome::Replayed(_) => panic!("Quorum drained leg exit must apply"),
    };
    scheduler.commit().await.unwrap();
    assert_eq!(drained_remainder.settled_scopes.len(), 1);
    assert!(drained_remainder.woken_nodes.is_empty());
    let mut scheduler = repository.begin_scheduler_transaction().await.unwrap();
    assert!(matches!(
        scheduler
            .apply_orchestration_controller_step(quorum_right_exit)
            .await
            .unwrap(),
        CommandOutcome::Replayed(record)
            if record.settled_scopes.len() == 1 && record.woken_nodes.is_empty()
    ));
    scheduler.commit().await.unwrap();

    let cancelling_quorum = applied(
        run_command!(
            repository,
            request_run_cancel,
            RequestRunCancel {
                audit: audit(TENANT_ID, PRINCIPAL_ID, "92d0", 'd', 'e'),
                run_id: quorum_admission.run_id.clone(),
                expected_run_version: drained_remainder.run.version,
                expected_cancel_generation: 0,
                reason_code: "fixture_cleanup".to_owned(),
            }
        )
        .unwrap(),
    );
    assert_eq!(cancelling_quorum.state, "cancelling");
    let mut scheduler = repository.begin_scheduler_transaction().await.unwrap();
    let quorum_cleanup = scheduler
        .drive_orchestration_convergence(orchestration_convergence(
            ["92d1", "92d2", "92d3", "92d4"],
            ["92d5", "92d6", "92d7", "92d8", "92d9", "92da"],
        ))
        .await
        .unwrap();
    scheduler.commit().await.unwrap();
    assert_eq!(quorum_cleanup.len(), 1);
    assert_eq!(quorum_cleanup[0].completed.run.state, "cancelled");

    let mut cancel_admission = admission(
        AdmissionIds {
            run: "run_0198f1c3-9a00-7c3e-b1f3-773c28369400",
            scope: "scp_0198f1c3-9a00-7c3e-b1f3-773c28369401",
            node: "nod_0198f1c3-9a00-7c3e-b1f3-773c28369402",
            job: "job_0198f1c3-9a00-7c3e-b1f3-773c28369403",
            value: "val_0198f1c3-9a00-7c3e-b1f3-773c28369404",
        },
        audit(TENANT_ID, PRINCIPAL_ID, "9405", '5', '6'),
        bindings.clone(),
        Utc::now() + Duration::minutes(5),
    );
    cancel_admission.entry_plan_node_key = PlanNodeKey::new("cancel_fork".to_owned()).unwrap();
    cancel_admission.entry_node_kind = PlanNodeKind::Fork;
    applied(run_command!(repository, admit_run, cancel_admission.clone()).unwrap());

    let mut scheduler = repository.begin_scheduler_transaction().await.unwrap();
    let cancel_fork_claimed = scheduler
        .claim_orchestration_jobs(orchestration_claim_with_ids(
            WORKER_D_ID,
            '7',
            "9410",
            ["9411", "9412", "9413", "9414"],
            ["9415", "9416", "9417"],
        ))
        .await
        .unwrap();
    scheduler.commit().await.unwrap();
    assert_eq!(cancel_fork_claimed.len(), 1);
    let cancel_fork_start = start_claimed_orchestration(
        &cancel_fork_claimed[0],
        WORKER_D_ID,
        '7',
        "9418",
        "9419",
        "941a",
        '8',
        '9',
    );
    let mut scheduler = repository.begin_scheduler_transaction().await.unwrap();
    let cancel_fork_started = match scheduler
        .start_orchestration_job(cancel_fork_start.clone())
        .await
        .unwrap()
    {
        CommandOutcome::Applied(record) => record,
        CommandOutcome::Replayed(_) => panic!("Cancel Fork start must apply"),
    };
    scheduler.commit().await.unwrap();
    let cancel_fork_step = fork_controller_step(
        &cancel_fork_start,
        cancel_fork_started.version,
        "9420",
        ["9421", "9422", "9423", "9424"],
        ["9425", "9426", "9427"],
        [
            ["9430", "9431", "9432", "9433", "9434", "9435"],
            ["9440", "9441", "9442", "9443", "9444", "9445"],
        ],
        ["9450", "9451"],
        'a',
        'b',
    );
    let mut scheduler = repository.begin_scheduler_transaction().await.unwrap();
    let opened_cancel = match scheduler
        .apply_orchestration_controller_step(cancel_fork_step)
        .await
        .unwrap()
    {
        CommandOutcome::Applied(record) => record,
        CommandOutcome::Replayed(_) => panic!("Cancel Fork open must apply"),
    };
    scheduler.commit().await.unwrap();
    assert_eq!(opened_cancel.activations.len(), 2);

    let mut scheduler = repository.begin_scheduler_transaction().await.unwrap();
    let first_cancel_leg = scheduler
        .claim_orchestration_jobs(orchestration_claim_with_ids(
            WORKER_D_ID,
            'c',
            "9460",
            ["9461", "9462", "9463", "9464"],
            ["9465", "9466", "9467"],
        ))
        .await
        .expect("first Cancel leg claim must succeed");
    scheduler.commit().await.unwrap();
    let mut scheduler = repository.begin_scheduler_transaction().await.unwrap();
    let second_cancel_leg = scheduler
        .claim_orchestration_jobs(orchestration_claim_with_ids(
            WORKER_D_ID,
            'd',
            "9470",
            ["9471", "9472", "9473", "9474"],
            ["9475", "9476", "9477"],
        ))
        .await
        .expect("second Cancel leg claim must succeed");
    scheduler.commit().await.unwrap();
    let cancel_legs = first_cancel_leg
        .into_iter()
        .chain(second_cancel_leg)
        .collect::<Vec<_>>();
    assert_eq!(cancel_legs.len(), 2);
    let cancel_left_claimed = cancel_legs
        .iter()
        .find(|claimed| {
            claimed.job.node_id.as_deref() == Some(opened_cancel.activations[0].node_id.as_str())
        })
        .unwrap()
        .clone();
    let cancel_right_claimed = cancel_legs
        .iter()
        .find(|claimed| {
            claimed.job.node_id.as_deref() == Some(opened_cancel.activations[1].node_id.as_str())
        })
        .unwrap()
        .clone();
    let cancel_left_start = start_claimed_orchestration(
        &cancel_left_claimed,
        WORKER_D_ID,
        'c',
        "9480",
        "9481",
        "9482",
        'e',
        'f',
    );
    let cancel_right_start = start_claimed_orchestration(
        &cancel_right_claimed,
        WORKER_D_ID,
        'd',
        "9483",
        "9484",
        "9485",
        '0',
        '1',
    );
    let mut scheduler = repository.begin_scheduler_transaction().await.unwrap();
    let cancel_left_started = match scheduler
        .start_orchestration_job(cancel_left_start.clone())
        .await
        .unwrap()
    {
        CommandOutcome::Applied(record) => record,
        CommandOutcome::Replayed(_) => panic!("Cancel left start must apply"),
    };
    scheduler.commit().await.unwrap();
    let mut scheduler = repository.begin_scheduler_transaction().await.unwrap();
    let cancel_right_started = match scheduler
        .start_orchestration_job(cancel_right_start)
        .await
        .unwrap()
    {
        CommandOutcome::Applied(record) => record,
        CommandOutcome::Replayed(_) => panic!("Cancel right start must apply"),
    };
    scheduler.commit().await.unwrap();
    assert_eq!(cancel_right_started.state, "running");

    let mut cancel_left_exit = parallel_leg_exit_step(
        &cancel_left_start,
        cancel_left_started.version,
        "9490",
        ["9491", "9492", "9493", "9494"],
        ["9495", "9496", "9497"],
        ["9498", "9499"],
        "949a",
        ["949b", "949c"],
        '2',
        '3',
        '4',
    );
    cancel_left_exit.mutations.remainder_cancellations = vec![remainder_cancellation_slot(
        fixture_platform_id("scp", "9442"),
        ["949d", "949e", "949f", "94a0"],
        ["94a1", "94a2", "94a3", "94a4", "94a5"],
    )];
    let cancel_requirements = repository
        .load_controller_mutation_requirements(
            &cancel_left_exit.fence,
            &cancel_left_exit.plan,
            &cancel_left_exit.observation,
        )
        .await
        .unwrap();
    assert_eq!(
        cancel_requirements.remainder_cancellation_scope_ids,
        vec![id(&opened_cancel.created_scopes[1].scope_id)]
    );
    let mut scheduler = repository.begin_scheduler_transaction().await.unwrap();
    let cancelled_remainder = match scheduler
        .apply_orchestration_controller_step(cancel_left_exit.clone())
        .await
        .unwrap()
    {
        CommandOutcome::Applied(record) => record,
        CommandOutcome::Replayed(_) => panic!("Cancel first leg exit must apply"),
    };
    scheduler.commit().await.unwrap();
    assert_eq!(cancelled_remainder.settled_scopes.len(), 1);
    assert_eq!(cancelled_remainder.woken_nodes.len(), 1);
    assert_eq!(
        cancelled_remainder.woken_nodes[0].plan_node_key.as_str(),
        "cancel_join"
    );
    assert_eq!(cancelled_remainder.cancelled_remainders.len(), 1);
    let cancelled_right = &cancelled_remainder.cancelled_remainders[0];
    assert_eq!(cancelled_right.scope.scope_id, opened_cancel.created_scopes[1].scope_id);
    assert_eq!(cancelled_right.scope.state, "cancelled");
    assert_eq!(cancelled_right.reason_code, "join_remainder_cancelled");
    assert_eq!(cancelled_right.node_id, opened_cancel.activations[1].node_id);
    assert!(cancelled_right.node_cancelling_version.is_some());
    assert_eq!(cancelled_right.job.job_id, cancel_right_claimed.job.job_id);
    assert_eq!(cancelled_right.job.state, "cancelled");
    assert_eq!(
        cancelled_right.settled_quota_account_ids,
        vec![QUOTA_ACCOUNT_ID]
    );
    assert_eq!(cancelled_remainder.run.active_work_count, 0);
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT reserved_value FROM insight_platform.quota_accounts WHERE tenant_id = $1 AND quota_account_id = $2",
        )
        .bind(TENANT_ID)
        .bind(QUOTA_ACCOUNT_ID)
        .fetch_one(&pool)
        .await
        .unwrap(),
        0
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            r#"
            SELECT count(*) FROM insight_platform.events
            WHERE tenant_id = $1 AND event_id = ANY($2)
            "#,
        )
        .bind(TENANT_ID)
        .bind(
            ["94a1", "94a2", "94a3", "94a4", "94a5"]
                .map(|suffix| format!("evt_0198f1c3-9a00-7c3e-b1f3-773c2836{suffix}"))
                .to_vec(),
        )
        .fetch_one(&pool)
        .await
        .unwrap(),
        5
    );

    let mut scheduler = repository.begin_scheduler_transaction().await.unwrap();
    assert!(matches!(
        scheduler
            .apply_orchestration_controller_step(cancel_left_exit.clone())
            .await
            .unwrap(),
        CommandOutcome::Replayed(record)
            if record.cancelled_remainders.len() == 1
                && record.woken_nodes.len() == 1
                && record.cancelled_remainders[0].job.state == "cancelled"
    ));
    scheduler.commit().await.unwrap();
    let mut conflicting_cancel_replay = cancel_left_exit;
    conflicting_cancel_replay.mutations.remainder_cancellations[0]
        .node_terminal_event_id = fixture_platform_id("evt", "94a6");
    let mut scheduler = repository.begin_scheduler_transaction().await.unwrap();
    assert!(matches!(
        scheduler
            .apply_orchestration_controller_step(conflicting_cancel_replay)
            .await,
        Err(RepositoryError::IdempotencyConflict)
    ));
    scheduler.rollback().await.unwrap();

    let mut scheduler = repository.begin_scheduler_transaction().await.unwrap();
    let cancel_join_claimed = scheduler
        .claim_orchestration_jobs(orchestration_claim_with_ids(
            WORKER_C_ID,
            '5',
            "94b0",
            ["94b1", "94b2", "94b3", "94b4"],
            ["94b5", "94b6", "94b7"],
        ))
        .await
        .expect("Cancel Join claim must succeed");
    scheduler.commit().await.unwrap();
    assert_eq!(cancel_join_claimed.len(), 1);
    assert_eq!(
        cancel_join_claimed[0].job.node_id.as_deref(),
        Some(opened_cancel.pending_nodes[0].node_id.as_str())
    );
    let cancel_join_start = start_claimed_orchestration(
        &cancel_join_claimed[0],
        WORKER_C_ID,
        '5',
        "94b8",
        "94b9",
        "94ba",
        '6',
        '7',
    );
    let mut scheduler = repository.begin_scheduler_transaction().await.unwrap();
    let cancel_join_started = match scheduler
        .start_orchestration_job(cancel_join_start.clone())
        .await
        .unwrap()
    {
        CommandOutcome::Applied(record) => record,
        CommandOutcome::Replayed(_) => panic!("Cancel Join start must apply"),
    };
    scheduler.commit().await.unwrap();
    let cancel_join_step = ApplyOrchestrationControllerStep {
        fence: JobFence {
            expected_job_version: cancel_join_started.version,
            ..cancel_join_start.fence.clone()
        },
        plan: runtime_plan(),
        observation: ControllerObservation::Join {
            children: vec![ChildOutcome::Succeeded, ChildOutcome::Cancelled],
        },
        idempotency_key_digest: digest('8'),
        request_digest: digest('9'),
        receipt_expires_at: Utc::now() + Duration::hours(1),
        mutations: ControllerStepMutationIds {
            receipt_id: fixture_platform_id("rcp", "94c0"),
            quota_entry_ids: ["94c1", "94c2", "94c3", "94c4"]
                .map(|suffix| fixture_platform_id("qle", suffix))
                .to_vec(),
            run_event_id: fixture_platform_id("evt", "94c5"),
            run_outbox_id: fixture_platform_id("obx", "94c5"),
            node_event_id: fixture_platform_id("evt", "94c6"),
            node_outbox_id: fixture_platform_id("obx", "94c6"),
            job_event_id: fixture_platform_id("evt", "94c7"),
            job_outbox_id: fixture_platform_id("obx", "94c7"),
            activations: vec![ControllerActivationSlot {
                node_execution_id: fixture_platform_id("nod", "94c8"),
                orchestration_job_id: fixture_platform_id("job", "94c9"),
                scope: None,
                node_event_id: fixture_platform_id("evt", "94ca"),
                node_outbox_id: fixture_platform_id("obx", "94ca"),
                job_event_id: fixture_platform_id("evt", "94cb"),
                job_outbox_id: fixture_platform_id("obx", "94cb"),
            }],
            pending_nodes: Vec::new(),
            structural_exit: None,
            pending_wake: None,
            remainder_cancellations: Vec::new(),
        },
    };
    let mut scheduler = repository.begin_scheduler_transaction().await.unwrap();
    let cancel_joined = match scheduler
        .apply_orchestration_controller_step(cancel_join_step)
        .await
        .unwrap()
    {
        CommandOutcome::Applied(record) => record,
        CommandOutcome::Replayed(_) => panic!("Cancel Join step must apply"),
    };
    scheduler.commit().await.unwrap();
    assert_eq!(cancel_joined.activations.len(), 1);
    assert_eq!(cancel_joined.activations[0].plan_node_key.as_str(), "finish");

    let cancelling_cancel_run = applied(
        run_command!(
            repository,
            request_run_cancel,
            RequestRunCancel {
                audit: audit(TENANT_ID, PRINCIPAL_ID, "94d0", 'd', 'e'),
                run_id: cancel_admission.run_id.clone(),
                expected_run_version: cancel_joined.run.version,
                expected_cancel_generation: 0,
                reason_code: "fixture_cleanup".to_owned(),
            }
        )
        .unwrap(),
    );
    assert_eq!(cancelling_cancel_run.state, "cancelling");
    let mut scheduler = repository.begin_scheduler_transaction().await.unwrap();
    let cancel_cleanup = scheduler
        .drive_orchestration_convergence(orchestration_convergence(
            ["94d1", "94d2", "94d3", "94d4"],
            ["94d5", "94d6", "94d7", "94d8", "94d9", "94da"],
        ))
        .await
        .unwrap();
    scheduler.commit().await.unwrap();
    assert_eq!(cancel_cleanup.len(), 1);
    assert_eq!(cancel_cleanup[0].completed.run.state, "cancelled");

    let mut failed_join_admission = admission(
        AdmissionIds {
            run: "run_0198f1c3-9a00-7c3e-b1f3-773c28369500",
            scope: "scp_0198f1c3-9a00-7c3e-b1f3-773c28369501",
            node: "nod_0198f1c3-9a00-7c3e-b1f3-773c28369502",
            job: "job_0198f1c3-9a00-7c3e-b1f3-773c28369503",
            value: "val_0198f1c3-9a00-7c3e-b1f3-773c28369504",
        },
        audit(TENANT_ID, PRINCIPAL_ID, "9505", '5', '6'),
        bindings.clone(),
        Utc::now() + Duration::minutes(5),
    );
    failed_join_admission.entry_plan_node_key = PlanNodeKey::new("fork".to_owned()).unwrap();
    failed_join_admission.entry_node_kind = PlanNodeKind::Fork;
    applied(
        run_command!(
            repository,
            admit_run,
            failed_join_admission.clone()
        )
        .unwrap(),
    );
    let mut scheduler = repository.begin_scheduler_transaction().await.unwrap();
    let failed_fork_claimed = scheduler
        .claim_orchestration_jobs(orchestration_claim_with_ids(
            WORKER_D_ID,
            '7',
            "9510",
            ["9511", "9512", "9513", "9514"],
            ["9515", "9516", "9517"],
        ))
        .await
        .unwrap();
    scheduler.commit().await.unwrap();
    let failed_fork_start = start_claimed_orchestration(
        &failed_fork_claimed[0],
        WORKER_D_ID,
        '7',
        "9518",
        "9519",
        "951a",
        '8',
        '9',
    );
    let mut scheduler = repository.begin_scheduler_transaction().await.unwrap();
    let failed_fork_started = match scheduler
        .start_orchestration_job(failed_fork_start.clone())
        .await
        .unwrap()
    {
        CommandOutcome::Applied(record) => record,
        CommandOutcome::Replayed(_) => panic!("failed-path Fork start must apply"),
    };
    scheduler.commit().await.unwrap();
    let failed_fork_step = fork_controller_step(
        &failed_fork_start,
        failed_fork_started.version,
        "9520",
        ["9521", "9522", "9523", "9524"],
        ["9525", "9526", "9527"],
        [
            ["9530", "9531", "9532", "9533", "9534", "9535"],
            ["9540", "9541", "9542", "9543", "9544", "9545"],
        ],
        ["9550", "9551"],
        'a',
        'b',
    );
    let mut scheduler = repository.begin_scheduler_transaction().await.unwrap();
    let opened_failed_fork = match scheduler
        .apply_orchestration_controller_step(failed_fork_step)
        .await
        .unwrap()
    {
        CommandOutcome::Applied(record) => record,
        CommandOutcome::Replayed(_) => panic!("failed-path Fork open must apply"),
    };
    scheduler.commit().await.unwrap();

    let mut scheduler = repository.begin_scheduler_transaction().await.unwrap();
    let first_failed_leg = scheduler
        .claim_orchestration_jobs(orchestration_claim_with_ids(
            WORKER_D_ID,
            'c',
            "9560",
            ["9561", "9562", "9563", "9564"],
            ["9565", "9566", "9567"],
        ))
        .await
        .unwrap();
    scheduler.commit().await.unwrap();
    let mut scheduler = repository.begin_scheduler_transaction().await.unwrap();
    let second_failed_leg = scheduler
        .claim_orchestration_jobs(orchestration_claim_with_ids(
            WORKER_D_ID,
            'd',
            "9570",
            ["9571", "9572", "9573", "9574"],
            ["9575", "9576", "9577"],
        ))
        .await
        .unwrap();
    scheduler.commit().await.unwrap();
    let failed_legs = first_failed_leg
        .into_iter()
        .chain(second_failed_leg)
        .collect::<Vec<_>>();
    assert_eq!(failed_legs.len(), 2);
    let failed_left_claimed = failed_legs
        .iter()
        .find(|claimed| {
            claimed.job.node_id.as_deref()
                == Some(opened_failed_fork.activations[0].node_id.as_str())
        })
        .unwrap()
        .clone();
    let failed_right_claimed = failed_legs
        .iter()
        .find(|claimed| {
            claimed.job.node_id.as_deref()
                == Some(opened_failed_fork.activations[1].node_id.as_str())
        })
        .unwrap()
        .clone();
    let failed_left_start = start_claimed_orchestration(
        &failed_left_claimed,
        WORKER_D_ID,
        'c',
        "9580",
        "9581",
        "9582",
        'e',
        'f',
    );
    let failed_right_start = start_claimed_orchestration(
        &failed_right_claimed,
        WORKER_D_ID,
        'd',
        "9583",
        "9584",
        "9585",
        '0',
        '1',
    );
    let mut scheduler = repository.begin_scheduler_transaction().await.unwrap();
    let failed_left_started = match scheduler
        .start_orchestration_job(failed_left_start.clone())
        .await
        .unwrap()
    {
        CommandOutcome::Applied(record) => record,
        CommandOutcome::Replayed(_) => panic!("failed left start must apply"),
    };
    scheduler.commit().await.unwrap();
    let mut scheduler = repository.begin_scheduler_transaction().await.unwrap();
    let failed_right_started = match scheduler
        .start_orchestration_job(failed_right_start)
        .await
        .unwrap()
    {
        CommandOutcome::Applied(record) => record,
        CommandOutcome::Replayed(_) => panic!("failed right start must apply"),
    };
    scheduler.commit().await.unwrap();
    assert_eq!(failed_right_started.state, "running");

    let committed_failure = Failure {
        code: FailureCode::Platform {
            code: PlatformFailureCode::DependencyUnavailable,
        },
        class: FailureClass::Dependency,
        retryability: Retryability::Never,
        safe_message: Some("A required dependency did not complete.".to_owned()),
        details_ref: None,
        source: FailureSource::Dependency,
    };
    let failed_left = FailOrchestrationJob {
        fence: JobFence {
            expected_job_version: failed_left_started.version,
            ..failed_left_start.fence.clone()
        },
        plan: runtime_plan(),
        cause: OrchestrationFailureCause::Committed {
            failure: committed_failure.clone(),
        },
        derived_expression: None,
        idempotency_key_digest: digest('2'),
        request_digest: digest('3'),
        receipt_expires_at: Utc::now() + Duration::hours(1),
        mutations: ControllerStepMutationIds {
            receipt_id: fixture_platform_id("rcp", "9590"),
            quota_entry_ids: ["9591", "9592", "9593", "9594"]
                .map(|suffix| fixture_platform_id("qle", suffix))
                .to_vec(),
            run_event_id: fixture_platform_id("evt", "9595"),
            run_outbox_id: fixture_platform_id("obx", "9595"),
            node_event_id: fixture_platform_id("evt", "9596"),
            node_outbox_id: fixture_platform_id("obx", "9596"),
            job_event_id: fixture_platform_id("evt", "9597"),
            job_outbox_id: fixture_platform_id("obx", "9597"),
            activations: Vec::new(),
            pending_nodes: Vec::new(),
            structural_exit: Some(ControllerStructuralExitSlot {
                scope_closing_event_id: fixture_platform_id("evt", "9598"),
                scope_closing_outbox_id: fixture_platform_id("obx", "9598"),
                scope_terminal_event_id: fixture_platform_id("evt", "9599"),
                scope_terminal_outbox_id: fixture_platform_id("obx", "9599"),
                loop_rollover: None,
            }),
            pending_wake: Some(ControllerPendingWakeSlot {
                orchestration_job_id: fixture_platform_id("job", "959a"),
                request_digest: digest('4'),
                node_event_id: fixture_platform_id("evt", "959b"),
                node_outbox_id: fixture_platform_id("obx", "959b"),
                job_event_id: fixture_platform_id("evt", "959c"),
                job_outbox_id: fixture_platform_id("obx", "959c"),
            }),
            remainder_cancellations: vec![remainder_cancellation_slot(
                fixture_platform_id("scp", "9542"),
                ["959d", "959e", "959f", "95a0"],
                ["95a1", "95a2", "95a3", "95a4", "95a5"],
            )],
        },
    };
    let mut scheduler = repository.begin_scheduler_transaction().await.unwrap();
    let failed_leg_result = match scheduler
        .fail_orchestration_job(failed_left.clone())
        .await
        .unwrap()
    {
        CommandOutcome::Applied(record) => record,
        CommandOutcome::Replayed(_) => panic!("first failed leg must apply"),
    };
    scheduler.commit().await.unwrap();
    assert_eq!(failed_leg_result.source_job.state, "failed");
    assert_eq!(failed_leg_result.failure, committed_failure);
    assert_eq!(failed_leg_result.settled_scopes[0].state, "failed");
    assert_eq!(failed_leg_result.cancelled_remainders.len(), 1);
    assert_eq!(
        failed_leg_result.cancelled_remainders[0].reason_code,
        "join_failure_sibling_cancelled"
    );
    assert_eq!(
        failed_leg_result.cancelled_remainders[0].node_id,
        opened_failed_fork.activations[1].node_id
    );
    assert_eq!(
        failed_leg_result.cancelled_remainders[0].job.job_id,
        failed_right_claimed.job.job_id
    );
    assert_eq!(
        failed_leg_result.cancelled_remainders[0].job.state,
        "cancelled"
    );
    assert_eq!(failed_leg_result.woken_nodes.len(), 1);
    assert_eq!(failed_leg_result.woken_nodes[0].plan_node_key.as_str(), "join");
    assert_eq!(failed_leg_result.run.state, "running");
    assert_eq!(failed_leg_result.run.active_work_count, 0);
    assert!(failed_leg_result.run.current.failure.is_none());
    let mut scheduler = repository.begin_scheduler_transaction().await.unwrap();
    assert!(matches!(
        scheduler
            .fail_orchestration_job(failed_left.clone())
            .await
            .unwrap(),
        CommandOutcome::Replayed(record)
            if record.source_job.state == "failed"
                && record.cancelled_remainders.len() == 1
                && record.woken_nodes.len() == 1
    ));
    scheduler.commit().await.unwrap();

    let mut scheduler = repository.begin_scheduler_transaction().await.unwrap();
    let failed_join_claimed = scheduler
        .claim_orchestration_jobs(orchestration_claim_with_ids(
            WORKER_C_ID,
            '5',
            "95b0",
            ["95b1", "95b2", "95b3", "95b4"],
            ["95b5", "95b6", "95b7"],
        ))
        .await
        .unwrap();
    scheduler.commit().await.unwrap();
    assert_eq!(failed_join_claimed.len(), 1);
    let failed_join_start = start_claimed_orchestration(
        &failed_join_claimed[0],
        WORKER_C_ID,
        '5',
        "95b8",
        "95b9",
        "95ba",
        '6',
        '7',
    );
    let mut scheduler = repository.begin_scheduler_transaction().await.unwrap();
    let failed_join_started = match scheduler
        .start_orchestration_job(failed_join_start.clone())
        .await
        .unwrap()
    {
        CommandOutcome::Applied(record) => record,
        CommandOutcome::Replayed(_) => panic!("failed Join start must apply"),
    };
    scheduler.commit().await.unwrap();
    let join_observation = ControllerObservation::Join {
        children: vec![ChildOutcome::Failed, ChildOutcome::Cancelled],
    };
    let fail_join = FailOrchestrationJob {
        fence: JobFence {
            expected_job_version: failed_join_started.version,
            ..failed_join_start.fence.clone()
        },
        plan: runtime_plan(),
        cause: OrchestrationFailureCause::Controller {
            observation: join_observation,
        },
        derived_expression: None,
        idempotency_key_digest: digest('8'),
        request_digest: digest('9'),
        receipt_expires_at: Utc::now() + Duration::hours(1),
        mutations: ControllerStepMutationIds {
            receipt_id: fixture_platform_id("rcp", "95c0"),
            quota_entry_ids: ["95c1", "95c2", "95c3", "95c4"]
                .map(|suffix| fixture_platform_id("qle", suffix))
                .to_vec(),
            run_event_id: fixture_platform_id("evt", "95c5"),
            run_outbox_id: fixture_platform_id("obx", "95c5"),
            node_event_id: fixture_platform_id("evt", "95c6"),
            node_outbox_id: fixture_platform_id("obx", "95c6"),
            job_event_id: fixture_platform_id("evt", "95c7"),
            job_outbox_id: fixture_platform_id("obx", "95c7"),
            activations: Vec::new(),
            pending_nodes: Vec::new(),
            structural_exit: Some(ControllerStructuralExitSlot {
                scope_closing_event_id: fixture_platform_id("evt", "95c8"),
                scope_closing_outbox_id: fixture_platform_id("obx", "95c8"),
                scope_terminal_event_id: fixture_platform_id("evt", "95c9"),
                scope_terminal_outbox_id: fixture_platform_id("obx", "95c9"),
                loop_rollover: None,
            }),
            pending_wake: None,
            remainder_cancellations: Vec::new(),
        },
    };
    let failure_requirements = repository
        .load_controller_failure_mutation_requirements(
            &fail_join.fence,
            &fail_join.plan,
            match &fail_join.cause {
                OrchestrationFailureCause::Controller { observation } => observation,
                _ => unreachable!(),
            },
        )
        .await
        .unwrap();
    assert!(failure_requirements.activation_scopes.is_empty());
    assert_eq!(failure_requirements.pending_node_count, 0);
    assert!(matches!(
        failure_requirements.structural_exit,
        insight_platform_postgres::repository::ControllerStructuralRequirement::Close
    ));
    assert!(!failure_requirements.pending_wake);
    assert!(failure_requirements
        .remainder_cancellation_scope_ids
        .is_empty());
    let mut scheduler = repository.begin_scheduler_transaction().await.unwrap();
    let failed_run = match scheduler
        .fail_orchestration_job(fail_join.clone())
        .await
        .unwrap()
    {
        CommandOutcome::Applied(record) => record,
        CommandOutcome::Replayed(_) => panic!("first failed Join must apply"),
    };
    scheduler.commit().await.unwrap();
    assert_eq!(failed_run.controller_code.as_deref(), Some("child_failed"));
    assert_eq!(failed_run.source_job.state, "failed");
    assert_eq!(failed_run.run.state, "failed");
    assert_eq!(failed_run.run.active_work_count, 0);
    assert_eq!(failed_run.run.current.failure.as_ref(), Some(&failed_run.failure));
    assert!(matches!(
        failed_run.failure.code,
        FailureCode::Platform {
            code: PlatformFailureCode::DependencyUnavailable
        }
    ));
    let mut scheduler = repository.begin_scheduler_transaction().await.unwrap();
    assert!(matches!(
        scheduler
            .fail_orchestration_job(fail_join)
            .await
            .unwrap(),
        CommandOutcome::Replayed(record)
            if record.run.state == "failed"
                && record.controller_code.as_deref() == Some("child_failed")
    ));
    scheduler.commit().await.unwrap();

    let mut boundary_admission = admission(
        AdmissionIds {
            run: "run_0198f1c3-9a00-7c3e-b1f3-773c28369600",
            scope: "scp_0198f1c3-9a00-7c3e-b1f3-773c28369601",
            node: "nod_0198f1c3-9a00-7c3e-b1f3-773c28369602",
            job: "job_0198f1c3-9a00-7c3e-b1f3-773c28369603",
            value: "val_0198f1c3-9a00-7c3e-b1f3-773c28369604",
        },
        audit(TENANT_ID, PRINCIPAL_ID, "9605", 'a', 'b'),
        bindings.clone(),
        Utc::now() + Duration::minutes(5),
    );
    boundary_admission.entry_plan_node_key = PlanNodeKey::new("boundary".to_owned()).unwrap();
    boundary_admission.entry_node_kind = PlanNodeKind::ErrorBoundary;
    applied(
        run_command!(repository, admit_run, boundary_admission.clone()).unwrap(),
    );
    let mut scheduler = repository.begin_scheduler_transaction().await.unwrap();
    let boundary_claimed = scheduler
        .claim_orchestration_jobs(orchestration_claim_with_ids(
            WORKER_D_ID,
            'c',
            "9610",
            ["9611", "9612", "9613", "9614"],
            ["9615", "9616", "9617"],
        ))
        .await
        .unwrap();
    scheduler.commit().await.unwrap();
    assert_eq!(boundary_claimed.len(), 1);
    let boundary_start = start_claimed_orchestration(
        &boundary_claimed[0],
        WORKER_D_ID,
        'c',
        "9618",
        "9619",
        "961a",
        'd',
        'e',
    );
    let mut scheduler = repository.begin_scheduler_transaction().await.unwrap();
    let boundary_started = match scheduler
        .start_orchestration_job(boundary_start.clone())
        .await
        .unwrap()
    {
        CommandOutcome::Applied(record) => record,
        CommandOutcome::Replayed(_) => panic!("ErrorBoundary start must apply"),
    };
    scheduler.commit().await.unwrap();
    let enter_boundary = ApplyOrchestrationControllerStep {
        fence: JobFence {
            expected_job_version: boundary_started.version,
            ..boundary_start.fence.clone()
        },
        plan: runtime_plan(),
        observation: ControllerObservation::ErrorBoundary { failure_code: None },
        idempotency_key_digest: digest('f'),
        request_digest: digest('0'),
        receipt_expires_at: Utc::now() + Duration::hours(1),
        mutations: ControllerStepMutationIds {
            receipt_id: fixture_platform_id("rcp", "9620"),
            quota_entry_ids: ["9621", "9622", "9623", "9624"]
                .map(|suffix| fixture_platform_id("qle", suffix))
                .to_vec(),
            run_event_id: fixture_platform_id("evt", "9625"),
            run_outbox_id: fixture_platform_id("obx", "9625"),
            node_event_id: fixture_platform_id("evt", "9626"),
            node_outbox_id: fixture_platform_id("obx", "9626"),
            job_event_id: fixture_platform_id("evt", "9627"),
            job_outbox_id: fixture_platform_id("obx", "9627"),
            activations: vec![ControllerActivationSlot {
                node_execution_id: fixture_platform_id("nod", "9628"),
                orchestration_job_id: fixture_platform_id("job", "9629"),
                scope: None,
                node_event_id: fixture_platform_id("evt", "962a"),
                node_outbox_id: fixture_platform_id("obx", "962a"),
                job_event_id: fixture_platform_id("evt", "962b"),
                job_outbox_id: fixture_platform_id("obx", "962b"),
            }],
            pending_nodes: Vec::new(),
            structural_exit: None,
            pending_wake: None,
            remainder_cancellations: Vec::new(),
        },
    };
    let mut scheduler = repository.begin_scheduler_transaction().await.unwrap();
    let boundary_entered = match scheduler
        .apply_orchestration_controller_step(enter_boundary)
        .await
        .unwrap()
    {
        CommandOutcome::Applied(record) => record,
        CommandOutcome::Replayed(_) => panic!("ErrorBoundary body activation must apply"),
    };
    scheduler.commit().await.unwrap();
    assert_eq!(
        boundary_entered.activations[0].plan_node_key.as_str(),
        "boundary_body"
    );

    let mut scheduler = repository.begin_scheduler_transaction().await.unwrap();
    let boundary_body_claimed = scheduler
        .claim_orchestration_jobs(orchestration_claim_with_ids(
            WORKER_D_ID,
            '1',
            "9630",
            ["9631", "9632", "9633", "9634"],
            ["9635", "9636", "9637"],
        ))
        .await
        .unwrap();
    scheduler.commit().await.unwrap();
    assert_eq!(boundary_body_claimed.len(), 1);
    let boundary_body_start = start_claimed_orchestration(
        &boundary_body_claimed[0],
        WORKER_D_ID,
        '1',
        "9638",
        "9639",
        "963a",
        '2',
        '3',
    );
    let mut scheduler = repository.begin_scheduler_transaction().await.unwrap();
    let boundary_body_started = match scheduler
        .start_orchestration_job(boundary_body_start.clone())
        .await
        .unwrap()
    {
        CommandOutcome::Applied(record) => record,
        CommandOutcome::Replayed(_) => panic!("ErrorBoundary body start must apply"),
    };
    scheduler.commit().await.unwrap();
    let route_boundary_failure = FailOrchestrationJob {
        fence: JobFence {
            expected_job_version: boundary_body_started.version,
            ..boundary_body_start.fence.clone()
        },
        plan: runtime_plan(),
        cause: OrchestrationFailureCause::Committed {
            failure: committed_failure.clone(),
        },
        derived_expression: None,
        idempotency_key_digest: digest('4'),
        request_digest: digest('5'),
        receipt_expires_at: Utc::now() + Duration::hours(1),
        mutations: ControllerStepMutationIds {
            receipt_id: fixture_platform_id("rcp", "9640"),
            quota_entry_ids: ["9641", "9642", "9643", "9644"]
                .map(|suffix| fixture_platform_id("qle", suffix))
                .to_vec(),
            run_event_id: fixture_platform_id("evt", "9645"),
            run_outbox_id: fixture_platform_id("obx", "9645"),
            node_event_id: fixture_platform_id("evt", "9646"),
            node_outbox_id: fixture_platform_id("obx", "9646"),
            job_event_id: fixture_platform_id("evt", "9647"),
            job_outbox_id: fixture_platform_id("obx", "9647"),
            activations: vec![ControllerActivationSlot {
                node_execution_id: fixture_platform_id("nod", "9648"),
                orchestration_job_id: fixture_platform_id("job", "9649"),
                scope: None,
                node_event_id: fixture_platform_id("evt", "964a"),
                node_outbox_id: fixture_platform_id("obx", "964a"),
                job_event_id: fixture_platform_id("evt", "964b"),
                job_outbox_id: fixture_platform_id("obx", "964b"),
            }],
            pending_nodes: Vec::new(),
            structural_exit: None,
            pending_wake: None,
            remainder_cancellations: Vec::new(),
        },
    };
    let mut scheduler = repository.begin_scheduler_transaction().await.unwrap();
    let routed_failure = match scheduler
        .fail_orchestration_job(route_boundary_failure.clone())
        .await
        .unwrap()
    {
        CommandOutcome::Applied(record) => record,
        CommandOutcome::Replayed(_) => panic!("ErrorBoundary failure routing must apply"),
    };
    scheduler.commit().await.unwrap();
    assert_eq!(routed_failure.source_job.state, "failed");
    assert!(routed_failure.settled_scopes.is_empty());
    assert_eq!(routed_failure.handler_activations.len(), 1);
    assert_eq!(
        routed_failure.handler_activations[0]
            .plan_node_key
            .as_str(),
        "boundary_handler"
    );
    assert_eq!(routed_failure.run.state, "running");
    assert_eq!(routed_failure.run.active_work_count, 0);
    assert!(routed_failure.run.current.failure.is_none());
    assert_eq!(
        sqlx::query_scalar::<_, String>(
            "SELECT state FROM insight_platform.run_nodes WHERE tenant_id = $1 AND node_id = $2",
        )
        .bind(TENANT_ID)
        .bind(boundary_admission.root_scope_id.to_string())
        .fetch_one(&pool)
        .await
        .unwrap(),
        "open"
    );
    assert_eq!(
        sqlx::query_scalar::<_, Option<String>>(
            "SELECT parent_node_id FROM insight_platform.run_nodes WHERE tenant_id = $1 AND node_id = $2",
        )
        .bind(TENANT_ID)
        .bind(routed_failure.handler_activations[0].node_id.clone())
        .fetch_one(&pool)
        .await
        .unwrap(),
        None
    );
    let mut scheduler = repository.begin_scheduler_transaction().await.unwrap();
    assert!(matches!(
        scheduler
            .fail_orchestration_job(route_boundary_failure.clone())
            .await
            .unwrap(),
        CommandOutcome::Replayed(record)
            if record.handler_activations.len() == 1
                && record.settled_scopes.is_empty()
    ));
    scheduler.commit().await.unwrap();

    let mut scheduler = repository.begin_scheduler_transaction().await.unwrap();
    let boundary_handler_claimed = scheduler
        .claim_orchestration_jobs(orchestration_claim_with_ids(
            WORKER_D_ID,
            '6',
            "9650",
            ["9651", "9652", "9653", "9654"],
            ["9655", "9656", "9657"],
        ))
        .await
        .unwrap();
    scheduler.commit().await.unwrap();
    assert_eq!(boundary_handler_claimed.len(), 1);
    let boundary_handler_start = start_claimed_orchestration(
        &boundary_handler_claimed[0],
        WORKER_D_ID,
        '6',
        "9658",
        "9659",
        "965a",
        '7',
        '8',
    );
    let mut scheduler = repository.begin_scheduler_transaction().await.unwrap();
    let boundary_handler_started = match scheduler
        .start_orchestration_job(boundary_handler_start.clone())
        .await
        .unwrap()
    {
        CommandOutcome::Applied(record) => record,
        CommandOutcome::Replayed(_) => panic!("ErrorBoundary handler start must apply"),
    };
    scheduler.commit().await.unwrap();
    let handler_failure = Failure {
        code: FailureCode::Platform {
            code: PlatformFailureCode::BudgetExhausted,
        },
        class: FailureClass::Quota,
        retryability: Retryability::Never,
        safe_message: Some("The error handler exhausted its budget.".to_owned()),
        details_ref: None,
        source: FailureSource::Platform,
    };
    let fail_boundary_handler = FailOrchestrationJob {
        fence: JobFence {
            expected_job_version: boundary_handler_started.version,
            ..boundary_handler_start.fence.clone()
        },
        plan: runtime_plan(),
        cause: OrchestrationFailureCause::Committed {
            failure: handler_failure.clone(),
        },
        derived_expression: None,
        idempotency_key_digest: digest('9'),
        request_digest: digest('a'),
        receipt_expires_at: Utc::now() + Duration::hours(1),
        mutations: ControllerStepMutationIds {
            receipt_id: fixture_platform_id("rcp", "9660"),
            quota_entry_ids: ["9661", "9662", "9663", "9664"]
                .map(|suffix| fixture_platform_id("qle", suffix))
                .to_vec(),
            run_event_id: fixture_platform_id("evt", "9665"),
            run_outbox_id: fixture_platform_id("obx", "9665"),
            node_event_id: fixture_platform_id("evt", "9666"),
            node_outbox_id: fixture_platform_id("obx", "9666"),
            job_event_id: fixture_platform_id("evt", "9667"),
            job_outbox_id: fixture_platform_id("obx", "9667"),
            activations: Vec::new(),
            pending_nodes: Vec::new(),
            structural_exit: Some(ControllerStructuralExitSlot {
                scope_closing_event_id: fixture_platform_id("evt", "9668"),
                scope_closing_outbox_id: fixture_platform_id("obx", "9668"),
                scope_terminal_event_id: fixture_platform_id("evt", "9669"),
                scope_terminal_outbox_id: fixture_platform_id("obx", "9669"),
                loop_rollover: None,
            }),
            pending_wake: None,
            remainder_cancellations: Vec::new(),
        },
    };
    let mut scheduler = repository.begin_scheduler_transaction().await.unwrap();
    let propagated_handler_failure = match scheduler
        .fail_orchestration_job(fail_boundary_handler)
        .await
        .unwrap()
    {
        CommandOutcome::Applied(record) => record,
        CommandOutcome::Replayed(_) => panic!("handler failure propagation must apply"),
    };
    scheduler.commit().await.unwrap();
    assert_eq!(propagated_handler_failure.run.state, "failed");
    assert_eq!(
        propagated_handler_failure.run.current.failure.as_ref(),
        Some(&handler_failure)
    );
    assert!(propagated_handler_failure.handler_activations.is_empty());
    let mut scheduler = repository.begin_scheduler_transaction().await.unwrap();
    assert!(matches!(
        scheduler
            .fail_orchestration_job(route_boundary_failure)
            .await
            .unwrap(),
        CommandOutcome::Replayed(record)
            if record.handler_activations.len() == 1
                && record.run.state == "failed"
    ));
    scheduler.commit().await.unwrap();

    let mut expression_admission = admission(
        AdmissionIds {
            run: "run_0198f1c3-9a00-7c3e-b1f3-773c2836aa00",
            scope: "scp_0198f1c3-9a00-7c3e-b1f3-773c2836aa01",
            node: "nod_0198f1c3-9a00-7c3e-b1f3-773c2836aa02",
            job: "job_0198f1c3-9a00-7c3e-b1f3-773c2836aa03",
            value: "val_0198f1c3-9a00-7c3e-b1f3-773c2836aa04",
        },
        audit(TENANT_ID, PRINCIPAL_ID, "aa05", '1', '2'),
        bindings.clone(),
        Utc::now() + Duration::minutes(5),
    );
    expression_admission.entry_plan_node_key =
        PlanNodeKey::new("input_branch".to_owned()).unwrap();
    expression_admission.entry_node_kind = PlanNodeKind::Branch;
    applied(
        run_command!(repository, admit_run, expression_admission.clone()).unwrap(),
    );
    let mut scheduler = repository.begin_scheduler_transaction().await.unwrap();
    let expression_claimed = scheduler
        .claim_orchestration_jobs(orchestration_claim_with_ids(
            WORKER_D_ID,
            '3',
            "aa06",
            ["aa07", "aa08", "aa09", "aa0a"],
            ["aa0b", "aa0c", "aa0d"],
        ))
        .await
        .unwrap();
    scheduler.commit().await.unwrap();
    assert_eq!(expression_claimed.len(), 1);
    let expression_start = start_claimed_orchestration(
        &expression_claimed[0],
        WORKER_D_ID,
        '3',
        "aa0e",
        "aa0f",
        "aa10",
        '4',
        '5',
    );
    let mut scheduler = repository.begin_scheduler_transaction().await.unwrap();
    let expression_started = match scheduler
        .start_orchestration_job(expression_start.clone())
        .await
        .unwrap()
    {
        CommandOutcome::Applied(record) => record,
        CommandOutcome::Replayed(_) => panic!("expression controller start must apply"),
    };
    scheduler.commit().await.unwrap();
    let expression_fence = JobFence {
        expected_job_version: expression_started.version,
        ..expression_start.fence.clone()
    };
    let expression_facts = repository
        .load_expression_controller_facts(&expression_fence, &runtime_plan())
        .await
        .unwrap();
    assert_eq!(expression_facts.plan_node_key.as_str(), "input_branch");
    assert!(expression_facts.node_execution_version > 0);
    assert_eq!(expression_facts.loop_iteration, 0);
    assert_eq!(expression_facts.inputs.len(), 1);
    assert_eq!(
        expression_facts.inputs[0].run_value_id,
        expression_admission.input.value_id
    );
    assert_eq!(
        expression_facts.inputs[0].value,
        expression_admission.input.value
    );
    let mut stale_expression_fence = expression_fence.clone();
    stale_expression_fence.expected_job_version -= 1;
    assert!(matches!(
        repository
            .load_expression_controller_facts(&stale_expression_fence, &runtime_plan())
            .await,
        Err(RepositoryError::Conflict("running Job fence"))
    ));
    let materialized_inputs = expression_facts
        .inputs
        .iter()
        .map(|input| {
            let ValueRef::Inline { value } = &input.value else {
                panic!("expression fixture input must be Inline")
            };
            CommittedExpressionInput {
                run_value_id: input.run_value_id.clone(),
                port: input.port.clone(),
                classification: input.classification,
                value: ClosedJsonValue::build(input.schema_digest.clone(), value.clone()).unwrap(),
            }
        })
        .collect::<Vec<_>>();
    let plan = runtime_plan();
    let evaluation = derive_expression_controller(
        plan.node(&expression_facts.plan_node_key).unwrap(),
        materialized_inputs.clone(),
        expression_facts.node_execution_id.clone(),
        expression_facts.node_execution_version,
        expression_facts.loop_iteration,
        ExpressionLimits::ABSOLUTE,
    )
    .unwrap();
    let branch_requirements = repository
        .load_controller_mutation_requirements(
            &expression_fence,
            &plan,
            &evaluation.observation,
        )
        .await
        .unwrap();
    assert_eq!(branch_requirements.activation_scopes, vec![false]);
    assert_eq!(branch_requirements.pending_node_count, 0);
    let expression_step = ApplyOrchestrationControllerStep {
        fence: expression_fence.clone(),
        plan: plan.clone(),
        observation: evaluation.observation.clone(),
        idempotency_key_digest: digest('6'),
        request_digest: digest('7'),
        receipt_expires_at: Utc::now() + Duration::hours(1),
        mutations: ControllerStepMutationIds {
            receipt_id: fixture_platform_id("rcp", "aa20"),
            quota_entry_ids: ["aa21", "aa22", "aa23", "aa24"]
                .map(|suffix| fixture_platform_id("qle", suffix))
                .to_vec(),
            run_event_id: fixture_platform_id("evt", "aa25"),
            run_outbox_id: fixture_platform_id("obx", "aa25"),
            node_event_id: fixture_platform_id("evt", "aa26"),
            node_outbox_id: fixture_platform_id("obx", "aa26"),
            job_event_id: fixture_platform_id("evt", "aa27"),
            job_outbox_id: fixture_platform_id("obx", "aa27"),
            activations: vec![ControllerActivationSlot {
                node_execution_id: fixture_platform_id("nod", "aa28"),
                orchestration_job_id: fixture_platform_id("job", "aa29"),
                scope: None,
                node_event_id: fixture_platform_id("evt", "aa2a"),
                node_outbox_id: fixture_platform_id("obx", "aa2a"),
                job_event_id: fixture_platform_id("evt", "aa2b"),
                job_outbox_id: fixture_platform_id("obx", "aa2b"),
            }],
            pending_nodes: vec![],
            structural_exit: None,
            pending_wake: None,
            remainder_cancellations: vec![],
        },
    };
    let mut downgraded_inputs = materialized_inputs.clone();
    downgraded_inputs[0].classification = DataClassification::Public;
    let downgraded_evaluation = derive_expression_controller(
        plan.node(&expression_facts.plan_node_key).unwrap(),
        downgraded_inputs.clone(),
        expression_facts.node_execution_id.clone(),
        expression_facts.node_execution_version,
        0,
        ExpressionLimits::ABSOLUTE,
    )
    .unwrap();
    let mut scheduler = repository.begin_scheduler_transaction().await.unwrap();
    assert!(matches!(
        scheduler
            .apply_derived_expression_controller_step(ApplyDerivedExpressionControllerStep {
                step: expression_step.clone(),
                materialized_inputs: downgraded_inputs,
                evaluation: downgraded_evaluation,
                output_value_ids: vec![],
            })
            .await,
        Err(RepositoryError::Conflict("derived expression input evidence"))
    ));
    scheduler.rollback().await.unwrap();
    let mut scheduler = repository.begin_scheduler_transaction().await.unwrap();
    let derived_branch = scheduler
        .apply_derived_expression_controller_step(ApplyDerivedExpressionControllerStep {
            step: expression_step,
            materialized_inputs,
            evaluation,
            output_value_ids: vec![],
        })
        .await
        .unwrap();
    scheduler.commit().await.unwrap();
    assert!(matches!(derived_branch, CommandOutcome::Applied(_)));
    let expression_run_version: i64 = sqlx::query_scalar(
        "SELECT version FROM insight_platform.runs WHERE tenant_id = $1 AND run_id = $2",
    )
    .bind(TENANT_ID)
    .bind(expression_admission.run_id.to_string())
    .fetch_one(&pool)
    .await
    .unwrap();
    applied(
        run_command!(
            repository,
            request_run_cancel,
            RequestRunCancel {
                audit: audit(TENANT_ID, PRINCIPAL_ID, "aa11", '6', '7'),
                run_id: expression_admission.run_id.clone(),
                expected_run_version: expression_run_version,
                expected_cancel_generation: 0,
                reason_code: "fixture_cleanup".to_owned(),
            }
        )
        .unwrap(),
    );
    let mut scheduler = repository.begin_scheduler_transaction().await.unwrap();
    let expression_cleanup = scheduler
        .drive_orchestration_convergence(orchestration_convergence(
            ["aa12", "aa13", "aa14", "aa15"],
            ["aa16", "aa17", "aa18", "aa19", "aa1a", "aa1b"],
        ))
        .await
        .unwrap();
    scheduler.commit().await.unwrap();
    assert_eq!(expression_cleanup.len(), 1);
    assert_eq!(expression_cleanup[0].completed.run.state, "cancelled");

    let mut compute_admission = admission(
        AdmissionIds {
            run: "run_0198f1c3-9a00-7c3e-b1f3-773c2836ab00",
            scope: "scp_0198f1c3-9a00-7c3e-b1f3-773c2836ab01",
            node: "nod_0198f1c3-9a00-7c3e-b1f3-773c2836ab02",
            job: "job_0198f1c3-9a00-7c3e-b1f3-773c2836ab03",
            value: "val_0198f1c3-9a00-7c3e-b1f3-773c2836ab04",
        },
        audit(TENANT_ID, PRINCIPAL_ID, "ab05", '8', '9'),
        bindings.clone(),
        Utc::now() + Duration::minutes(5),
    );
    compute_admission.entry_plan_node_key = PlanNodeKey::new("input_compute".to_owned()).unwrap();
    compute_admission.entry_node_kind = PlanNodeKind::Compute;
    applied(run_command!(repository, admit_run, compute_admission.clone()).unwrap());
    let mut scheduler = repository.begin_scheduler_transaction().await.unwrap();
    let compute_claimed = scheduler
        .claim_orchestration_jobs(orchestration_claim_with_ids(
            WORKER_D_ID,
            'a',
            "ab06",
            ["ab07", "ab08", "ab09", "ab0a"],
            ["ab0b", "ab0c", "ab0d"],
        ))
        .await
        .unwrap();
    scheduler.commit().await.unwrap();
    assert_eq!(compute_claimed.len(), 1);
    let compute_start = start_claimed_orchestration(
        &compute_claimed[0],
        WORKER_D_ID,
        'a',
        "ab0e",
        "ab0f",
        "ab10",
        'b',
        'c',
    );
    let mut scheduler = repository.begin_scheduler_transaction().await.unwrap();
    let compute_started = match scheduler
        .start_orchestration_job(compute_start.clone())
        .await
        .unwrap()
    {
        CommandOutcome::Applied(record) => record,
        CommandOutcome::Replayed(_) => panic!("Compute start must apply"),
    };
    scheduler.commit().await.unwrap();
    let compute_fence = JobFence {
        expected_job_version: compute_started.version,
        ..compute_start.fence.clone()
    };
    let compute_facts = repository
        .load_expression_controller_facts(&compute_fence, &runtime_plan())
        .await
        .unwrap();
    let compute_inputs = compute_facts
        .inputs
        .iter()
        .map(|input| {
            let ValueRef::Inline { value } = &input.value else {
                panic!("Compute fixture input must be Inline")
            };
            CommittedExpressionInput {
                run_value_id: input.run_value_id.clone(),
                port: input.port.clone(),
                classification: input.classification,
                value: ClosedJsonValue::build(input.schema_digest.clone(), value.clone()).unwrap(),
            }
        })
        .collect::<Vec<_>>();
    let compute_plan = runtime_plan();
    let compute_evaluation = derive_expression_controller(
        compute_plan.node(&compute_facts.plan_node_key).unwrap(),
        compute_inputs.clone(),
        compute_facts.node_execution_id.clone(),
        compute_facts.node_execution_version,
        0,
        ExpressionLimits::ABSOLUTE,
    )
    .unwrap();
    let compute_output_id = fixture_platform_id("val", "ab20");
    let compute_step = ApplyOrchestrationControllerStep {
        fence: compute_fence,
        plan: compute_plan,
        observation: compute_evaluation.observation.clone(),
        idempotency_key_digest: digest('d'),
        request_digest: digest('e'),
        receipt_expires_at: Utc::now() + Duration::hours(1),
        mutations: ControllerStepMutationIds {
            receipt_id: fixture_platform_id("rcp", "ab21"),
            quota_entry_ids: ["ab22", "ab23", "ab24", "ab25"]
                .map(|suffix| fixture_platform_id("qle", suffix))
                .to_vec(),
            run_event_id: fixture_platform_id("evt", "ab26"),
            run_outbox_id: fixture_platform_id("obx", "ab26"),
            node_event_id: fixture_platform_id("evt", "ab27"),
            node_outbox_id: fixture_platform_id("obx", "ab27"),
            job_event_id: fixture_platform_id("evt", "ab28"),
            job_outbox_id: fixture_platform_id("obx", "ab28"),
            activations: vec![ControllerActivationSlot {
                node_execution_id: fixture_platform_id("nod", "ab29"),
                orchestration_job_id: fixture_platform_id("job", "ab2a"),
                scope: None,
                node_event_id: fixture_platform_id("evt", "ab2b"),
                node_outbox_id: fixture_platform_id("obx", "ab2b"),
                job_event_id: fixture_platform_id("evt", "ab2c"),
                job_outbox_id: fixture_platform_id("obx", "ab2c"),
            }],
            pending_nodes: vec![],
            structural_exit: None,
            pending_wake: None,
            remainder_cancellations: vec![],
        },
    };
    let mut scheduler = repository.begin_scheduler_transaction().await.unwrap();
    assert!(matches!(
        scheduler
            .apply_derived_expression_controller_step(ApplyDerivedExpressionControllerStep {
                step: compute_step.clone(),
                materialized_inputs: compute_inputs.clone(),
                evaluation: compute_evaluation.clone(),
                output_value_ids: vec![compute_admission.input.value_id.clone()],
            })
            .await,
        Err(RepositoryError::Database(_))
    ));
    scheduler.rollback().await.unwrap();
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT count(*) FROM insight_platform.run_values WHERE tenant_id = $1 AND run_id = $2",
        )
        .bind(TENANT_ID)
        .bind(compute_admission.run_id.to_string())
        .fetch_one(&pool)
        .await
        .unwrap(),
        1
    );
    let mut scheduler = repository.begin_scheduler_transaction().await.unwrap();
    let computed = scheduler
        .apply_derived_expression_controller_step(ApplyDerivedExpressionControllerStep {
            step: compute_step,
            materialized_inputs: compute_inputs,
            evaluation: compute_evaluation,
            output_value_ids: vec![compute_output_id.clone()],
        })
        .await
        .unwrap();
    scheduler.commit().await.unwrap();
    assert!(matches!(computed, CommandOutcome::Applied(_)));
    let stored_compute: (String, String, Value) = sqlx::query_as(
        "SELECT classification, schema_digest, inline_value FROM insight_platform.run_values WHERE tenant_id = $1 AND value_id = $2",
    )
    .bind(TENANT_ID)
    .bind(compute_output_id.to_string())
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(stored_compute.0, DataClassification::Internal.as_str());
    assert_eq!(
        stored_compute.1,
        agent_schema().canonical_digest.to_string()
    );
    assert_eq!(stored_compute.2, json!({"question": "select one"}));
    let stored_scope_payload: Value = sqlx::query_scalar(
        "SELECT payload FROM insight_platform.run_nodes WHERE tenant_id = $1 AND node_id = $2",
    )
    .bind(TENANT_ID)
    .bind(compute_admission.root_scope_id.to_string())
    .fetch_one(&pool)
    .await
    .unwrap();
    assert!(stored_scope_payload.to_string().contains(&compute_output_id.to_string()));
    let mut scheduler = repository.begin_scheduler_transaction().await.unwrap();
    let terminal_claimed = scheduler
        .claim_orchestration_jobs(orchestration_claim_with_ids(
            WORKER_D_ID,
            'f',
            "ab30",
            ["ab31", "ab32", "ab33", "ab34"],
            ["ab35", "ab36", "ab37"],
        ))
        .await
        .unwrap();
    scheduler.commit().await.unwrap();
    assert_eq!(terminal_claimed.len(), 1);
    let terminal_start = start_claimed_orchestration(
        &terminal_claimed[0],
        WORKER_D_ID,
        'f',
        "ab38",
        "ab39",
        "ab3a",
        '0',
        '1',
    );
    let mut scheduler = repository.begin_scheduler_transaction().await.unwrap();
    let terminal_started = match scheduler
        .start_orchestration_job(terminal_start.clone())
        .await
        .unwrap()
    {
        CommandOutcome::Applied(record) => record,
        CommandOutcome::Replayed(_) => panic!("Return start must apply"),
    };
    scheduler.commit().await.unwrap();
    let terminal_body = json!({"question": "select one"});
    let terminal_content_digest = canonical_digest(&terminal_body).unwrap().parse().unwrap();
    let terminal = CommitPlanTerminal {
        fence: JobFence {
            expected_job_version: terminal_started.version,
            ..terminal_start.fence
        },
        plan: runtime_plan(),
        value: MaterializedTerminalValue {
            value_id: compute_output_id.clone(),
            classification: DataClassification::Internal,
            schema_digest: agent_schema().canonical_digest,
            content_digest: terminal_content_digest,
            body: terminal_body,
        },
        idempotency_key_digest: digest('2'),
        request_digest: digest('3'),
        receipt_expires_at: Utc::now() + Duration::hours(1),
        mutations: OrchestrationTerminalMutationIds {
            receipt_id: fixture_platform_id("rcp", "ab3b"),
            quota_entry_ids: ["ab3c", "ab3d", "ab3e", "ab3f"]
                .map(|suffix| fixture_platform_id("qle", suffix))
                .to_vec(),
            run_event_id: fixture_platform_id("evt", "ab40"),
            run_outbox_id: fixture_platform_id("obx", "ab40"),
            node_event_id: fixture_platform_id("evt", "ab41"),
            node_outbox_id: fixture_platform_id("obx", "ab41"),
            scope_closing_event_id: fixture_platform_id("evt", "ab42"),
            scope_closing_outbox_id: fixture_platform_id("obx", "ab42"),
            scope_terminal_event_id: fixture_platform_id("evt", "ab43"),
            scope_terminal_outbox_id: fixture_platform_id("obx", "ab43"),
            job_event_id: fixture_platform_id("evt", "ab44"),
            job_outbox_id: fixture_platform_id("obx", "ab44"),
        },
    };
    let mut wrong_value = terminal.clone();
    wrong_value.value.value_id = fixture_platform_id("val", "ab45");
    let mut scheduler = repository.begin_scheduler_transaction().await.unwrap();
    let wrong_value_error = scheduler.commit_plan_terminal(wrong_value).await.unwrap_err();
    assert!(matches!(
        &wrong_value_error,
        RepositoryError::Conflict("terminal RunValue evidence")
    ), "unexpected terminal rejection: {wrong_value_error:?}");
    scheduler.rollback().await.unwrap();
    let mut scheduler = repository.begin_scheduler_transaction().await.unwrap();
    let completed = scheduler.commit_plan_terminal(terminal.clone()).await.unwrap();
    scheduler.commit().await.unwrap();
    assert!(matches!(
        completed,
        CommandOutcome::Applied(ref record)
            if record.run.state == "succeeded"
                && record.run.output_value_id.as_deref() == Some(compute_output_id.to_string().as_str())
    ));
    let mut scheduler = repository.begin_scheduler_transaction().await.unwrap();
    assert!(matches!(
        scheduler.commit_plan_terminal(terminal).await.unwrap(),
        CommandOutcome::Replayed(record) if record.run.state == "succeeded"
    ));
    scheduler.commit().await.unwrap();

    let raised_failure = Failure {
        code: FailureCode::Platform {
            code: PlatformFailureCode::AgentOutputInvalid,
        },
        class: FailureClass::Validation,
        retryability: Retryability::Never,
        safe_message: Some("declared terminal failure".to_owned()),
        details_ref: None,
        source: FailureSource::Agent,
    };
    let raised_body = serde_json::to_value(&raised_failure).unwrap();
    let mut raise_admission = admission(
        AdmissionIds {
            run: "run_0198f1c3-9a00-7c3e-b1f3-773c2836ab50",
            scope: "scp_0198f1c3-9a00-7c3e-b1f3-773c2836ab51",
            node: "nod_0198f1c3-9a00-7c3e-b1f3-773c2836ab52",
            job: "job_0198f1c3-9a00-7c3e-b1f3-773c2836ab53",
            value: "val_0198f1c3-9a00-7c3e-b1f3-773c2836ab54",
        },
        audit(TENANT_ID, PRINCIPAL_ID, "ab55", '4', '5'),
        bindings.clone(),
        Utc::now() + Duration::minutes(5),
    );
    raise_admission.entry_plan_node_key = PlanNodeKey::new("raise".to_owned()).unwrap();
    raise_admission.entry_node_kind = PlanNodeKind::Raise;
    raise_admission.input.schema_digest = agent_error_schema().canonical_digest;
    raise_admission.input.content_digest = canonical_digest(&raised_body).unwrap().parse().unwrap();
    raise_admission.input.value = ValueRef::Inline {
        value: raised_body.clone(),
    };
    applied(run_command!(repository, admit_run, raise_admission.clone()).unwrap());
    let mut scheduler = repository.begin_scheduler_transaction().await.unwrap();
    let raise_claimed = scheduler
        .claim_orchestration_jobs(orchestration_claim_with_ids(
            WORKER_D_ID,
            '6',
            "ab56",
            ["ab57", "ab58", "ab59", "ab5a"],
            ["ab5b", "ab5c", "ab5d"],
        ))
        .await
        .unwrap();
    scheduler.commit().await.unwrap();
    assert_eq!(raise_claimed.len(), 1);
    let raise_start = start_claimed_orchestration(
        &raise_claimed[0],
        WORKER_D_ID,
        '6',
        "ab5e",
        "ab5f",
        "ab60",
        '7',
        '8',
    );
    let mut scheduler = repository.begin_scheduler_transaction().await.unwrap();
    let raise_started = match scheduler
        .start_orchestration_job(raise_start.clone())
        .await
        .unwrap()
    {
        CommandOutcome::Applied(record) => record,
        CommandOutcome::Replayed(_) => panic!("Raise start must apply"),
    };
    scheduler.commit().await.unwrap();
    let raise_terminal = CommitPlanTerminal {
        fence: JobFence {
            expected_job_version: raise_started.version,
            ..raise_start.fence
        },
        plan: runtime_plan(),
        value: MaterializedTerminalValue {
            value_id: raise_admission.input.value_id,
            classification: raise_admission.input.classification,
            schema_digest: raise_admission.input.schema_digest,
            content_digest: raise_admission.input.content_digest,
            body: raised_body,
        },
        idempotency_key_digest: digest('9'),
        request_digest: digest('a'),
        receipt_expires_at: Utc::now() + Duration::hours(1),
        mutations: OrchestrationTerminalMutationIds {
            receipt_id: fixture_platform_id("rcp", "ab61"),
            quota_entry_ids: ["ab62", "ab63", "ab64", "ab65"]
                .map(|suffix| fixture_platform_id("qle", suffix))
                .to_vec(),
            run_event_id: fixture_platform_id("evt", "ab66"),
            run_outbox_id: fixture_platform_id("obx", "ab66"),
            node_event_id: fixture_platform_id("evt", "ab67"),
            node_outbox_id: fixture_platform_id("obx", "ab67"),
            scope_closing_event_id: fixture_platform_id("evt", "ab68"),
            scope_closing_outbox_id: fixture_platform_id("obx", "ab68"),
            scope_terminal_event_id: fixture_platform_id("evt", "ab69"),
            scope_terminal_outbox_id: fixture_platform_id("obx", "ab69"),
            job_event_id: fixture_platform_id("evt", "ab6a"),
            job_outbox_id: fixture_platform_id("obx", "ab6a"),
        },
    };
    let mut scheduler = repository.begin_scheduler_transaction().await.unwrap();
    let raised = scheduler
        .commit_plan_terminal(raise_terminal)
        .await
        .unwrap();
    scheduler.commit().await.unwrap();
    assert!(matches!(
        raised,
        CommandOutcome::Applied(record)
            if record.run.state == "failed"
                && record.run.current.failure.as_ref() == Some(&raised_failure)
                && record.run.output_value_id.is_none()
    ));

    let unsafe_failure = Failure {
        safe_message: Some("unsafe\u{0007}message".to_owned()),
        ..raised_failure
    };
    let unsafe_body = serde_json::to_value(&unsafe_failure).unwrap();
    let mut unsafe_raise_admission = admission(
        AdmissionIds {
            run: "run_0198f1c3-9a00-7c3e-b1f3-773c2836af00",
            scope: "scp_0198f1c3-9a00-7c3e-b1f3-773c2836af01",
            node: "nod_0198f1c3-9a00-7c3e-b1f3-773c2836af02",
            job: "job_0198f1c3-9a00-7c3e-b1f3-773c2836af03",
            value: "val_0198f1c3-9a00-7c3e-b1f3-773c2836af04",
        },
        audit(TENANT_ID, PRINCIPAL_ID, "af05", 'b', 'c'),
        bindings.clone(),
        Utc::now() + Duration::minutes(5),
    );
    unsafe_raise_admission.entry_plan_node_key = PlanNodeKey::new("raise".to_owned()).unwrap();
    unsafe_raise_admission.entry_node_kind = PlanNodeKind::Raise;
    unsafe_raise_admission.input.schema_digest = agent_error_schema().canonical_digest;
    unsafe_raise_admission.input.content_digest =
        canonical_digest(&unsafe_body).unwrap().parse().unwrap();
    unsafe_raise_admission.input.value = ValueRef::Inline {
        value: unsafe_body.clone(),
    };
    applied(run_command!(
        repository,
        admit_run,
        unsafe_raise_admission.clone()
    )
    .unwrap());
    let mut scheduler = repository.begin_scheduler_transaction().await.unwrap();
    let unsafe_raise_claimed = scheduler
        .claim_orchestration_jobs(orchestration_claim_with_ids(
            WORKER_D_ID,
            'd',
            "af06",
            ["af07", "af08", "af09", "af0a"],
            ["af0b", "af0c", "af0d"],
        ))
        .await
        .unwrap();
    scheduler.commit().await.unwrap();
    assert_eq!(unsafe_raise_claimed.len(), 1);
    let unsafe_raise_start = start_claimed_orchestration(
        &unsafe_raise_claimed[0],
        WORKER_D_ID,
        'd',
        "af0e",
        "af0f",
        "af10",
        'e',
        'f',
    );
    let mut scheduler = repository.begin_scheduler_transaction().await.unwrap();
    let unsafe_raise_started = match scheduler
        .start_orchestration_job(unsafe_raise_start.clone())
        .await
        .unwrap()
    {
        CommandOutcome::Applied(record) => record,
        CommandOutcome::Replayed(_) => panic!("unsafe Raise start must apply"),
    };
    scheduler.commit().await.unwrap();
    let unsafe_raise_terminal = CommitPlanTerminal {
        fence: JobFence {
            expected_job_version: unsafe_raise_started.version,
            ..unsafe_raise_start.fence
        },
        plan: runtime_plan(),
        value: MaterializedTerminalValue {
            value_id: unsafe_raise_admission.input.value_id,
            classification: unsafe_raise_admission.input.classification,
            schema_digest: unsafe_raise_admission.input.schema_digest,
            content_digest: unsafe_raise_admission.input.content_digest,
            body: unsafe_body,
        },
        idempotency_key_digest: digest('0'),
        request_digest: digest('1'),
        receipt_expires_at: Utc::now() + Duration::hours(1),
        mutations: OrchestrationTerminalMutationIds {
            receipt_id: fixture_platform_id("rcp", "af11"),
            quota_entry_ids: ["af12", "af13", "af14", "af15"]
                .map(|suffix| fixture_platform_id("qle", suffix))
                .to_vec(),
            run_event_id: fixture_platform_id("evt", "af16"),
            run_outbox_id: fixture_platform_id("obx", "af16"),
            node_event_id: fixture_platform_id("evt", "af17"),
            node_outbox_id: fixture_platform_id("obx", "af17"),
            scope_closing_event_id: fixture_platform_id("evt", "af18"),
            scope_closing_outbox_id: fixture_platform_id("obx", "af18"),
            scope_terminal_event_id: fixture_platform_id("evt", "af19"),
            scope_terminal_outbox_id: fixture_platform_id("obx", "af19"),
            job_event_id: fixture_platform_id("evt", "af1a"),
            job_outbox_id: fixture_platform_id("obx", "af1a"),
        },
    };
    let mut scheduler = repository.begin_scheduler_transaction().await.unwrap();
    let unsafe_raise_error = scheduler
        .commit_plan_terminal(unsafe_raise_terminal)
        .await
        .unwrap_err();
    assert!(
        matches!(&unsafe_raise_error, RepositoryError::InvalidInput(_)),
        "unexpected unsafe Failure rejection: {unsafe_raise_error:?}"
    );
    scheduler.rollback().await.unwrap();
    let unsafe_run_state: (String, Option<String>) = sqlx::query_as(
        "SELECT state, current_payload ->> 'failure' FROM insight_platform.runs WHERE tenant_id = $1 AND run_id = $2",
    )
    .bind(TENANT_ID)
    .bind(unsafe_raise_admission.run_id.to_string())
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(unsafe_run_state, ("running".to_owned(), None));
    let unsafe_run_version: i64 = sqlx::query_scalar(
        "SELECT version FROM insight_platform.runs WHERE tenant_id = $1 AND run_id = $2",
    )
    .bind(TENANT_ID)
    .bind(unsafe_raise_admission.run_id.to_string())
    .fetch_one(&pool)
    .await
    .unwrap();
    let unsafe_cancelled = applied(
        run_command!(
            repository,
            request_run_cancel,
            RequestRunCancel {
                audit: audit(TENANT_ID, PRINCIPAL_ID, "af1b", '2', '3'),
                run_id: unsafe_raise_admission.run_id,
                expected_run_version: unsafe_run_version,
                expected_cancel_generation: 0,
                reason_code: "invalid_terminal_fixture_cleanup".to_owned(),
            }
        )
        .unwrap(),
    );
    assert_eq!(unsafe_cancelled.state, "cancelling");
    let mut scheduler = repository.begin_scheduler_transaction().await.unwrap();
    let unsafe_converged = scheduler
        .drive_orchestration_convergence(orchestration_convergence(
            ["af1c", "af1d", "af1e", "af1f"],
            ["af20", "af21", "af22", "af23", "af24", "af25"],
        ))
        .await
        .unwrap();
    scheduler.commit().await.unwrap();
    assert_eq!(unsafe_converged.len(), 1);
    assert_eq!(unsafe_converged[0].completed.run.state, "cancelled");
    assert_eq!(unsafe_converged[0].completed.job.state, "cancelled");

    let mut loop_admission = admission(
        AdmissionIds {
            run: "run_0198f1c3-9a00-7c3e-b1f3-773c28369700",
            scope: "scp_0198f1c3-9a00-7c3e-b1f3-773c28369701",
            node: "nod_0198f1c3-9a00-7c3e-b1f3-773c28369702",
            job: "job_0198f1c3-9a00-7c3e-b1f3-773c28369703",
            value: "val_0198f1c3-9a00-7c3e-b1f3-773c28369704",
        },
        audit(TENANT_ID, PRINCIPAL_ID, "9705", 'b', 'c'),
        bindings.clone(),
        Utc::now() + Duration::minutes(5),
    );
    loop_admission.entry_plan_node_key = PlanNodeKey::new("loop".to_owned()).unwrap();
    loop_admission.entry_node_kind = PlanNodeKind::Loop;
    applied(run_command!(repository, admit_run, loop_admission.clone()).unwrap());
    let mut scheduler = repository.begin_scheduler_transaction().await.unwrap();
    let loop_claimed = scheduler
        .claim_orchestration_jobs(orchestration_claim_with_ids(
            WORKER_D_ID,
            'd',
            "9710",
            ["9711", "9712", "9713", "9714"],
            ["9715", "9716", "9717"],
        ))
        .await
        .unwrap();
    scheduler.commit().await.unwrap();
    assert_eq!(loop_claimed.len(), 1);
    let loop_start = start_claimed_orchestration(
        &loop_claimed[0],
        WORKER_D_ID,
        'd',
        "9718",
        "9719",
        "971a",
        'e',
        'f',
    );
    let mut scheduler = repository.begin_scheduler_transaction().await.unwrap();
    let loop_started = match scheduler
        .start_orchestration_job(loop_start.clone())
        .await
        .unwrap()
    {
        CommandOutcome::Applied(record) => record,
        CommandOutcome::Replayed(_) => panic!("Loop start must apply"),
    };
    scheduler.commit().await.unwrap();
    let open_loop_iteration = ApplyOrchestrationControllerStep {
        fence: JobFence {
            expected_job_version: loop_started.version,
            ..loop_start.fence.clone()
        },
        plan: runtime_plan(),
        observation: ControllerObservation::Loop {
            iteration: 0,
            condition: true,
        },
        idempotency_key_digest: digest('0'),
        request_digest: digest('1'),
        receipt_expires_at: Utc::now() + Duration::hours(1),
        mutations: ControllerStepMutationIds {
            receipt_id: fixture_platform_id("rcp", "9720"),
            quota_entry_ids: ["9721", "9722", "9723", "9724"]
                .map(|suffix| fixture_platform_id("qle", suffix))
                .to_vec(),
            run_event_id: fixture_platform_id("evt", "9725"),
            run_outbox_id: fixture_platform_id("obx", "9725"),
            node_event_id: fixture_platform_id("evt", "9726"),
            node_outbox_id: fixture_platform_id("obx", "9726"),
            job_event_id: fixture_platform_id("evt", "9727"),
            job_outbox_id: fixture_platform_id("obx", "9727"),
            activations: vec![ControllerActivationSlot {
                node_execution_id: fixture_platform_id("nod", "9728"),
                orchestration_job_id: fixture_platform_id("job", "9729"),
                scope: Some(ControllerScopeSlot {
                    scope_instance_id: fixture_platform_id("scp", "972a"),
                    scope_event_id: fixture_platform_id("evt", "972d"),
                    scope_outbox_id: fixture_platform_id("obx", "972d"),
                }),
                node_event_id: fixture_platform_id("evt", "972b"),
                node_outbox_id: fixture_platform_id("obx", "972b"),
                job_event_id: fixture_platform_id("evt", "972c"),
                job_outbox_id: fixture_platform_id("obx", "972c"),
            }],
            pending_nodes: vec![ControllerPendingNodeSlot {
                node_execution_id: fixture_platform_id("nod", "972e"),
                node_event_id: fixture_platform_id("evt", "972f"),
                node_outbox_id: fixture_platform_id("obx", "972f"),
            }],
            structural_exit: None,
            pending_wake: None,
            remainder_cancellations: Vec::new(),
        },
    };
    let mut scheduler = repository.begin_scheduler_transaction().await.unwrap();
    let opened_loop = match scheduler
        .apply_orchestration_controller_step(open_loop_iteration.clone())
        .await
        .unwrap()
    {
        CommandOutcome::Applied(record) => record,
        CommandOutcome::Replayed(_) => panic!("Loop iteration open must apply"),
    };
    scheduler.commit().await.unwrap();
    assert_eq!(opened_loop.activations.len(), 1);
    assert_eq!(opened_loop.created_scopes[0].scope_kind, "loop_iteration");
    assert_eq!(opened_loop.pending_nodes[0].plan_node_key.as_str(), "loop");
    let mut scheduler = repository.begin_scheduler_transaction().await.unwrap();
    assert!(matches!(
        scheduler
            .apply_orchestration_controller_step(open_loop_iteration)
            .await
            .unwrap(),
        CommandOutcome::Replayed(record)
            if record.created_scopes.len() == 1 && record.pending_nodes.len() == 1
    ));
    scheduler.commit().await.unwrap();

    let mut scheduler = repository.begin_scheduler_transaction().await.unwrap();
    let loop_body_claimed = scheduler
        .claim_orchestration_jobs(orchestration_claim_with_ids(
            WORKER_D_ID,
            '2',
            "9730",
            ["9731", "9732", "9733", "9734"],
            ["9735", "9736", "9737"],
        ))
        .await
        .unwrap();
    scheduler.commit().await.unwrap();
    assert_eq!(loop_body_claimed.len(), 1);
    let loop_body_start = start_claimed_orchestration(
        &loop_body_claimed[0],
        WORKER_D_ID,
        '2',
        "9738",
        "9739",
        "973a",
        '3',
        '4',
    );
    let mut scheduler = repository.begin_scheduler_transaction().await.unwrap();
    let loop_body_started = match scheduler
        .start_orchestration_job(loop_body_start.clone())
        .await
        .unwrap()
    {
        CommandOutcome::Applied(record) => record,
        CommandOutcome::Replayed(_) => panic!("Loop body start must apply"),
    };
    scheduler.commit().await.unwrap();
    let loop_body_fence = JobFence {
        expected_job_version: loop_body_started.version,
        ..loop_body_start.fence.clone()
    };
    let loop_body_facts = repository
        .load_expression_controller_facts(&loop_body_fence, &runtime_plan())
        .await
        .unwrap();
    assert!(loop_body_facts.inputs.is_empty());
    let loop_body_plan = runtime_plan();
    let loop_body_evaluation = derive_expression_controller(
        loop_body_plan
            .node(&loop_body_facts.plan_node_key)
            .unwrap(),
        vec![],
        loop_body_facts.node_execution_id.clone(),
        loop_body_facts.node_execution_version,
        loop_body_facts.loop_iteration,
        ExpressionLimits::ABSOLUTE,
    )
    .unwrap();
    let loop_body_value_id = fixture_platform_id("val", "973e");
    let rolled_scope_id = fixture_platform_id("scp", "974d");
    let rolled_value_id = fixture_platform_id("val", "974f");
    let settle_loop_iteration = ApplyOrchestrationControllerStep {
        fence: loop_body_fence,
        plan: loop_body_plan,
        observation: loop_body_evaluation.observation.clone(),
        idempotency_key_digest: digest('5'),
        request_digest: digest('6'),
        receipt_expires_at: Utc::now() + Duration::hours(1),
        mutations: ControllerStepMutationIds {
            receipt_id: fixture_platform_id("rcp", "9740"),
            quota_entry_ids: ["9741", "9742", "9743", "9744"]
                .map(|suffix| fixture_platform_id("qle", suffix))
                .to_vec(),
            run_event_id: fixture_platform_id("evt", "9745"),
            run_outbox_id: fixture_platform_id("obx", "9745"),
            node_event_id: fixture_platform_id("evt", "9746"),
            node_outbox_id: fixture_platform_id("obx", "9746"),
            job_event_id: fixture_platform_id("evt", "9747"),
            job_outbox_id: fixture_platform_id("obx", "9747"),
            activations: Vec::new(),
            pending_nodes: Vec::new(),
            structural_exit: Some(ControllerStructuralExitSlot {
                scope_closing_event_id: fixture_platform_id("evt", "9748"),
                scope_closing_outbox_id: fixture_platform_id("obx", "9748"),
                scope_terminal_event_id: fixture_platform_id("evt", "9749"),
                scope_terminal_outbox_id: fixture_platform_id("obx", "9749"),
                loop_rollover: Some(ControllerLoopRolloverSlot {
                    scope: ControllerScopeSlot {
                        scope_instance_id: rolled_scope_id.clone(),
                        scope_event_id: fixture_platform_id("evt", "974e"),
                        scope_outbox_id: fixture_platform_id("obx", "974e"),
                    },
                    carried_value_ids: vec![rolled_value_id.clone()],
                }),
            }),
            pending_wake: Some(ControllerPendingWakeSlot {
                orchestration_job_id: fixture_platform_id("job", "974a"),
                request_digest: digest('7'),
                node_event_id: fixture_platform_id("evt", "974b"),
                node_outbox_id: fixture_platform_id("obx", "974b"),
                job_event_id: fixture_platform_id("evt", "974c"),
                job_outbox_id: fixture_platform_id("obx", "974c"),
            }),
            remainder_cancellations: Vec::new(),
        },
    };
    let derived_loop_body_settlement = ApplyDerivedExpressionControllerStep {
        step: settle_loop_iteration,
        materialized_inputs: vec![],
        evaluation: loop_body_evaluation,
        output_value_ids: vec![loop_body_value_id.clone()],
    };
    let mut conflicting_rollover = derived_loop_body_settlement.clone();
    conflicting_rollover
        .step
        .mutations
        .structural_exit
        .as_mut()
        .unwrap()
        .loop_rollover
        .as_mut()
        .unwrap()
        .carried_value_ids = vec![loop_admission.input.value_id.clone()];
    let mut scheduler = repository.begin_scheduler_transaction().await.unwrap();
    assert!(matches!(
        scheduler
            .apply_derived_expression_controller_step(conflicting_rollover)
            .await,
        Err(RepositoryError::Database(_))
    ));
    scheduler.rollback().await.unwrap();
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT count(*) FROM insight_platform.run_values WHERE tenant_id = $1 AND run_id = $2",
        )
        .bind(TENANT_ID)
        .bind(loop_admission.run_id.to_string())
        .fetch_one(&pool)
        .await
        .unwrap(),
        1
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT count(*) FROM insight_platform.run_nodes WHERE tenant_id = $1 AND node_id = $2",
        )
        .bind(TENANT_ID)
        .bind(rolled_scope_id.to_string())
        .fetch_one(&pool)
        .await
        .unwrap(),
        0
    );
    let mut scheduler = repository.begin_scheduler_transaction().await.unwrap();
    let settled_loop = match scheduler
        .apply_derived_expression_controller_step(derived_loop_body_settlement.clone())
        .await
        .unwrap()
    {
        CommandOutcome::Applied(record) => record,
        CommandOutcome::Replayed(_) => panic!("Loop iteration settlement must apply"),
    };
    scheduler.commit().await.unwrap();
    assert_eq!(settled_loop.settled_scopes[0].state, "succeeded");
    assert_eq!(settled_loop.created_scopes.len(), 1);
    assert_eq!(settled_loop.created_scopes[0].scope_id, rolled_scope_id.to_string());
    assert_eq!(settled_loop.woken_nodes.len(), 1);
    assert_eq!(settled_loop.woken_nodes[0].plan_node_key.as_str(), "loop");
    let mut scheduler = repository.begin_scheduler_transaction().await.unwrap();
    assert!(matches!(
        scheduler
            .apply_derived_expression_controller_step(derived_loop_body_settlement)
            .await
            .unwrap(),
        CommandOutcome::Replayed(record)
            if record.settled_scopes[0].scope_kind == "loop_iteration"
                && record.woken_nodes.len() == 1
    ));
    scheduler.commit().await.unwrap();
    let rolled_scope_payload: Value = sqlx::query_scalar(
        "SELECT payload FROM insight_platform.run_nodes WHERE tenant_id = $1 AND node_id = $2 AND state = 'open'",
    )
    .bind(TENANT_ID)
    .bind(rolled_scope_id.to_string())
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(
        rolled_scope_payload["descriptor"]["iteration"],
        json!(1)
    );
    assert!(rolled_scope_payload
        .to_string()
        .contains(&rolled_value_id.to_string()));
    let rolled_value: (String, Value) = sqlx::query_as(
        "SELECT classification, inline_value FROM insight_platform.run_values WHERE tenant_id = $1 AND value_id = $2",
    )
    .bind(TENANT_ID)
    .bind(rolled_value_id.to_string())
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(rolled_value.0, DataClassification::Internal.as_str());
    assert_eq!(rolled_value.1, json!({"iteration": 0}));

    let mut scheduler = repository.begin_scheduler_transaction().await.unwrap();
    let loop_continuation_claimed = scheduler
        .claim_orchestration_jobs(orchestration_claim_with_ids(
            WORKER_D_ID,
            '8',
            "9750",
            ["9751", "9752", "9753", "9754"],
            ["9755", "9756", "9757"],
        ))
        .await
        .unwrap();
    scheduler.commit().await.unwrap();
    assert_eq!(loop_continuation_claimed.len(), 1);
    let loop_continuation_start = start_claimed_orchestration(
        &loop_continuation_claimed[0],
        WORKER_D_ID,
        '8',
        "9758",
        "9759",
        "975a",
        '9',
        'a',
    );
    let mut scheduler = repository.begin_scheduler_transaction().await.unwrap();
    let loop_continuation_started = match scheduler
        .start_orchestration_job(loop_continuation_start.clone())
        .await
        .unwrap()
    {
        CommandOutcome::Applied(record) => record,
        CommandOutcome::Replayed(_) => panic!("Loop continuation start must apply"),
    };
    scheduler.commit().await.unwrap();
    let continuation_scope_id: String = sqlx::query_scalar(
        "SELECT scope_id FROM insight_platform.run_nodes WHERE tenant_id = $1 AND node_id = $2",
    )
    .bind(TENANT_ID)
    .bind(loop_continuation_claimed[0].job.node_id.as_ref().unwrap())
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(continuation_scope_id, rolled_scope_id.to_string());
    let second_loop_fence = JobFence {
        expected_job_version: loop_continuation_started.version,
        ..loop_continuation_start.fence.clone()
    };
    let second_loop_facts = repository
        .load_expression_controller_facts(&second_loop_fence, &runtime_plan())
        .await
        .unwrap();
    assert_eq!(second_loop_facts.loop_iteration, 1);
    let second_loop_plan = runtime_plan();
    let second_loop_evaluation = derive_expression_controller(
        second_loop_plan
            .node(&second_loop_facts.plan_node_key)
            .unwrap(),
        vec![],
        second_loop_facts.node_execution_id.clone(),
        second_loop_facts.node_execution_version,
        second_loop_facts.loop_iteration,
        ExpressionLimits::ABSOLUTE,
    )
    .unwrap();
    let open_second_loop_body = ApplyDerivedExpressionControllerStep {
        step: ApplyOrchestrationControllerStep {
            fence: second_loop_fence,
            plan: second_loop_plan,
            observation: second_loop_evaluation.observation.clone(),
            idempotency_key_digest: digest('1'),
            request_digest: digest('2'),
            receipt_expires_at: Utc::now() + Duration::hours(1),
            mutations: ControllerStepMutationIds {
                receipt_id: fixture_platform_id("rcp", "ac00"),
                quota_entry_ids: ["ac01", "ac02", "ac03", "ac04"]
                    .map(|suffix| fixture_platform_id("qle", suffix))
                    .to_vec(),
                run_event_id: fixture_platform_id("evt", "ac05"),
                run_outbox_id: fixture_platform_id("obx", "ac05"),
                node_event_id: fixture_platform_id("evt", "ac06"),
                node_outbox_id: fixture_platform_id("obx", "ac06"),
                job_event_id: fixture_platform_id("evt", "ac07"),
                job_outbox_id: fixture_platform_id("obx", "ac07"),
                activations: vec![ControllerActivationSlot {
                    node_execution_id: fixture_platform_id("nod", "ac08"),
                    orchestration_job_id: fixture_platform_id("job", "ac09"),
                    scope: None,
                    node_event_id: fixture_platform_id("evt", "ac0a"),
                    node_outbox_id: fixture_platform_id("obx", "ac0a"),
                    job_event_id: fixture_platform_id("evt", "ac0b"),
                    job_outbox_id: fixture_platform_id("obx", "ac0b"),
                }],
                pending_nodes: vec![ControllerPendingNodeSlot {
                    node_execution_id: fixture_platform_id("nod", "ac0c"),
                    node_event_id: fixture_platform_id("evt", "ac0d"),
                    node_outbox_id: fixture_platform_id("obx", "ac0d"),
                }],
                structural_exit: None,
                pending_wake: None,
                remainder_cancellations: vec![],
            },
        },
        materialized_inputs: vec![],
        evaluation: second_loop_evaluation,
        output_value_ids: vec![],
    };
    let mut scheduler = repository.begin_scheduler_transaction().await.unwrap();
    let second_loop_body = match scheduler
        .apply_derived_expression_controller_step(open_second_loop_body)
        .await
        .unwrap()
    {
        CommandOutcome::Applied(record) => record,
        CommandOutcome::Replayed(_) => panic!("second Loop condition must apply"),
    };
    scheduler.commit().await.unwrap();
    assert!(second_loop_body.created_scopes.is_empty());
    let second_body_scope: String = sqlx::query_scalar(
        "SELECT scope_id FROM insight_platform.run_nodes WHERE tenant_id = $1 AND node_id = $2",
    )
    .bind(TENANT_ID)
    .bind(&second_loop_body.activations[0].node_id)
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(second_body_scope, rolled_scope_id.to_string());

    let mut scheduler = repository.begin_scheduler_transaction().await.unwrap();
    let second_body_claimed = scheduler
        .claim_orchestration_jobs(orchestration_claim_with_ids(
            WORKER_D_ID,
            '3',
            "ac10",
            ["ac11", "ac12", "ac13", "ac14"],
            ["ac15", "ac16", "ac17"],
        ))
        .await
        .unwrap();
    scheduler.commit().await.unwrap();
    assert_eq!(second_body_claimed.len(), 1);
    let second_body_start = start_claimed_orchestration(
        &second_body_claimed[0],
        WORKER_D_ID,
        '3',
        "ac18",
        "ac19",
        "ac1a",
        '4',
        '5',
    );
    let mut scheduler = repository.begin_scheduler_transaction().await.unwrap();
    let second_body_started = match scheduler
        .start_orchestration_job(second_body_start.clone())
        .await
        .unwrap()
    {
        CommandOutcome::Applied(record) => record,
        CommandOutcome::Replayed(_) => panic!("second Loop body start must apply"),
    };
    scheduler.commit().await.unwrap();
    let second_body_fence = JobFence {
        expected_job_version: second_body_started.version,
        ..second_body_start.fence.clone()
    };
    let second_body_facts = repository
        .load_expression_controller_facts(&second_body_fence, &runtime_plan())
        .await
        .unwrap();
    let second_body_plan = runtime_plan();
    let second_body_evaluation = derive_expression_controller(
        second_body_plan
            .node(&second_body_facts.plan_node_key)
            .unwrap(),
        vec![],
        second_body_facts.node_execution_id.clone(),
        second_body_facts.node_execution_version,
        second_body_facts.loop_iteration,
        ExpressionLimits::ABSOLUTE,
    )
    .unwrap();
    let final_scope_id = fixture_platform_id("scp", "ac2b");
    let final_carried_value_id = fixture_platform_id("val", "ac2d");
    let settle_second_body = ApplyDerivedExpressionControllerStep {
        step: ApplyOrchestrationControllerStep {
            fence: second_body_fence,
            plan: second_body_plan,
            observation: second_body_evaluation.observation.clone(),
            idempotency_key_digest: digest('6'),
            request_digest: digest('7'),
            receipt_expires_at: Utc::now() + Duration::hours(1),
            mutations: ControllerStepMutationIds {
                receipt_id: fixture_platform_id("rcp", "ac21"),
                quota_entry_ids: ["ac22", "ac23", "ac24", "ac25"]
                    .map(|suffix| fixture_platform_id("qle", suffix))
                    .to_vec(),
                run_event_id: fixture_platform_id("evt", "ac26"),
                run_outbox_id: fixture_platform_id("obx", "ac26"),
                node_event_id: fixture_platform_id("evt", "ac27"),
                node_outbox_id: fixture_platform_id("obx", "ac27"),
                job_event_id: fixture_platform_id("evt", "ac28"),
                job_outbox_id: fixture_platform_id("obx", "ac28"),
                activations: vec![],
                pending_nodes: vec![],
                structural_exit: Some(ControllerStructuralExitSlot {
                    scope_closing_event_id: fixture_platform_id("evt", "ac29"),
                    scope_closing_outbox_id: fixture_platform_id("obx", "ac29"),
                    scope_terminal_event_id: fixture_platform_id("evt", "ac2a"),
                    scope_terminal_outbox_id: fixture_platform_id("obx", "ac2a"),
                    loop_rollover: Some(ControllerLoopRolloverSlot {
                        scope: ControllerScopeSlot {
                            scope_instance_id: final_scope_id.clone(),
                            scope_event_id: fixture_platform_id("evt", "ac2c"),
                            scope_outbox_id: fixture_platform_id("obx", "ac2c"),
                        },
                        carried_value_ids: vec![final_carried_value_id.clone()],
                    }),
                }),
                pending_wake: Some(ControllerPendingWakeSlot {
                    orchestration_job_id: fixture_platform_id("job", "ac2e"),
                    request_digest: digest('8'),
                    node_event_id: fixture_platform_id("evt", "ac2f"),
                    node_outbox_id: fixture_platform_id("obx", "ac2f"),
                    job_event_id: fixture_platform_id("evt", "ac30"),
                    job_outbox_id: fixture_platform_id("obx", "ac30"),
                }),
                remainder_cancellations: vec![],
            },
        },
        materialized_inputs: vec![],
        evaluation: second_body_evaluation,
        output_value_ids: vec![fixture_platform_id("val", "ac20")],
    };
    let mut scheduler = repository.begin_scheduler_transaction().await.unwrap();
    let second_settlement = match scheduler
        .apply_derived_expression_controller_step(settle_second_body)
        .await
        .unwrap()
    {
        CommandOutcome::Applied(record) => record,
        CommandOutcome::Replayed(_) => panic!("second Loop settlement must apply"),
    };
    scheduler.commit().await.unwrap();
    assert_eq!(second_settlement.created_scopes[0].scope_id, final_scope_id.to_string());

    let mut scheduler = repository.begin_scheduler_transaction().await.unwrap();
    let final_continuation_claimed = scheduler
        .claim_orchestration_jobs(orchestration_claim_with_ids(
            WORKER_D_ID,
            '9',
            "ac31",
            ["ac32", "ac33", "ac34", "ac35"],
            ["ac36", "ac37", "ac38"],
        ))
        .await
        .unwrap();
    scheduler.commit().await.unwrap();
    assert_eq!(final_continuation_claimed.len(), 1);
    let final_continuation_start = start_claimed_orchestration(
        &final_continuation_claimed[0],
        WORKER_D_ID,
        '9',
        "ac39",
        "ac3a",
        "ac3b",
        'a',
        'b',
    );
    let mut scheduler = repository.begin_scheduler_transaction().await.unwrap();
    let final_continuation_started = match scheduler
        .start_orchestration_job(final_continuation_start.clone())
        .await
        .unwrap()
    {
        CommandOutcome::Applied(record) => record,
        CommandOutcome::Replayed(_) => panic!("final Loop continuation start must apply"),
    };
    scheduler.commit().await.unwrap();
    let exit_loop = ApplyOrchestrationControllerStep {
        fence: JobFence {
            expected_job_version: final_continuation_started.version,
            ..final_continuation_start.fence.clone()
        },
        plan: runtime_plan(),
        observation: ControllerObservation::Loop {
            iteration: 2,
            condition: false,
        },
        idempotency_key_digest: digest('b'),
        request_digest: digest('c'),
        receipt_expires_at: Utc::now() + Duration::hours(1),
        mutations: ControllerStepMutationIds {
            receipt_id: fixture_platform_id("rcp", "9760"),
            quota_entry_ids: ["9761", "9762", "9763", "9764"]
                .map(|suffix| fixture_platform_id("qle", suffix))
                .to_vec(),
            run_event_id: fixture_platform_id("evt", "9765"),
            run_outbox_id: fixture_platform_id("obx", "9765"),
            node_event_id: fixture_platform_id("evt", "9766"),
            node_outbox_id: fixture_platform_id("obx", "9766"),
            job_event_id: fixture_platform_id("evt", "9767"),
            job_outbox_id: fixture_platform_id("obx", "9767"),
            activations: vec![ControllerActivationSlot {
                node_execution_id: fixture_platform_id("nod", "9768"),
                orchestration_job_id: fixture_platform_id("job", "9769"),
                scope: None,
                node_event_id: fixture_platform_id("evt", "976a"),
                node_outbox_id: fixture_platform_id("obx", "976a"),
                job_event_id: fixture_platform_id("evt", "976b"),
                job_outbox_id: fixture_platform_id("obx", "976b"),
            }],
            pending_nodes: Vec::new(),
            structural_exit: Some(ControllerStructuralExitSlot {
                scope_closing_event_id: fixture_platform_id("evt", "976c"),
                scope_closing_outbox_id: fixture_platform_id("obx", "976c"),
                scope_terminal_event_id: fixture_platform_id("evt", "976d"),
                scope_terminal_outbox_id: fixture_platform_id("obx", "976d"),
                loop_rollover: None,
            }),
            pending_wake: None,
            remainder_cancellations: Vec::new(),
        },
    };
    let mut scheduler = repository.begin_scheduler_transaction().await.unwrap();
    let exited_loop = match scheduler
        .apply_orchestration_controller_step(exit_loop)
        .await
        .unwrap()
    {
        CommandOutcome::Applied(record) => record,
        CommandOutcome::Replayed(_) => panic!("Loop exit must apply"),
    };
    scheduler.commit().await.unwrap();
    assert_eq!(exited_loop.activations[0].plan_node_key.as_str(), "finish");
    let closed_rolled_scope: (String, i64) = sqlx::query_as(
        "SELECT state, version FROM insight_platform.run_nodes WHERE tenant_id = $1 AND node_id = $2",
    )
    .bind(TENANT_ID)
    .bind(rolled_scope_id.to_string())
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(closed_rolled_scope.0, "succeeded");
    assert_eq!(closed_rolled_scope.1, 4);
    let final_scope: (String, i64, String) = sqlx::query_as(
        "SELECT state, version, parent_node_id FROM insight_platform.run_nodes WHERE tenant_id = $1 AND node_id = $2",
    )
    .bind(TENANT_ID)
    .bind(final_scope_id.to_string())
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(final_scope.0, "succeeded");
    assert_eq!(final_scope.1, 3);
    let first_scope_parent: String = sqlx::query_scalar(
        "SELECT parent_node_id FROM insight_platform.run_nodes WHERE tenant_id = $1 AND node_id = $2",
    )
    .bind(TENANT_ID)
    .bind(rolled_scope_id.to_string())
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(final_scope.2, first_scope_parent);
    assert_ne!(final_scope.2, rolled_scope_id.to_string());
    let cancelling_loop_run = applied(
        run_command!(
            repository,
            request_run_cancel,
            RequestRunCancel {
                audit: audit(TENANT_ID, PRINCIPAL_ID, "9770", 'd', 'e'),
                run_id: loop_admission.run_id.clone(),
                expected_run_version: exited_loop.run.version,
                expected_cancel_generation: 0,
                reason_code: "fixture_cleanup".to_owned(),
            }
        )
        .unwrap(),
    );
    assert_eq!(cancelling_loop_run.state, "cancelling");
    let mut scheduler = repository.begin_scheduler_transaction().await.unwrap();
    let loop_cleanup = scheduler
        .drive_orchestration_convergence(orchestration_convergence(
            ["9771", "9772", "9773", "9774"],
            ["9775", "9776", "9777", "9778", "9779", "977a"],
        ))
        .await
        .unwrap();
    scheduler.commit().await.unwrap();
    assert_eq!(loop_cleanup.len(), 1);
    assert_eq!(loop_cleanup[0].completed.run.state, "cancelled");

    let mut map_profile = checked_in_hard_limit_profile();
    map_profile.registry_plan.branch_legs.q1_default = 2;
    let map_repository =
        PgRepository::with_hard_limit_profile(pool.clone(), &map_profile).unwrap();
    let mut map_admission = admission(
        AdmissionIds {
            run: "run_0198f1c3-9a00-7c3e-b1f3-773c28369800",
            scope: "scp_0198f1c3-9a00-7c3e-b1f3-773c28369801",
            node: "nod_0198f1c3-9a00-7c3e-b1f3-773c28369802",
            job: "job_0198f1c3-9a00-7c3e-b1f3-773c28369803",
            value: "val_0198f1c3-9a00-7c3e-b1f3-773c28369804",
        },
        audit(TENANT_ID, PRINCIPAL_ID, "9805", '7', '8'),
        bindings.clone(),
        Utc::now() + Duration::minutes(5),
    );
    map_admission.entry_plan_node_key = PlanNodeKey::new("map".to_owned()).unwrap();
    map_admission.entry_node_kind = PlanNodeKind::Map;
    applied(run_command!(map_repository, admit_run, map_admission.clone()).unwrap());
    let mut scheduler = map_repository.begin_scheduler_transaction().await.unwrap();
    let map_claimed = scheduler
        .claim_orchestration_jobs(orchestration_claim_with_ids(
            WORKER_D_ID,
            '9',
            "9810",
            ["9811", "9812", "9813", "9814"],
            ["9815", "9816", "9817"],
        ))
        .await
        .unwrap();
    scheduler.commit().await.unwrap();
    assert_eq!(map_claimed.len(), 1);
    let map_start = start_claimed_orchestration(
        &map_claimed[0],
        WORKER_D_ID,
        '9',
        "9818",
        "9819",
        "981a",
        'a',
        'b',
    );
    let mut scheduler = map_repository.begin_scheduler_transaction().await.unwrap();
    let map_started = match scheduler
        .start_orchestration_job(map_start.clone())
        .await
        .unwrap()
    {
        CommandOutcome::Applied(record) => record,
        CommandOutcome::Replayed(_) => panic!("Map start must apply"),
    };
    scheduler.commit().await.unwrap();
    let first_map_fence = JobFence {
        expected_job_version: map_started.version,
        ..map_start.fence.clone()
    };
    let first_map_facts = map_repository
        .load_expression_controller_facts(&first_map_fence, &runtime_plan())
        .await
        .unwrap();
    assert!(first_map_facts.inputs.is_empty());
    let first_map_plan = runtime_plan();
    let first_map_evaluation = derive_expression_controller(
        first_map_plan
            .node(&first_map_facts.plan_node_key)
            .unwrap(),
        vec![],
        first_map_facts.node_execution_id.clone(),
        first_map_facts.node_execution_version,
        first_map_facts.loop_iteration,
        ExpressionLimits::ABSOLUTE,
    )
    .unwrap();
    assert_eq!(
        first_map_evaluation.observation,
        ControllerObservation::Map { item_count: 3 }
    );
    let first_map_value_ids = [
        fixture_platform_id("val", "982e"),
        fixture_platform_id("val", "982f"),
    ];
    let open_first_map_batch = ApplyOrchestrationControllerStep {
        fence: first_map_fence,
        plan: first_map_plan,
        observation: first_map_evaluation.observation.clone(),
        idempotency_key_digest: digest('c'),
        request_digest: digest('d'),
        receipt_expires_at: Utc::now() + Duration::hours(1),
        mutations: ControllerStepMutationIds {
            receipt_id: fixture_platform_id("rcp", "9820"),
            quota_entry_ids: ["9821", "9822", "9823", "9824"]
                .map(|suffix| fixture_platform_id("qle", suffix))
                .to_vec(),
            run_event_id: fixture_platform_id("evt", "9825"),
            run_outbox_id: fixture_platform_id("obx", "9825"),
            node_event_id: fixture_platform_id("evt", "9826"),
            node_outbox_id: fixture_platform_id("obx", "9826"),
            job_event_id: fixture_platform_id("evt", "9827"),
            job_outbox_id: fixture_platform_id("obx", "9827"),
            activations: vec![
                ControllerActivationSlot {
                    node_execution_id: fixture_platform_id("nod", "9828"),
                    orchestration_job_id: fixture_platform_id("job", "9829"),
                    scope: Some(ControllerScopeSlot {
                        scope_instance_id: fixture_platform_id("scp", "982a"),
                        scope_event_id: fixture_platform_id("evt", "982d"),
                        scope_outbox_id: fixture_platform_id("obx", "982d"),
                    }),
                    node_event_id: fixture_platform_id("evt", "982b"),
                    node_outbox_id: fixture_platform_id("obx", "982b"),
                    job_event_id: fixture_platform_id("evt", "982c"),
                    job_outbox_id: fixture_platform_id("obx", "982c"),
                },
                ControllerActivationSlot {
                    node_execution_id: fixture_platform_id("nod", "9838"),
                    orchestration_job_id: fixture_platform_id("job", "9839"),
                    scope: Some(ControllerScopeSlot {
                        scope_instance_id: fixture_platform_id("scp", "983a"),
                        scope_event_id: fixture_platform_id("evt", "983d"),
                        scope_outbox_id: fixture_platform_id("obx", "983d"),
                    }),
                    node_event_id: fixture_platform_id("evt", "983b"),
                    node_outbox_id: fixture_platform_id("obx", "983b"),
                    job_event_id: fixture_platform_id("evt", "983c"),
                    job_outbox_id: fixture_platform_id("obx", "983c"),
                },
            ],
            pending_nodes: vec![ControllerPendingNodeSlot {
                node_execution_id: fixture_platform_id("nod", "9840"),
                node_event_id: fixture_platform_id("evt", "9841"),
                node_outbox_id: fixture_platform_id("obx", "9841"),
            }],
            structural_exit: None,
            pending_wake: Some(ControllerPendingWakeSlot {
                orchestration_job_id: fixture_platform_id("job", "9806"),
                request_digest: digest('e'),
                node_event_id: fixture_platform_id("evt", "9842"),
                node_outbox_id: fixture_platform_id("obx", "9842"),
                job_event_id: fixture_platform_id("evt", "9843"),
                job_outbox_id: fixture_platform_id("obx", "9843"),
            }),
            remainder_cancellations: Vec::new(),
        },
    };
    let first_map_derived = ApplyDerivedExpressionControllerStep {
        step: open_first_map_batch,
        materialized_inputs: vec![],
        evaluation: first_map_evaluation,
        output_value_ids: first_map_value_ids.to_vec(),
    };
    let first_map_requirements = map_repository
        .load_controller_mutation_requirements(
            &first_map_derived.step.fence,
            &first_map_derived.step.plan,
            &first_map_derived.step.observation,
        )
        .await
        .unwrap();
    assert_eq!(first_map_requirements.activation_scopes, vec![true, true]);
    assert_eq!(first_map_requirements.pending_node_count, 1);
    assert!(first_map_requirements.pending_wake);
    let mut scheduler = map_repository.begin_scheduler_transaction().await.unwrap();
    let first_map_batch = match scheduler
        .apply_derived_expression_controller_step(first_map_derived.clone())
        .await
        .unwrap()
    {
        CommandOutcome::Applied(record) => record,
        CommandOutcome::Replayed(_) => panic!("first Map batch must apply"),
    };
    scheduler.commit().await.unwrap();
    assert_eq!(first_map_batch.activations.len(), 2);
    assert_eq!(first_map_batch.created_scopes.len(), 2);
    assert_eq!(first_map_batch.pending_nodes.len(), 1);
    assert_eq!(first_map_batch.woken_nodes.len(), 1);
    assert_eq!(first_map_batch.woken_nodes[0].node_kind, PlanNodeKind::Map);
    let mut scheduler = map_repository.begin_scheduler_transaction().await.unwrap();
    assert!(matches!(
        scheduler
            .apply_derived_expression_controller_step(first_map_derived)
            .await
            .unwrap(),
        CommandOutcome::Replayed(record)
            if record.activations.len() == 2 && record.woken_nodes.len() == 1
    ));
    scheduler.commit().await.unwrap();

    let mut scheduler = map_repository.begin_scheduler_transaction().await.unwrap();
    let map_admission_continuation = scheduler
        .claim_orchestration_jobs(orchestration_claim_with_ids(
            WORKER_D_ID,
            'f',
            "9844",
            ["9845", "9846", "9847", "9848"],
            ["9849", "984a", "984b"],
        ))
        .await
        .unwrap();
    scheduler.commit().await.unwrap();
    assert_eq!(map_admission_continuation.len(), 1);
    assert_eq!(
        map_admission_continuation[0].job.job_id,
        first_map_batch.woken_nodes[0].job.job_id
    );
    let map_admission_start = start_claimed_orchestration(
        &map_admission_continuation[0],
        WORKER_D_ID,
        'f',
        "984c",
        "984d",
        "984e",
        '0',
        '1',
    );
    let mut scheduler = map_repository.begin_scheduler_transaction().await.unwrap();
    let map_admission_started = match scheduler
        .start_orchestration_job(map_admission_start.clone())
        .await
        .unwrap()
    {
        CommandOutcome::Applied(record) => record,
        CommandOutcome::Replayed(_) => panic!("Map admission continuation start must apply"),
    };
    scheduler.commit().await.unwrap();
    let final_map_fence = JobFence {
        expected_job_version: map_admission_started.version,
        ..map_admission_start.fence.clone()
    };
    let final_map_facts = map_repository
        .load_expression_controller_facts(&final_map_fence, &runtime_plan())
        .await
        .unwrap();
    assert!(final_map_facts.inputs.is_empty());
    let final_map_plan = runtime_plan();
    let final_map_evaluation = derive_expression_controller(
        final_map_plan
            .node(&final_map_facts.plan_node_key)
            .unwrap(),
        vec![],
        final_map_facts.node_execution_id.clone(),
        final_map_facts.node_execution_version,
        final_map_facts.loop_iteration,
        ExpressionLimits::ABSOLUTE,
    )
    .unwrap();
    let final_map_value_id = fixture_platform_id("val", "984f");
    let open_final_map_batch = ApplyOrchestrationControllerStep {
        fence: final_map_fence,
        plan: final_map_plan,
        observation: final_map_evaluation.observation.clone(),
        idempotency_key_digest: digest('2'),
        request_digest: digest('3'),
        receipt_expires_at: Utc::now() + Duration::hours(1),
        mutations: ControllerStepMutationIds {
            receipt_id: fixture_platform_id("rcp", "9850"),
            quota_entry_ids: ["9851", "9852", "9853", "9854"]
                .map(|suffix| fixture_platform_id("qle", suffix))
                .to_vec(),
            run_event_id: fixture_platform_id("evt", "9855"),
            run_outbox_id: fixture_platform_id("obx", "9855"),
            node_event_id: fixture_platform_id("evt", "9856"),
            node_outbox_id: fixture_platform_id("obx", "9856"),
            job_event_id: fixture_platform_id("evt", "9857"),
            job_outbox_id: fixture_platform_id("obx", "9857"),
            activations: vec![ControllerActivationSlot {
                node_execution_id: fixture_platform_id("nod", "9858"),
                orchestration_job_id: fixture_platform_id("job", "9859"),
                scope: Some(ControllerScopeSlot {
                    scope_instance_id: fixture_platform_id("scp", "985a"),
                    scope_event_id: fixture_platform_id("evt", "985d"),
                    scope_outbox_id: fixture_platform_id("obx", "985d"),
                }),
                node_event_id: fixture_platform_id("evt", "985b"),
                node_outbox_id: fixture_platform_id("obx", "985b"),
                job_event_id: fixture_platform_id("evt", "985c"),
                job_outbox_id: fixture_platform_id("obx", "985c"),
            }],
            pending_nodes: vec![ControllerPendingNodeSlot {
                node_execution_id: fixture_platform_id("nod", "985e"),
                node_event_id: fixture_platform_id("evt", "985f"),
                node_outbox_id: fixture_platform_id("obx", "985f"),
            }],
            structural_exit: None,
            pending_wake: None,
            remainder_cancellations: Vec::new(),
        },
    };
    let mut scheduler = map_repository.begin_scheduler_transaction().await.unwrap();
    let final_map_batch = match scheduler
        .apply_derived_expression_controller_step(ApplyDerivedExpressionControllerStep {
            step: open_final_map_batch,
            materialized_inputs: vec![],
            evaluation: final_map_evaluation,
            output_value_ids: vec![final_map_value_id.clone()],
        })
        .await
        .unwrap()
    {
        CommandOutcome::Applied(record) => record,
        CommandOutcome::Replayed(_) => panic!("final Map batch must apply"),
    };
    scheduler.commit().await.unwrap();
    assert_eq!(final_map_batch.activations.len(), 1);
    assert_eq!(final_map_batch.created_scopes.len(), 1);
    assert_eq!(final_map_batch.pending_nodes.len(), 1);
    assert!(final_map_batch.woken_nodes.is_empty());
    let map_scope_indexes = sqlx::query_scalar::<_, i32>(
        r#"
        SELECT (payload #>> '{descriptor,item_index}')::integer
        FROM insight_platform.run_nodes
        WHERE tenant_id = $1 AND run_id = $2 AND node_kind = 'map_item'
        ORDER BY (payload #>> '{descriptor,item_index}')::integer
        "#,
    )
    .bind(TENANT_ID)
    .bind(map_admission.run_id.to_string())
    .fetch_all(&pool)
    .await
    .unwrap();
    assert_eq!(map_scope_indexes, vec![0, 1, 2]);
    let stored_map_values = sqlx::query_as::<_, (String, String, Value)>(
        r#"
        SELECT value_id, classification, inline_value
        FROM insight_platform.run_values
        WHERE tenant_id = $1 AND value_id = ANY($2)
        ORDER BY value_id
        "#,
    )
    .bind(TENANT_ID)
    .bind(
        first_map_value_ids
            .iter()
            .chain(std::iter::once(&final_map_value_id))
            .map(ToString::to_string)
            .collect::<Vec<_>>(),
    )
    .fetch_all(&pool)
    .await
    .unwrap();
    assert_eq!(stored_map_values.len(), 3);
    assert!(stored_map_values
        .iter()
        .all(|(_, classification, _)| classification == DataClassification::Internal.as_str()));
    assert_eq!(
        stored_map_values
            .iter()
            .map(|(_, _, value)| value.clone())
            .collect::<Vec<_>>(),
        vec![json!({"item": 0}), json!({"item": 1}), json!({"item": 2})]
    );
    for (scope, value_id) in first_map_batch
        .created_scopes
        .iter()
        .chain(final_map_batch.created_scopes.iter())
        .zip(
            first_map_value_ids
                .iter()
                .chain(std::iter::once(&final_map_value_id)),
        )
    {
        let payload: Value = sqlx::query_scalar(
            "SELECT payload FROM insight_platform.run_nodes WHERE tenant_id = $1 AND node_id = $2",
        )
        .bind(TENANT_ID)
        .bind(scope.scope_id.to_string())
        .fetch_one(&pool)
        .await
        .unwrap();
        assert!(payload.to_string().contains(&value_id.to_string()));
        assert_eq!(
            payload["environment"]["bindings"][0]["port"]["producer_node_id"],
            json!("map")
        );
        assert_eq!(
            payload["environment"]["bindings"][0]["port"]["port_id"],
            json!("item")
        );
    }

    let map_item_fixtures = [
        (
            '4',
            "9860",
            ["9861", "9862", "9863", "9864"],
            ["9865", "9866", "9867"],
            ["9868", "9869", "986a"],
            "986b",
            ["986c", "986d", "986e", "986f"],
            ["9870", "9871", "9872"],
            ["9873", "9874"],
            "9875",
            ["9876", "9877"],
        ),
        (
            '5',
            "9880",
            ["9881", "9882", "9883", "9884"],
            ["9885", "9886", "9887"],
            ["9888", "9889", "988a"],
            "988b",
            ["988c", "988d", "988e", "988f"],
            ["9890", "9891", "9892"],
            ["9893", "9894"],
            "9895",
            ["9896", "9897"],
        ),
        (
            '6',
            "98a0",
            ["98a1", "98a2", "98a3", "98a4"],
            ["98a5", "98a6", "98a7"],
            ["98a8", "98a9", "98aa"],
            "98ab",
            ["98ac", "98ad", "98ae", "98af"],
            ["98b0", "98b1", "98b2"],
            ["98b3", "98b4"],
            "98b5",
            ["98b6", "98b7"],
        ),
    ];
    let mut map_settlement_job_id = None;
    for (
        index,
        (
            token,
            reservation,
            claim_quota,
            claim_events,
            start_ids,
            settle_receipt,
            settle_quota,
            settle_events,
            scope_events,
            wake_job,
            wake_events,
        ),
    ) in map_item_fixtures.into_iter().enumerate()
    {
        let mut scheduler = map_repository.begin_scheduler_transaction().await.unwrap();
        let claimed = scheduler
            .claim_orchestration_jobs(orchestration_claim_with_ids(
                WORKER_D_ID,
                token,
                reservation,
                claim_quota,
                claim_events,
            ))
            .await
            .unwrap();
        scheduler.commit().await.unwrap();
        assert_eq!(claimed.len(), 1);
        let start = start_claimed_orchestration(
            &claimed[0],
            WORKER_D_ID,
            token,
            start_ids[0],
            start_ids[1],
            start_ids[2],
            char::from_digit(u32::try_from(index + 7).unwrap(), 16).unwrap(),
            char::from_digit(u32::try_from(index + 8).unwrap(), 16).unwrap(),
        );
        let mut scheduler = map_repository.begin_scheduler_transaction().await.unwrap();
        let started = match scheduler.start_orchestration_job(start.clone()).await.unwrap() {
            CommandOutcome::Applied(record) => record,
            CommandOutcome::Replayed(_) => panic!("Map item start must apply"),
        };
        scheduler.commit().await.unwrap();
        let settle_mutations = ControllerStepMutationIds {
            receipt_id: fixture_platform_id("rcp", settle_receipt),
            quota_entry_ids: settle_quota
                .map(|suffix| fixture_platform_id("qle", suffix))
                .to_vec(),
            run_event_id: fixture_platform_id("evt", settle_events[0]),
            run_outbox_id: fixture_platform_id("obx", settle_events[0]),
            node_event_id: fixture_platform_id("evt", settle_events[1]),
            node_outbox_id: fixture_platform_id("obx", settle_events[1]),
            job_event_id: fixture_platform_id("evt", settle_events[2]),
            job_outbox_id: fixture_platform_id("obx", settle_events[2]),
            activations: Vec::new(),
            pending_nodes: Vec::new(),
            structural_exit: Some(ControllerStructuralExitSlot {
                scope_closing_event_id: fixture_platform_id("evt", scope_events[0]),
                scope_closing_outbox_id: fixture_platform_id("obx", scope_events[0]),
                scope_terminal_event_id: fixture_platform_id("evt", scope_events[1]),
                scope_terminal_outbox_id: fixture_platform_id("obx", scope_events[1]),
                loop_rollover: None,
            }),
            pending_wake: Some(ControllerPendingWakeSlot {
                orchestration_job_id: fixture_platform_id("job", wake_job),
                request_digest: digest(
                    char::from_digit(u32::try_from(index + 12).unwrap(), 16).unwrap(),
                ),
                node_event_id: fixture_platform_id("evt", wake_events[0]),
                node_outbox_id: fixture_platform_id("obx", wake_events[0]),
                job_event_id: fixture_platform_id("evt", wake_events[1]),
                job_outbox_id: fixture_platform_id("obx", wake_events[1]),
            }),
            remainder_cancellations: Vec::new(),
        };
        let settle = ApplyOrchestrationControllerStep {
            fence: JobFence {
                expected_job_version: started.version,
                ..start.fence.clone()
            },
            plan: runtime_plan(),
            observation: ControllerObservation::None,
            idempotency_key_digest: digest(
                char::from_digit(u32::try_from(index + 10).unwrap(), 16).unwrap(),
            ),
            request_digest: digest(
                char::from_digit(u32::try_from(index + 11).unwrap(), 16).unwrap(),
            ),
            receipt_expires_at: Utc::now() + Duration::hours(1),
            mutations: settle_mutations.clone(),
        };
        let (settled_scopes, woken_nodes) = if index == 0 {
            let failure = FailOrchestrationJob {
                fence: settle.fence.clone(),
                plan: settle.plan.clone(),
                cause: OrchestrationFailureCause::Committed {
                    failure: committed_failure.clone(),
                },
                derived_expression: None,
                idempotency_key_digest: settle.idempotency_key_digest.clone(),
                request_digest: settle.request_digest.clone(),
                receipt_expires_at: settle.receipt_expires_at,
                mutations: settle_mutations,
            };
            let mut scheduler = map_repository.begin_scheduler_transaction().await.unwrap();
            let failed = match scheduler
                .fail_orchestration_job(failure.clone())
                .await
                .unwrap()
            {
                CommandOutcome::Applied(record) => record,
                CommandOutcome::Replayed(_) => panic!("Map item failure must apply"),
            };
            scheduler.commit().await.unwrap();
            assert_eq!(failed.failure, committed_failure);
            assert!(failed.cancelled_remainders.is_empty());
            let mut scheduler = map_repository.begin_scheduler_transaction().await.unwrap();
            assert!(matches!(
                scheduler.fail_orchestration_job(failure).await.unwrap(),
                CommandOutcome::Replayed(record)
                    if record.settled_scopes[0].state == "failed"
                        && record.woken_nodes.is_empty()
            ));
            scheduler.commit().await.unwrap();
            (failed.settled_scopes, failed.woken_nodes)
        } else {
            let mut scheduler = map_repository.begin_scheduler_transaction().await.unwrap();
            let settled = match scheduler
                .apply_orchestration_controller_step(settle)
                .await
                .unwrap()
            {
                CommandOutcome::Applied(record) => record,
                CommandOutcome::Replayed(_) => panic!("Map item settlement must apply"),
            };
            scheduler.commit().await.unwrap();
            (settled.settled_scopes, settled.woken_nodes)
        };
        assert_eq!(settled_scopes[0].scope_kind, "map_item");
        assert_eq!(
            settled_scopes[0].state,
            if index == 0 { "failed" } else { "succeeded" }
        );
        if index < 2 {
            assert!(woken_nodes.is_empty());
        } else {
            assert_eq!(woken_nodes.len(), 1);
            map_settlement_job_id = Some(woken_nodes[0].job.job_id.clone());
        }
    }
    let mut scheduler = map_repository.begin_scheduler_transaction().await.unwrap();
    let map_settlement_claimed = scheduler
        .claim_orchestration_jobs(orchestration_claim_with_ids(
            WORKER_D_ID,
            'd',
            "98c0",
            ["98c1", "98c2", "98c3", "98c4"],
            ["98c5", "98c6", "98c7"],
        ))
        .await
        .unwrap();
    scheduler.commit().await.unwrap();
    assert_eq!(map_settlement_claimed.len(), 1);
    assert_eq!(
        map_settlement_claimed[0].job.job_id,
        map_settlement_job_id.unwrap()
    );
    let map_settlement_start = start_claimed_orchestration(
        &map_settlement_claimed[0],
        WORKER_D_ID,
        'd',
        "98c8",
        "98c9",
        "98ca",
        'e',
        'f',
    );
    let mut scheduler = map_repository.begin_scheduler_transaction().await.unwrap();
    let map_settlement_started = match scheduler
        .start_orchestration_job(map_settlement_start.clone())
        .await
        .unwrap()
    {
        CommandOutcome::Applied(record) => record,
        CommandOutcome::Replayed(_) => panic!("Map settlement start must apply"),
    };
    scheduler.commit().await.unwrap();
    let complete_map = ApplyOrchestrationControllerStep {
        fence: JobFence {
            expected_job_version: map_settlement_started.version,
            ..map_settlement_start.fence.clone()
        },
        plan: runtime_plan(),
        observation: ControllerObservation::MapSettlement {
            children: vec![
                ChildOutcome::Failed,
                ChildOutcome::Succeeded,
                ChildOutcome::Succeeded,
            ],
        },
        idempotency_key_digest: digest('0'),
        request_digest: digest('1'),
        receipt_expires_at: Utc::now() + Duration::hours(1),
        mutations: ControllerStepMutationIds {
            receipt_id: fixture_platform_id("rcp", "98d0"),
            quota_entry_ids: ["98d1", "98d2", "98d3", "98d4"]
                .map(|suffix| fixture_platform_id("qle", suffix))
                .to_vec(),
            run_event_id: fixture_platform_id("evt", "98d5"),
            run_outbox_id: fixture_platform_id("obx", "98d5"),
            node_event_id: fixture_platform_id("evt", "98d6"),
            node_outbox_id: fixture_platform_id("obx", "98d6"),
            job_event_id: fixture_platform_id("evt", "98d7"),
            job_outbox_id: fixture_platform_id("obx", "98d7"),
            activations: vec![ControllerActivationSlot {
                node_execution_id: fixture_platform_id("nod", "98d8"),
                orchestration_job_id: fixture_platform_id("job", "98d9"),
                scope: None,
                node_event_id: fixture_platform_id("evt", "98da"),
                node_outbox_id: fixture_platform_id("obx", "98da"),
                job_event_id: fixture_platform_id("evt", "98db"),
                job_outbox_id: fixture_platform_id("obx", "98db"),
            }],
            pending_nodes: Vec::new(),
            structural_exit: None,
            pending_wake: None,
            remainder_cancellations: Vec::new(),
        },
    };
    let mut scheduler = map_repository.begin_scheduler_transaction().await.unwrap();
    let completed_map = match scheduler
        .apply_orchestration_controller_step(complete_map)
        .await
        .unwrap()
    {
        CommandOutcome::Applied(record) => record,
        CommandOutcome::Replayed(_) => panic!("Map settlement completion must apply"),
    };
    scheduler.commit().await.unwrap();
    assert_eq!(completed_map.activations[0].plan_node_key.as_str(), "finish");
    let cancelling_map_run = applied(
        run_command!(
            map_repository,
            request_run_cancel,
            RequestRunCancel {
                audit: audit(TENANT_ID, PRINCIPAL_ID, "98e0", '2', '3'),
                run_id: map_admission.run_id.clone(),
                expected_run_version: completed_map.run.version,
                expected_cancel_generation: 0,
                reason_code: "fixture_cleanup".to_owned(),
            }
        )
        .unwrap(),
    );
    assert_eq!(cancelling_map_run.state, "cancelling");
    let mut scheduler = map_repository.begin_scheduler_transaction().await.unwrap();
    let map_cleanup = scheduler
        .drive_orchestration_convergence(orchestration_convergence(
            ["98e1", "98e2", "98e3", "98e4"],
            ["98e5", "98e6", "98e7", "98e8", "98e9", "98ea"],
        ))
        .await
        .unwrap();
    scheduler.commit().await.unwrap();
    assert_eq!(map_cleanup.len(), 1);
    assert_eq!(map_cleanup[0].completed.run.state, "cancelled");

    let mut fail_fast_admission = admission(
        AdmissionIds {
            run: "run_0198f1c3-9a00-7c3e-b1f3-773c28369900",
            scope: "scp_0198f1c3-9a00-7c3e-b1f3-773c28369901",
            node: "nod_0198f1c3-9a00-7c3e-b1f3-773c28369902",
            job: "job_0198f1c3-9a00-7c3e-b1f3-773c28369903",
            value: "val_0198f1c3-9a00-7c3e-b1f3-773c28369904",
        },
        audit(TENANT_ID, PRINCIPAL_ID, "9905", '4', '5'),
        bindings.clone(),
        Utc::now() + Duration::minutes(5),
    );
    fail_fast_admission.entry_plan_node_key =
        PlanNodeKey::new("fail_fast_map".to_owned()).unwrap();
    fail_fast_admission.entry_node_kind = PlanNodeKind::Map;
    applied(
        run_command!(
            map_repository,
            admit_run,
            fail_fast_admission.clone()
        )
        .unwrap(),
    );
    let mut scheduler = map_repository.begin_scheduler_transaction().await.unwrap();
    let fail_fast_root_claimed = scheduler
        .claim_orchestration_jobs(orchestration_claim_with_ids(
            WORKER_D_ID,
            '0',
            "9906",
            ["9907", "9908", "9909", "990a"],
            ["990b", "990c", "990d"],
        ))
        .await
        .unwrap();
    scheduler.commit().await.unwrap();
    assert_eq!(fail_fast_root_claimed.len(), 1);
    let fail_fast_root_start = start_claimed_orchestration(
        &fail_fast_root_claimed[0],
        WORKER_D_ID,
        '0',
        "990e",
        "990f",
        "9910",
        '1',
        '2',
    );
    let mut scheduler = map_repository.begin_scheduler_transaction().await.unwrap();
    let fail_fast_root_started = match scheduler
        .start_orchestration_job(fail_fast_root_start.clone())
        .await
        .unwrap()
    {
        CommandOutcome::Applied(record) => record,
        CommandOutcome::Replayed(_) => panic!("FailFast Map root start must apply"),
    };
    scheduler.commit().await.unwrap();
    let open_fail_fast_map = ApplyOrchestrationControllerStep {
        fence: JobFence {
            expected_job_version: fail_fast_root_started.version,
            ..fail_fast_root_start.fence.clone()
        },
        plan: runtime_plan(),
        observation: ControllerObservation::Map { item_count: 3 },
        idempotency_key_digest: digest('3'),
        request_digest: digest('4'),
        receipt_expires_at: Utc::now() + Duration::hours(1),
        mutations: ControllerStepMutationIds {
            receipt_id: fixture_platform_id("rcp", "9911"),
            quota_entry_ids: ["9912", "9913", "9914", "9915"]
                .map(|suffix| fixture_platform_id("qle", suffix))
                .to_vec(),
            run_event_id: fixture_platform_id("evt", "9916"),
            run_outbox_id: fixture_platform_id("obx", "9916"),
            node_event_id: fixture_platform_id("evt", "9917"),
            node_outbox_id: fixture_platform_id("obx", "9917"),
            job_event_id: fixture_platform_id("evt", "9918"),
            job_outbox_id: fixture_platform_id("obx", "9918"),
            activations: vec![
                ControllerActivationSlot {
                    node_execution_id: fixture_platform_id("nod", "9919"),
                    orchestration_job_id: fixture_platform_id("job", "991a"),
                    scope: Some(ControllerScopeSlot {
                        scope_instance_id: fixture_platform_id("scp", "991b"),
                        scope_event_id: fixture_platform_id("evt", "991c"),
                        scope_outbox_id: fixture_platform_id("obx", "991c"),
                    }),
                    node_event_id: fixture_platform_id("evt", "991d"),
                    node_outbox_id: fixture_platform_id("obx", "991d"),
                    job_event_id: fixture_platform_id("evt", "991e"),
                    job_outbox_id: fixture_platform_id("obx", "991e"),
                },
                ControllerActivationSlot {
                    node_execution_id: fixture_platform_id("nod", "991f"),
                    orchestration_job_id: fixture_platform_id("job", "9920"),
                    scope: Some(ControllerScopeSlot {
                        scope_instance_id: fixture_platform_id("scp", "9921"),
                        scope_event_id: fixture_platform_id("evt", "9922"),
                        scope_outbox_id: fixture_platform_id("obx", "9922"),
                    }),
                    node_event_id: fixture_platform_id("evt", "9923"),
                    node_outbox_id: fixture_platform_id("obx", "9923"),
                    job_event_id: fixture_platform_id("evt", "9924"),
                    job_outbox_id: fixture_platform_id("obx", "9924"),
                },
            ],
            pending_nodes: vec![ControllerPendingNodeSlot {
                node_execution_id: fixture_platform_id("nod", "9925"),
                node_event_id: fixture_platform_id("evt", "9926"),
                node_outbox_id: fixture_platform_id("obx", "9926"),
            }],
            structural_exit: None,
            pending_wake: None,
            remainder_cancellations: Vec::new(),
        },
    };
    let mut scheduler = map_repository.begin_scheduler_transaction().await.unwrap();
    let opened_fail_fast = match scheduler
        .apply_orchestration_controller_step(open_fail_fast_map)
        .await
        .unwrap()
    {
        CommandOutcome::Applied(record) => record,
        CommandOutcome::Replayed(_) => panic!("FailFast Map admission must apply"),
    };
    scheduler.commit().await.unwrap();
    assert_eq!(opened_fail_fast.activations.len(), 2);
    assert_eq!(opened_fail_fast.created_scopes.len(), 2);
    assert_eq!(opened_fail_fast.pending_nodes.len(), 1);
    assert!(opened_fail_fast.woken_nodes.is_empty());
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            r#"
            SELECT count(*) FROM insight_platform.jobs
            WHERE tenant_id = $1 AND run_id = $2 AND node_id = $3
            "#,
        )
        .bind(TENANT_ID)
        .bind(fail_fast_admission.run_id.to_string())
        .bind(&opened_fail_fast.pending_nodes[0].node_id)
        .fetch_one(&pool)
        .await
        .unwrap(),
        0,
        "the next FailFast batch must remain behind a durable admission barrier",
    );

    let mut scheduler = map_repository.begin_scheduler_transaction().await.unwrap();
    let fail_fast_item_a = scheduler
        .claim_orchestration_jobs(orchestration_claim_with_ids(
            WORKER_D_ID,
            '5',
            "9930",
            ["9931", "9932", "9933", "9934"],
            ["9935", "9936", "9937"],
        ))
        .await
        .unwrap();
    scheduler.commit().await.unwrap();
    let mut scheduler = map_repository.begin_scheduler_transaction().await.unwrap();
    let fail_fast_item_b = scheduler
        .claim_orchestration_jobs(orchestration_claim_with_ids(
            WORKER_C_ID,
            '6',
            "9940",
            ["9941", "9942", "9943", "9944"],
            ["9945", "9946", "9947"],
        ))
        .await
        .unwrap();
    scheduler.commit().await.unwrap();
    assert_eq!(fail_fast_item_a.len(), 1);
    assert_eq!(fail_fast_item_b.len(), 1);
    let fail_fast_item_a_start = start_claimed_orchestration(
        &fail_fast_item_a[0],
        WORKER_D_ID,
        '5',
        "9938",
        "9939",
        "993a",
        '7',
        '8',
    );
    let fail_fast_item_b_start = start_claimed_orchestration(
        &fail_fast_item_b[0],
        WORKER_C_ID,
        '6',
        "9948",
        "9949",
        "994a",
        '9',
        'a',
    );
    let mut scheduler = map_repository.begin_scheduler_transaction().await.unwrap();
    let fail_fast_item_a_started = match scheduler
        .start_orchestration_job(fail_fast_item_a_start.clone())
        .await
        .unwrap()
    {
        CommandOutcome::Applied(record) => record,
        CommandOutcome::Replayed(_) => panic!("FailFast first item start must apply"),
    };
    scheduler.commit().await.unwrap();
    let mut scheduler = map_repository.begin_scheduler_transaction().await.unwrap();
    let fail_fast_item_b_started = match scheduler
        .start_orchestration_job(fail_fast_item_b_start.clone())
        .await
        .unwrap()
    {
        CommandOutcome::Applied(record) => record,
        CommandOutcome::Replayed(_) => panic!("FailFast second item start must apply"),
    };
    scheduler.commit().await.unwrap();

    let (
        failed_item_start,
        failed_item_started,
        cancelled_item_scope_id,
        cancelled_item_node_id,
        cancelled_item_job_id,
    ) = if fail_fast_item_a[0].job.node_id.as_deref()
        == Some(opened_fail_fast.activations[0].node_id.as_str())
    {
        (
            fail_fast_item_a_start,
            fail_fast_item_a_started,
            opened_fail_fast.created_scopes[1].scope_id.clone(),
            opened_fail_fast.activations[1].node_id.clone(),
            fail_fast_item_b[0].job.job_id.clone(),
        )
    } else {
        (
            fail_fast_item_b_start,
            fail_fast_item_b_started,
            opened_fail_fast.created_scopes[1].scope_id.clone(),
            opened_fail_fast.activations[1].node_id.clone(),
            fail_fast_item_a[0].job.job_id.clone(),
        )
    };
    let fail_fast_item = FailOrchestrationJob {
        fence: JobFence {
            expected_job_version: failed_item_started.version,
            ..failed_item_start.fence.clone()
        },
        plan: runtime_plan(),
        cause: OrchestrationFailureCause::Committed {
            failure: committed_failure.clone(),
        },
        derived_expression: None,
        idempotency_key_digest: digest('b'),
        request_digest: digest('c'),
        receipt_expires_at: Utc::now() + Duration::hours(1),
        mutations: ControllerStepMutationIds {
            receipt_id: fixture_platform_id("rcp", "9950"),
            quota_entry_ids: ["9951", "9952", "9953", "9954"]
                .map(|suffix| fixture_platform_id("qle", suffix))
                .to_vec(),
            run_event_id: fixture_platform_id("evt", "9955"),
            run_outbox_id: fixture_platform_id("obx", "9955"),
            node_event_id: fixture_platform_id("evt", "9956"),
            node_outbox_id: fixture_platform_id("obx", "9956"),
            job_event_id: fixture_platform_id("evt", "9957"),
            job_outbox_id: fixture_platform_id("obx", "9957"),
            activations: Vec::new(),
            pending_nodes: Vec::new(),
            structural_exit: Some(ControllerStructuralExitSlot {
                scope_closing_event_id: fixture_platform_id("evt", "9958"),
                scope_closing_outbox_id: fixture_platform_id("obx", "9958"),
                scope_terminal_event_id: fixture_platform_id("evt", "9959"),
                scope_terminal_outbox_id: fixture_platform_id("obx", "9959"),
                loop_rollover: None,
            }),
            pending_wake: Some(ControllerPendingWakeSlot {
                orchestration_job_id: fixture_platform_id("job", "995a"),
                request_digest: digest('d'),
                node_event_id: fixture_platform_id("evt", "995b"),
                node_outbox_id: fixture_platform_id("obx", "995b"),
                job_event_id: fixture_platform_id("evt", "995c"),
                job_outbox_id: fixture_platform_id("obx", "995c"),
            }),
            remainder_cancellations: vec![remainder_cancellation_slot(
                id(&cancelled_item_scope_id),
                ["995d", "995e", "995f", "9960"],
                ["9961", "9962", "9963", "9964", "9965"],
            )],
        },
    };
    let mut scheduler = map_repository.begin_scheduler_transaction().await.unwrap();
    let fail_fast_result = match scheduler
        .fail_orchestration_job(fail_fast_item.clone())
        .await
        .unwrap()
    {
        CommandOutcome::Applied(record) => record,
        CommandOutcome::Replayed(_) => panic!("FailFast item failure must apply"),
    };
    scheduler.commit().await.unwrap();
    assert_eq!(fail_fast_result.settled_scopes[0].state, "failed");
    assert_eq!(fail_fast_result.cancelled_remainders.len(), 1);
    assert_eq!(
        fail_fast_result.cancelled_remainders[0].reason_code,
        "map_failure_sibling_cancelled"
    );
    assert_eq!(
        fail_fast_result.cancelled_remainders[0].scope.scope_id,
        cancelled_item_scope_id
    );
    assert_eq!(
        fail_fast_result.cancelled_remainders[0].node_id,
        cancelled_item_node_id
    );
    assert_eq!(
        fail_fast_result.cancelled_remainders[0].job.job_id,
        cancelled_item_job_id
    );
    assert_eq!(fail_fast_result.woken_nodes.len(), 1);
    assert_eq!(
        fail_fast_result.woken_nodes[0].plan_node_key.as_str(),
        "fail_fast_map"
    );
    assert_eq!(fail_fast_result.run.state, "running");
    assert_eq!(fail_fast_result.run.active_work_count, 0);
    let fail_fast_scope_indexes = sqlx::query_scalar::<_, i32>(
        r#"
        SELECT (payload #>> '{descriptor,item_index}')::integer
        FROM insight_platform.run_nodes
        WHERE tenant_id = $1 AND run_id = $2 AND node_kind = 'map_item'
        ORDER BY (payload #>> '{descriptor,item_index}')::integer
        "#,
    )
    .bind(TENANT_ID)
    .bind(fail_fast_admission.run_id.to_string())
    .fetch_all(&pool)
    .await
    .unwrap();
    assert_eq!(
        fail_fast_scope_indexes,
        vec![0, 1],
        "FailFast must not admit the third item after the first batch fails",
    );
    let fail_fast_barrier = sqlx::query_as::<_, (String, i32)>(
        r#"
        SELECT payload #>> '{wait,kind}',
               (payload #>> '{wait,admitted_item_count}')::integer
        FROM insight_platform.run_nodes
        WHERE tenant_id = $1 AND run_id = $2 AND node_id = $3
          AND record_kind = 'node_execution'
        "#,
    )
    .bind(TENANT_ID)
    .bind(fail_fast_admission.run_id.to_string())
    .bind(&opened_fail_fast.pending_nodes[0].node_id)
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(fail_fast_barrier, ("map_settlement".to_owned(), 2));
    let mut scheduler = map_repository.begin_scheduler_transaction().await.unwrap();
    assert!(matches!(
        scheduler
            .fail_orchestration_job(fail_fast_item)
            .await
            .unwrap(),
        CommandOutcome::Replayed(record)
            if record.cancelled_remainders.len() == 1
                && record.cancelled_remainders[0].reason_code
                    == "map_failure_sibling_cancelled"
                && record.woken_nodes.len() == 1
    ));
    scheduler.commit().await.unwrap();

    let mut scheduler = map_repository.begin_scheduler_transaction().await.unwrap();
    let fail_fast_settlement_claimed = scheduler
        .claim_orchestration_jobs(orchestration_claim_with_ids(
            WORKER_D_ID,
            'e',
            "9970",
            ["9971", "9972", "9973", "9974"],
            ["9975", "9976", "9977"],
        ))
        .await
        .unwrap();
    scheduler.commit().await.unwrap();
    assert_eq!(fail_fast_settlement_claimed.len(), 1);
    assert_eq!(
        fail_fast_settlement_claimed[0].job.job_id,
        fail_fast_result.woken_nodes[0].job.job_id
    );
    let fail_fast_settlement_start = start_claimed_orchestration(
        &fail_fast_settlement_claimed[0],
        WORKER_D_ID,
        'e',
        "9978",
        "9979",
        "997a",
        'f',
        '0',
    );
    let mut scheduler = map_repository.begin_scheduler_transaction().await.unwrap();
    let fail_fast_settlement_started = match scheduler
        .start_orchestration_job(fail_fast_settlement_start.clone())
        .await
        .unwrap()
    {
        CommandOutcome::Applied(record) => record,
        CommandOutcome::Replayed(_) => panic!("FailFast settlement start must apply"),
    };
    scheduler.commit().await.unwrap();
    let fail_fast_settlement = FailOrchestrationJob {
        fence: JobFence {
            expected_job_version: fail_fast_settlement_started.version,
            ..fail_fast_settlement_start.fence.clone()
        },
        plan: runtime_plan(),
        cause: OrchestrationFailureCause::Controller {
            observation: ControllerObservation::MapSettlement {
                children: vec![ChildOutcome::Failed, ChildOutcome::Cancelled],
            },
        },
        derived_expression: None,
        idempotency_key_digest: digest('1'),
        request_digest: digest('2'),
        receipt_expires_at: Utc::now() + Duration::hours(1),
        mutations: ControllerStepMutationIds {
            receipt_id: fixture_platform_id("rcp", "9980"),
            quota_entry_ids: ["9981", "9982", "9983", "9984"]
                .map(|suffix| fixture_platform_id("qle", suffix))
                .to_vec(),
            run_event_id: fixture_platform_id("evt", "9985"),
            run_outbox_id: fixture_platform_id("obx", "9985"),
            node_event_id: fixture_platform_id("evt", "9986"),
            node_outbox_id: fixture_platform_id("obx", "9986"),
            job_event_id: fixture_platform_id("evt", "9987"),
            job_outbox_id: fixture_platform_id("obx", "9987"),
            activations: Vec::new(),
            pending_nodes: Vec::new(),
            structural_exit: Some(ControllerStructuralExitSlot {
                scope_closing_event_id: fixture_platform_id("evt", "9988"),
                scope_closing_outbox_id: fixture_platform_id("obx", "9988"),
                scope_terminal_event_id: fixture_platform_id("evt", "9989"),
                scope_terminal_outbox_id: fixture_platform_id("obx", "9989"),
                loop_rollover: None,
            }),
            pending_wake: None,
            remainder_cancellations: Vec::new(),
        },
    };
    let mut scheduler = map_repository.begin_scheduler_transaction().await.unwrap();
    let fail_fast_terminal = match scheduler
        .fail_orchestration_job(fail_fast_settlement.clone())
        .await
        .unwrap()
    {
        CommandOutcome::Applied(record) => record,
        CommandOutcome::Replayed(_) => panic!("FailFast Map terminal failure must apply"),
    };
    scheduler.commit().await.unwrap();
    assert_eq!(fail_fast_terminal.run.state, "failed");
    assert_eq!(
        fail_fast_terminal.controller_code.as_deref(),
        Some("map_item_failed")
    );
    assert_eq!(fail_fast_terminal.run.active_work_count, 0);
    let mut scheduler = map_repository.begin_scheduler_transaction().await.unwrap();
    assert!(matches!(
        scheduler
            .fail_orchestration_job(fail_fast_settlement)
            .await
            .unwrap(),
        CommandOutcome::Replayed(record)
            if record.run.state == "failed"
                && record.controller_code.as_deref() == Some("map_item_failed")
    ));
    scheduler.commit().await.unwrap();
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT reserved_value FROM insight_platform.quota_accounts WHERE tenant_id = $1 AND quota_account_id = $2",
        )
        .bind(TENANT_ID)
        .bind(QUOTA_ACCOUNT_ID)
        .fetch_one(&pool)
        .await
        .unwrap(),
        0
    );
            }};
        }
        async fn run_task_first_winner_fixture(
            repository: PgRepository,
            pool: PgPool,
            bindings: RunBindingsSnapshot,
        ) {
            task_first_winner_fixture!(repository, pool, bindings);
        }
        Box::pin(run_task_first_winner_fixture(
            repository.clone(),
            pool.clone(),
            bindings.clone(),
        ))
        .await;
        })
        .await
        .unwrap();
    });
}

struct AdmissionIds<'a> {
    run: &'a str,
    scope: &'a str,
    node: &'a str,
    job: &'a str,
    value: &'a str,
}

fn admission(
    ids: AdmissionIds<'_>,
    mut audit: CommandAudit,
    bindings: RunBindingsSnapshot,
    deadline: chrono::DateTime<Utc>,
) -> AdmitRun {
    let input: Value = json!({"question": "select one"});
    let content_digest = canonical_digest(&input).unwrap().parse().unwrap();
    audit.idempotency_key_digest = canonical_digest(&json!(audit.receipt_id.to_string()))
        .unwrap()
        .parse()
        .unwrap();
    AdmitRun {
        audit,
        admission_scope_id: id(AGENT_ID),
        run_id: id(ids.run),
        agent_deployment_id: id(AGENT_DEPLOYMENT_ID),
        root_scope_id: id(ids.scope),
        entry_node_execution_id: id(ids.node),
        orchestration_job_id: id(ids.job),
        entry_plan_node_key: PlanNodeKey::new("entry".to_owned()).unwrap(),
        entry_node_kind: PlanNodeKind::Start,
        bindings,
        input: RunInputValue {
            value_id: id(ids.value),
            classification: DataClassification::Internal,
            schema_digest: agent_schema().canonical_digest,
            content_digest,
            value: ValueRef::Inline { value: input },
        },
        deadline,
        inline_limits: JsonLimits::CONTRACT_FIXTURE,
        attempt_limit: 3,
        retry_backoff_milliseconds: 100,
    }
}

fn orchestration_claim() -> ClaimOrchestrationJobs {
    orchestration_claim_with_ids(
        WORKER_ID,
        '9',
        "7020",
        ["7021", "7022", "7023", "7024"],
        ["7025", "7026", "7027"],
    )
}

fn orchestration_claim_with_ids(
    worker_id: &str,
    token: char,
    reservation_suffix: &str,
    quota_suffixes: [&str; 4],
    event_suffixes: [&str; 3],
) -> ClaimOrchestrationJobs {
    let platform_id = |prefix: &str, suffix: &str| {
        id(&format!(
            "{prefix}_0198f1c3-9a00-7c3e-b1f3-773c2836{suffix}"
        ))
    };
    ClaimOrchestrationJobs {
        worker_id: id(worker_id),
        limit: 1,
        lease_milliseconds: 30_000,
        slots: vec![OrchestrationClaimSlot {
            lease_token_digest: digest(token),
            quota_reservation_id: platform_id("ures", reservation_suffix),
            quota_entry_ids: quota_suffixes
                .map(|suffix| platform_id("qle", suffix))
                .to_vec(),
            run_event_id: platform_id("evt", event_suffixes[0]),
            run_outbox_id: platform_id("obx", event_suffixes[0]),
            node_event_id: platform_id("evt", event_suffixes[1]),
            node_outbox_id: platform_id("obx", event_suffixes[1]),
            job_event_id: platform_id("evt", event_suffixes[2]),
            job_outbox_id: platform_id("obx", event_suffixes[2]),
        }],
    }
}

fn fixture_platform_id(prefix: &str, suffix: &str) -> ResourceId {
    id(&format!(
        "{prefix}_0198f1c3-9a00-7c3e-b1f3-773c2836{suffix}"
    ))
}

#[allow(clippy::too_many_arguments)]
fn start_claimed_orchestration(
    claimed: &ClaimedOrchestrationJob,
    worker_id: &str,
    lease_token: char,
    receipt_suffix: &str,
    job_event_suffix: &str,
    node_event_suffix: &str,
    idempotency: char,
    request: char,
) -> StartOrchestrationJob {
    StartOrchestrationJob {
        fence: JobFence {
            tenant_id: TENANT_ID.to_owned(),
            job_id: claimed.job.job_id.clone(),
            worker_id: id(worker_id),
            lease_epoch: claimed.job.lease_epoch,
            expected_job_version: claimed.job.version,
            lease_token_digest: digest(lease_token),
        },
        receipt_id: fixture_platform_id("rcp", receipt_suffix),
        idempotency_key_digest: digest(idempotency),
        request_digest: digest(request),
        receipt_expires_at: Utc::now() + Duration::hours(1),
        job_event_id: fixture_platform_id("evt", job_event_suffix),
        job_outbox_id: fixture_platform_id("obx", job_event_suffix),
        node_event_id: fixture_platform_id("evt", node_event_suffix),
        node_outbox_id: fixture_platform_id("obx", node_event_suffix),
    }
}

#[allow(clippy::too_many_arguments)]
fn parallel_leg_exit_step(
    start: &StartOrchestrationJob,
    started_version: i64,
    receipt_suffix: &str,
    quota_suffixes: [&str; 4],
    source_event_suffixes: [&str; 3],
    scope_event_suffixes: [&str; 2],
    wake_job_suffix: &str,
    wake_event_suffixes: [&str; 2],
    idempotency: char,
    request: char,
    wake_request: char,
) -> ApplyOrchestrationControllerStep {
    ApplyOrchestrationControllerStep {
        fence: JobFence {
            expected_job_version: started_version,
            ..start.fence.clone()
        },
        plan: runtime_plan(),
        observation: ControllerObservation::None,
        idempotency_key_digest: digest(idempotency),
        request_digest: digest(request),
        receipt_expires_at: Utc::now() + Duration::hours(1),
        mutations: ControllerStepMutationIds {
            receipt_id: fixture_platform_id("rcp", receipt_suffix),
            quota_entry_ids: quota_suffixes
                .map(|suffix| fixture_platform_id("qle", suffix))
                .to_vec(),
            run_event_id: fixture_platform_id("evt", source_event_suffixes[0]),
            run_outbox_id: fixture_platform_id("obx", source_event_suffixes[0]),
            node_event_id: fixture_platform_id("evt", source_event_suffixes[1]),
            node_outbox_id: fixture_platform_id("obx", source_event_suffixes[1]),
            job_event_id: fixture_platform_id("evt", source_event_suffixes[2]),
            job_outbox_id: fixture_platform_id("obx", source_event_suffixes[2]),
            activations: Vec::new(),
            pending_nodes: Vec::new(),
            structural_exit: Some(ControllerStructuralExitSlot {
                scope_closing_event_id: fixture_platform_id("evt", scope_event_suffixes[0]),
                scope_closing_outbox_id: fixture_platform_id("obx", scope_event_suffixes[0]),
                scope_terminal_event_id: fixture_platform_id("evt", scope_event_suffixes[1]),
                scope_terminal_outbox_id: fixture_platform_id("obx", scope_event_suffixes[1]),
                loop_rollover: None,
            }),
            pending_wake: Some(ControllerPendingWakeSlot {
                orchestration_job_id: fixture_platform_id("job", wake_job_suffix),
                request_digest: digest(wake_request),
                node_event_id: fixture_platform_id("evt", wake_event_suffixes[0]),
                node_outbox_id: fixture_platform_id("obx", wake_event_suffixes[0]),
                job_event_id: fixture_platform_id("evt", wake_event_suffixes[1]),
                job_outbox_id: fixture_platform_id("obx", wake_event_suffixes[1]),
            }),
            remainder_cancellations: Vec::new(),
        },
    }
}

fn remainder_cancellation_slot(
    expected_scope_id: ResourceId,
    quota_suffixes: [&str; 4],
    event_suffixes: [&str; 5],
) -> ControllerRemainderCancellationSlot {
    ControllerRemainderCancellationSlot {
        expected_scope_id,
        quota_entry_ids: quota_suffixes
            .map(|suffix| fixture_platform_id("qle", suffix))
            .to_vec(),
        node_cancelling_event_id: fixture_platform_id("evt", event_suffixes[0]),
        node_cancelling_outbox_id: fixture_platform_id("obx", event_suffixes[0]),
        node_terminal_event_id: fixture_platform_id("evt", event_suffixes[1]),
        node_terminal_outbox_id: fixture_platform_id("obx", event_suffixes[1]),
        scope_closing_event_id: fixture_platform_id("evt", event_suffixes[2]),
        scope_closing_outbox_id: fixture_platform_id("obx", event_suffixes[2]),
        scope_terminal_event_id: fixture_platform_id("evt", event_suffixes[3]),
        scope_terminal_outbox_id: fixture_platform_id("obx", event_suffixes[3]),
        job_terminal_event_id: fixture_platform_id("evt", event_suffixes[4]),
        job_terminal_outbox_id: fixture_platform_id("obx", event_suffixes[4]),
    }
}

#[allow(clippy::too_many_arguments)]
fn fork_controller_step(
    start: &StartOrchestrationJob,
    started_version: i64,
    receipt_suffix: &str,
    quota_suffixes: [&str; 4],
    source_event_suffixes: [&str; 3],
    leg_suffixes: [[&str; 6]; 2],
    pending_suffixes: [&str; 2],
    idempotency: char,
    request: char,
) -> ApplyOrchestrationControllerStep {
    ApplyOrchestrationControllerStep {
        fence: JobFence {
            expected_job_version: started_version,
            ..start.fence.clone()
        },
        plan: runtime_plan(),
        observation: ControllerObservation::None,
        idempotency_key_digest: digest(idempotency),
        request_digest: digest(request),
        receipt_expires_at: Utc::now() + Duration::hours(1),
        mutations: ControllerStepMutationIds {
            receipt_id: fixture_platform_id("rcp", receipt_suffix),
            quota_entry_ids: quota_suffixes
                .map(|suffix| fixture_platform_id("qle", suffix))
                .to_vec(),
            run_event_id: fixture_platform_id("evt", source_event_suffixes[0]),
            run_outbox_id: fixture_platform_id("obx", source_event_suffixes[0]),
            node_event_id: fixture_platform_id("evt", source_event_suffixes[1]),
            node_outbox_id: fixture_platform_id("obx", source_event_suffixes[1]),
            job_event_id: fixture_platform_id("evt", source_event_suffixes[2]),
            job_outbox_id: fixture_platform_id("obx", source_event_suffixes[2]),
            activations: leg_suffixes
                .into_iter()
                .map(|suffixes| ControllerActivationSlot {
                    node_execution_id: fixture_platform_id("nod", suffixes[0]),
                    orchestration_job_id: fixture_platform_id("job", suffixes[1]),
                    scope: Some(ControllerScopeSlot {
                        scope_instance_id: fixture_platform_id("scp", suffixes[2]),
                        scope_event_id: fixture_platform_id("evt", suffixes[5]),
                        scope_outbox_id: fixture_platform_id("obx", suffixes[5]),
                    }),
                    node_event_id: fixture_platform_id("evt", suffixes[3]),
                    node_outbox_id: fixture_platform_id("obx", suffixes[3]),
                    job_event_id: fixture_platform_id("evt", suffixes[4]),
                    job_outbox_id: fixture_platform_id("obx", suffixes[4]),
                })
                .collect(),
            pending_nodes: vec![ControllerPendingNodeSlot {
                node_execution_id: fixture_platform_id("nod", pending_suffixes[0]),
                node_event_id: fixture_platform_id("evt", pending_suffixes[1]),
                node_outbox_id: fixture_platform_id("obx", pending_suffixes[1]),
            }],
            structural_exit: None,
            pending_wake: None,
            remainder_cancellations: Vec::new(),
        },
    }
}

fn orchestration_convergence(
    quota_suffixes: [&str; 4],
    event_suffixes: [&str; 6],
) -> DriveOrchestrationConvergence {
    let platform_id = |prefix: &str, suffix: &str| {
        id(&format!(
            "{prefix}_0198f1c3-9a00-7c3e-b1f3-773c2836{suffix}"
        ))
    };
    DriveOrchestrationConvergence {
        shard: SafetyScanShard::whole(),
        after: None,
        limit: 1,
        slots: vec![OrchestrationConvergenceSlot {
            quota_entry_ids: quota_suffixes
                .map(|suffix| platform_id("qle", suffix))
                .to_vec(),
            run_event_id: platform_id("evt", event_suffixes[0]),
            run_outbox_id: platform_id("obx", event_suffixes[0]),
            node_event_id: platform_id("evt", event_suffixes[1]),
            node_outbox_id: platform_id("obx", event_suffixes[1]),
            node_cancelling_event_id: platform_id("evt", event_suffixes[5]),
            node_cancelling_outbox_id: platform_id("obx", event_suffixes[5]),
            scope_closing_event_id: platform_id("evt", event_suffixes[2]),
            scope_closing_outbox_id: platform_id("obx", event_suffixes[2]),
            scope_terminal_event_id: platform_id("evt", event_suffixes[3]),
            scope_terminal_outbox_id: platform_id("obx", event_suffixes[3]),
            job_event_id: platform_id("evt", event_suffixes[4]),
            job_outbox_id: platform_id("obx", event_suffixes[4]),
        }],
    }
}

fn expired_task_scan(
    resume_suffix: &str,
    request_digest: char,
    event_suffixes: [&str; 4],
) -> DriveExpiredOrchestrationTasks {
    let platform_id = |prefix: &str, suffix: &str| {
        id(&format!(
            "{prefix}_0198f1c3-9a00-7c3e-b1f3-773c2836{suffix}"
        ))
    };
    DriveExpiredOrchestrationTasks {
        shard: SafetyScanShard::whole(),
        after: None,
        limit: 1,
        slots: vec![ExpiredOrchestrationTaskSlot {
            resume_job_id: platform_id("job", resume_suffix),
            resume_request_digest: digest(request_digest),
            task_event_id: platform_id("evt", event_suffixes[0]),
            task_outbox_id: platform_id("obx", event_suffixes[0]),
            run_event_id: platform_id("evt", event_suffixes[1]),
            run_outbox_id: platform_id("obx", event_suffixes[1]),
            node_event_id: platform_id("evt", event_suffixes[2]),
            node_outbox_id: platform_id("obx", event_suffixes[2]),
            job_event_id: platform_id("evt", event_suffixes[3]),
            job_outbox_id: platform_id("obx", event_suffixes[3]),
        }],
    }
}

async fn claim_with_serializable_retry(
    repository: &PgRepository,
    command: ClaimOrchestrationJobs,
) -> Result<Vec<ClaimedOrchestrationJob>, RepositoryError> {
    for retry in 0..4 {
        let mut transaction = repository.begin_scheduler_transaction().await?;
        match transaction.claim_orchestration_jobs(command.clone()).await {
            Ok(claimed) => match transaction.commit().await {
                Ok(()) => return Ok(claimed),
                Err(failure) if is_serialization_failure(&failure) && retry < 3 => continue,
                Err(failure) => return Err(failure),
            },
            Err(failure) if is_serialization_failure(&failure) && retry < 3 => {
                transaction.rollback().await?;
            }
            Err(failure) => {
                transaction.rollback().await?;
                return Err(failure);
            }
        }
    }
    unreachable!("bounded retry loop either returns or attempts the fourth transaction")
}

fn is_serialization_failure(failure: &RepositoryError) -> bool {
    matches!(
        failure,
        RepositoryError::Database(sqlx::Error::Database(database))
            if database.code().as_deref() == Some("40001")
    )
}

async fn seed_authorities(repository: &PgRepository, pool: &PgPool) {
    for tenant_id in [TENANT_ID, TENANT_B_ID] {
        repository
            .create_tenant(NewTenant {
                tenant_id: tenant_id.to_owned(),
                state: "active".to_owned(),
                config: TenantConfig::default(),
            })
            .await
            .unwrap();
    }
    for (principal_id, authority, subject) in
        [(PRINCIPAL_ID, '1', '2'), (DENIED_PRINCIPAL_ID, '3', '4')]
    {
        repository
            .create_principal(NewPrincipal {
                principal_id: id(principal_id),
                authentication_authority_digest: digest(authority),
                subject_digest: digest(subject),
                installation_bindings: PrincipalBindingsPayload {
                    installation_bindings: vec![],
                },
            })
            .await
            .unwrap();
    }
    for tenant_id in [TENANT_ID, TENANT_B_ID] {
        repository
            .bind_tenant_principal(NewTenantPrincipal {
                tenant_id: id(tenant_id),
                principal_id: id(PRINCIPAL_ID),
                principal_kind: PrincipalKind::AgentRunner,
                payload: TenantPrincipalPayload {
                    permissions: PermissionSet::new(vec![
                        Permission::AgentRun,
                        Permission::InteractionRespond,
                        Permission::RuntimeControl,
                        Permission::TenantManage,
                    ])
                    .unwrap(),
                },
            })
            .await
            .unwrap();
    }
    repository
        .bind_tenant_principal(NewTenantPrincipal {
            tenant_id: id(TENANT_ID),
            principal_id: id(DENIED_PRINCIPAL_ID),
            principal_kind: PrincipalKind::AgentRunner,
            payload: TenantPrincipalPayload {
                permissions: PermissionSet::new(vec![Permission::RuntimeRead]).unwrap(),
            },
        })
        .await
        .unwrap();
    let tenant_count: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM insight_platform.tenants WHERE tenant_id IN ($1, $2)",
    )
    .bind(TENANT_ID)
    .bind(TENANT_B_ID)
    .fetch_one(pool)
    .await
    .unwrap();
    assert_eq!(tenant_count, 2);
}

async fn seed_agent_registry(pool: &PgPool) -> (RunBindingsSnapshot, ExactDeploymentRef) {
    let plan = runtime_plan();
    let typed_plan_digest = plan.canonical_digest(plan_limits()).unwrap();
    let typed_plan_bytes = canonical_json(&serde_json::to_value(&plan).unwrap()).unwrap();
    let child_plan = child_runtime_plan();
    let child_typed_plan_digest = child_plan.canonical_digest(plan_limits()).unwrap();
    let resource_payload = TypedPayload::new(1, &json!({"fixture": "phase2-run"})).unwrap();
    let agent_plan_payload = TypedPayload::new(
        1,
        &PublishedVersionPayload {
            document: ResourceDocument::Agent(AgentResourceSpec {
                authoring_package: AuthoringPackage {
                    artifact: ArtifactRef::new(
                        id(AGENT_AUTHORING_ARTIFACT_ID),
                        digest('d'),
                        1,
                        "application/json",
                        DataClassification::Internal,
                        Some("agent-plan.json".to_owned()),
                    )
                    .unwrap(),
                    manifest_digest: digest('e'),
                },
                contract_digest: digest('f'),
                dependency_versions: vec![],
                policy_versions: vec![],
                input_schema: agent_schema(),
                output_schema: agent_schema(),
                error_schema: agent_error_schema(),
                typed_plan_artifact_id: id(TYPED_PLAN_ARTIFACT_ID),
                typed_plan_digest: typed_plan_digest.clone(),
            }),
            validation: ValidationSummary {
                validator_digest: digest('1'),
                validated_draft_digest: digest('2'),
                dependency_closure_digest: digest('3'),
                security_evidence_digest: digest('4'),
                warnings: vec![],
            },
        },
    )
    .unwrap();
    let child_interface_payload = TypedPayload::new(
        1,
        &PublishedVersionPayload {
            document: ResourceDocument::Agent(AgentResourceSpec {
                authoring_package: AuthoringPackage {
                    artifact: ArtifactRef::new(
                        id("art_0198f1c3-9a00-7c3e-b1f3-773c2836701c"),
                        digest('c'),
                        1,
                        "application/json",
                        DataClassification::Internal,
                        Some("child-agent-interface.json".to_owned()),
                    )
                    .unwrap(),
                    manifest_digest: digest('d'),
                },
                contract_digest: digest('e'),
                dependency_versions: vec![],
                policy_versions: vec![],
                input_schema: agent_schema(),
                output_schema: agent_schema(),
                error_schema: agent_error_schema(),
                typed_plan_artifact_id: id("art_0198f1c3-9a00-7c3e-b1f3-773c2836701c"),
                typed_plan_digest: child_typed_plan_digest,
            }),
            validation: ValidationSummary {
                validator_digest: digest('1'),
                validated_draft_digest: digest('2'),
                dependency_closure_digest: digest('3'),
                security_evidence_digest: digest('4'),
                warnings: vec![],
            },
        },
    )
    .unwrap();
    let scheduling = SchedulingPolicyDocument {
        version: 1,
        weight: 1,
        burst: 2,
        aging_rounds: 2,
    };
    let rules_digest = scheduling.canonical_digest().unwrap();
    let policy_payload = TypedPayload::new(
        1,
        &PublishedVersionPayload {
            document: ResourceDocument::Policy(PolicyResourceSpec {
                authoring_package: AuthoringPackage {
                    artifact: ArtifactRef::new(
                        id(POLICY_EVIDENCE_ARTIFACT_ID),
                        digest('4'),
                        1,
                        "application/json",
                        DataClassification::Internal,
                        Some("scheduling-policy.json".to_owned()),
                    )
                    .unwrap(),
                    manifest_digest: digest('3'),
                },
                contract_digest: digest('2'),
                dependency_versions: vec![],
                policy_versions: vec![],
                policy_kind: PolicyKind::Scheduling,
                rules_digest,
                selection: None,
                scheduling: Some(scheduling),
                retention: None,
                mcp_protocol: None,
                mcp_auth: None,
                sandbox_isolation: None,
                sandbox_resource: None,
                sandbox_network: None,
                sandbox_artifact_io: None,
                sandbox_secret_resolution: None,
            }),
            validation: ValidationSummary {
                validator_digest: digest('1'),
                validated_draft_digest: digest('0'),
                dependency_closure_digest: digest('a'),
                security_evidence_digest: digest('b'),
                warnings: vec![],
            },
        },
    )
    .unwrap();
    let selection_document = CandidateSelectionPolicyDocument {
        schema_version: 1,
        mode: CandidateSelectionMode::OnlyCandidate,
        route_schema_digest: None,
    };
    let selection_policy_payload = TypedPayload::new(
        1,
        &PublishedVersionPayload {
            document: ResourceDocument::Policy(PolicyResourceSpec {
                authoring_package: AuthoringPackage {
                    artifact: ArtifactRef::new(
                        id(POLICY_EVIDENCE_ARTIFACT_ID),
                        digest('4'),
                        1,
                        "application/json",
                        DataClassification::Internal,
                        Some("selection-policy.json".to_owned()),
                    )
                    .unwrap(),
                    manifest_digest: digest('3'),
                },
                contract_digest: digest('2'),
                dependency_versions: vec![],
                policy_versions: vec![],
                policy_kind: PolicyKind::Selection,
                rules_digest: canonical_digest(&serde_json::to_value(&selection_document).unwrap())
                    .unwrap()
                    .parse()
                    .unwrap(),
                selection: Some(selection_document.clone()),
                scheduling: None,
                retention: None,
                mcp_protocol: None,
                mcp_auth: None,
                sandbox_isolation: None,
                sandbox_resource: None,
                sandbox_network: None,
                sandbox_artifact_io: None,
                sandbox_secret_resolution: None,
            }),
            validation: ValidationSummary {
                validator_digest: digest('1'),
                validated_draft_digest: digest('0'),
                dependency_closure_digest: digest('a'),
                security_evidence_digest: digest('b'),
                warnings: vec![],
            },
        },
    )
    .unwrap();
    let skill_contents = vec![
        SkillPackageContent {
            path: "instructions/review.md".to_owned(),
            bytes: b"Review bounded output".to_vec(),
        },
        SkillPackageContent {
            path: "skill.json".to_owned(),
            bytes: b"{}".to_vec(),
        },
    ];
    let skill_entries = vec![
        SkillPackageEntry {
            path: "instructions/review.md".to_owned(),
            kind: SkillPackageEntryKind::Instruction,
            media_type: "text/markdown".to_owned(),
            byte_length: u64::try_from(skill_contents[0].bytes.len()).unwrap(),
            content_digest: digest_bytes(&skill_contents[0].bytes),
            data_classification: DataClassification::Internal,
            executable: false,
        },
        SkillPackageEntry {
            path: "skill.json".to_owned(),
            kind: SkillPackageEntryKind::Manifest,
            media_type: "application/json".to_owned(),
            byte_length: u64::try_from(skill_contents[1].bytes.len()).unwrap(),
            content_digest: digest_bytes(&skill_contents[1].bytes),
            data_classification: DataClassification::Internal,
            executable: false,
        },
    ];
    let skill_expanded_bytes = skill_entries
        .iter()
        .map(|entry| entry.byte_length)
        .sum::<u64>();
    let skill_manifest_digest: Sha256Digest = canonical_digest(&json!({
        "entries": skill_entries,
        "schema_version": 1,
        "total_byte_length": skill_expanded_bytes,
    }))
    .unwrap()
    .parse()
    .unwrap();
    let skill_sections = vec![SkillInstructionSection {
        section_id: "review".to_owned(),
        phase: SkillInstructionPhase::Validation,
        audience: SkillInstructionAudience::Validator,
        body: SkillArtifactSliceRef {
            path: "instructions/review.md".to_owned(),
            content_digest: skill_entries[0].content_digest.clone(),
            byte_offset: 7,
            byte_length: 7,
        },
        max_tokens: 8,
        data_classification: DataClassification::Internal,
    }];
    let skill_instruction_digest: Sha256Digest =
        canonical_digest(&serde_json::to_value(&skill_sections).unwrap())
            .unwrap()
            .parse()
            .unwrap();
    let skill_requirement_digest: Sha256Digest = canonical_digest(&json!({
        "capability": [],
        "context": [],
        "model": [],
        "skill_dependencies": [],
    }))
    .unwrap()
    .parse()
    .unwrap();
    let skill_manifest = SkillPackageManifest {
        schema_version: 1,
        entries: skill_entries,
        total_byte_length: skill_expanded_bytes,
        canonical_digest: skill_manifest_digest.clone(),
    };
    let skill_package_bytes =
        encode_canonical_skill_package(&skill_manifest, &skill_contents).unwrap();
    let skill_package_digest = digest_bytes(&skill_package_bytes);
    let skill_payload = TypedPayload::new(
        1,
        &PublishedVersionPayload {
            document: ResourceDocument::Skill(SkillResourceSpec {
                authoring_package: AuthoringPackage {
                    artifact: ArtifactRef::new(
                        id(SKILL_PACKAGE_ARTIFACT_ID),
                        skill_package_digest.clone(),
                        u64::try_from(skill_package_bytes.len()).unwrap(),
                        SKILL_PACKAGE_MEDIA_TYPE,
                        DataClassification::Internal,
                        Some("review.skill".to_owned()),
                    )
                    .unwrap(),
                    manifest_digest: skill_manifest_digest.clone(),
                },
                contract_digest: digest('c'),
                dependency_versions: vec![],
                policy_versions: vec![],
                interface: SkillInterface {
                    qualified_name: "review.method".to_owned(),
                    purpose: "Review one bounded result".to_owned(),
                    task_input_schema: agent_schema(),
                    produced_guidance_schema: agent_schema(),
                    compatible_agent_interfaces: vec![id(AGENT_INTERFACE_ID)],
                },
                manifest: skill_manifest,
                instruction_sections: skill_sections,
                skill_dependencies: vec![],
                capability_requirements: vec![],
                context_requirements: vec![],
                model_requirements: vec![],
                instruction_set_digest: skill_instruction_digest,
                requirement_set_digest: skill_requirement_digest,
            }),
            validation: ValidationSummary {
                validator_digest: digest('1'),
                validated_draft_digest: digest('2'),
                dependency_closure_digest: digest('3'),
                security_evidence_digest: digest('4'),
                warnings: vec![],
            },
        },
    )
    .unwrap();
    for (resource_id, resource_kind) in [
        (POLICY_ID, "policy"),
        (SELECTION_POLICY_ID, "policy"),
        (AGENT_ID, "agent"),
        (CHILD_AGENT_ID, "agent"),
        (SKILL_ID, "skill"),
    ] {
        sqlx::query(
            r#"
            INSERT INTO insight_platform.resources (
                tenant_id, resource_id, resource_kind, lifecycle_state, gate_state,
                payload_schema_version, payload, payload_digest
            ) VALUES ($1, $2, $3, 'active', 'enabled', $4, $5, $6)
            "#,
        )
        .bind(TENANT_ID)
        .bind(resource_id)
        .bind(resource_kind)
        .bind(resource_payload.schema_version)
        .bind(&resource_payload.value)
        .bind(&resource_payload.digest)
        .execute(pool)
        .await
        .unwrap();
    }
    let versions = [
        (
            POLICY_REVISION_ID,
            POLICY_ID,
            "policy_revision",
            digest('5'),
        ),
        (
            SELECTION_POLICY_REVISION_ID,
            SELECTION_POLICY_ID,
            "policy_revision",
            digest('7'),
        ),
        (
            AGENT_INTERFACE_ID,
            AGENT_ID,
            "agent_interface_revision",
            digest('6'),
        ),
        (
            AGENT_PLAN_ID,
            AGENT_ID,
            "agent_plan_revision",
            typed_plan_digest.clone(),
        ),
        (
            CHILD_AGENT_INTERFACE_ID,
            CHILD_AGENT_ID,
            "agent_interface_revision",
            digest('8'),
        ),
        (
            CHILD_AGENT_PLAN_ID,
            CHILD_AGENT_ID,
            "agent_plan_revision",
            digest('9'),
        ),
        (SKILL_REVISION_ID, SKILL_ID, "skill_revision", digest('d')),
    ];
    for (version_id, resource_id, kind, content_digest) in &versions {
        let version_payload = match *version_id {
            POLICY_REVISION_ID => &policy_payload,
            SELECTION_POLICY_REVISION_ID => &selection_policy_payload,
            AGENT_INTERFACE_ID | AGENT_PLAN_ID => &agent_plan_payload,
            CHILD_AGENT_INTERFACE_ID | CHILD_AGENT_PLAN_ID => &child_interface_payload,
            SKILL_REVISION_ID => &skill_payload,
            _ => &resource_payload,
        };
        sqlx::query(
            r#"
            INSERT INTO insight_platform.resource_versions (
                tenant_id, resource_version_id, resource_id, resource_version_kind,
                revision_no, content_digest, payload_schema_version, payload,
                payload_digest, created_by
            ) VALUES ($1, $2, $3, $4, 1, $5, $6, $7, $8, $9)
            "#,
        )
        .bind(TENANT_ID)
        .bind(version_id)
        .bind(resource_id)
        .bind(kind)
        .bind(content_digest.to_string())
        .bind(version_payload.schema_version)
        .bind(&version_payload.value)
        .bind(&version_payload.digest)
        .bind(PRINCIPAL_ID)
        .execute(pool)
        .await
        .unwrap();
    }
    let typed_plan_metadata =
        TypedPayload::new(1, &json!({"display_name": "typed-plan.json"})).unwrap();
    sqlx::query(
        r#"
        INSERT INTO insight_platform.artifact_blobs (
            tenant_id, blob_id, backend, storage_binding_digest,
            security_domain_digest, object_reference_ciphertext, object_generation, key_id,
            encryption_domain_id, content_digest, size_bytes, state, verified_at,
            created_at, updated_at
        ) VALUES ($1, $2, 'fixture', $3, $4, $5, 'typed-plan-generation-1',
                  'fixture-key', $6, $7, $8, 'verified', statement_timestamp(),
                  statement_timestamp(), statement_timestamp())
        "#,
    )
    .bind(TENANT_ID)
    .bind(TYPED_PLAN_BLOB_ID)
    .bind(digest('c').to_string())
    .bind(digest('d').to_string())
    .bind(vec![7_u8, 8, 9])
    .bind(TYPED_PLAN_ENCRYPTION_DOMAIN_ID)
    .bind(typed_plan_digest.to_string())
    .bind(i64::try_from(typed_plan_bytes.len()).unwrap())
    .execute(pool)
    .await
    .unwrap();
    sqlx::query(
        r#"
        INSERT INTO insight_platform.artifacts (
            tenant_id, artifact_id, blob_id, purpose, classification,
            expected_size_bytes, expected_digest, declared_media_type,
            verified_media_type, state, metadata_schema_version, metadata,
            metadata_digest, retention_policy_revision_id, retain_until, created_by
        ) VALUES ($1, $2, $3, 'typed_plan', 'internal', $4, $5,
                  'application/json', 'application/json', 'ready', $6, $7, $8,
                  $9, $10, $11)
        "#,
    )
    .bind(TENANT_ID)
    .bind(TYPED_PLAN_ARTIFACT_ID)
    .bind(TYPED_PLAN_BLOB_ID)
    .bind(i64::try_from(typed_plan_bytes.len()).unwrap())
    .bind(typed_plan_digest.to_string())
    .bind(typed_plan_metadata.schema_version)
    .bind(&typed_plan_metadata.value)
    .bind(&typed_plan_metadata.digest)
    .bind(POLICY_REVISION_ID)
    .bind(Utc::now() + Duration::days(30))
    .bind(PRINCIPAL_ID)
    .execute(pool)
    .await
    .unwrap();
    let skill_package_metadata =
        TypedPayload::new(1, &json!({"display_name": "review.skill"})).unwrap();
    sqlx::query(
        r#"
        INSERT INTO insight_platform.artifact_blobs (
            tenant_id, blob_id, backend, storage_binding_digest,
            security_domain_digest, object_reference_ciphertext, object_generation, key_id,
            encryption_domain_id, content_digest, size_bytes, state, verified_at,
            created_at, updated_at
        ) VALUES ($1, $2, 'fixture', $3, $4, $5, 'skill-package-generation-1',
                  'fixture-key', $6, $7, $8, 'verified', statement_timestamp(),
                  statement_timestamp(), statement_timestamp())
        "#,
    )
    .bind(TENANT_ID)
    .bind(SKILL_PACKAGE_BLOB_ID)
    .bind(digest('e').to_string())
    .bind(digest('f').to_string())
    .bind(vec![10_u8, 11, 12])
    .bind(SKILL_PACKAGE_ENCRYPTION_DOMAIN_ID)
    .bind(skill_package_digest.to_string())
    .bind(i64::try_from(skill_package_bytes.len()).unwrap())
    .execute(pool)
    .await
    .unwrap();
    sqlx::query(
        r#"
        INSERT INTO insight_platform.artifacts (
            tenant_id, artifact_id, blob_id, purpose, classification,
            expected_size_bytes, expected_digest, declared_media_type,
            verified_media_type, state, metadata_schema_version, metadata,
            metadata_digest, retention_policy_revision_id, retain_until, created_by
        ) VALUES ($1, $2, $3, 'authoring_document', 'internal', $4, $5,
                  $6, $6, 'ready', $7, $8, $9, $10, $11, $12)
        "#,
    )
    .bind(TENANT_ID)
    .bind(SKILL_PACKAGE_ARTIFACT_ID)
    .bind(SKILL_PACKAGE_BLOB_ID)
    .bind(i64::try_from(skill_package_bytes.len()).unwrap())
    .bind(skill_package_digest.to_string())
    .bind(SKILL_PACKAGE_MEDIA_TYPE)
    .bind(skill_package_metadata.schema_version)
    .bind(&skill_package_metadata.value)
    .bind(&skill_package_metadata.digest)
    .bind(POLICY_REVISION_ID)
    .bind(Utc::now() + Duration::days(30))
    .bind(PRINCIPAL_ID)
    .execute(pool)
    .await
    .unwrap();
    sqlx::query(
        "UPDATE insight_platform.resource_versions SET artifact_id = $3 WHERE tenant_id = $1 AND resource_version_id = $2",
    )
    .bind(TENANT_ID)
    .bind(AGENT_PLAN_ID)
    .bind(TYPED_PLAN_ARTIFACT_ID)
    .execute(pool)
    .await
    .unwrap();
    let evidence_metadata = TypedPayload::new(1, &json!({"fixture": "policy-evidence"})).unwrap();
    sqlx::query(
        r#"
        INSERT INTO insight_platform.artifact_blobs (
            tenant_id, blob_id, backend, storage_binding_digest,
            security_domain_digest, object_reference_ciphertext, object_generation, key_id,
            encryption_domain_id, content_digest, size_bytes, state, verified_at,
            created_at, updated_at
        ) VALUES ($1, $2, 'fixture', $3, $4, $5, 'fixture-generation-1',
                  'fixture-key', $6, $7, 1, 'verified', statement_timestamp(),
                  statement_timestamp(), statement_timestamp())
        "#,
    )
    .bind(TENANT_ID)
    .bind(POLICY_EVIDENCE_BLOB_ID)
    .bind(digest('a').to_string())
    .bind(digest('b').to_string())
    .bind(vec![1_u8])
    .bind("enc_0198f1c3-9a00-7c3e-b1f3-773c2836700e")
    .bind(digest('4').to_string())
    .execute(pool)
    .await
    .unwrap();
    sqlx::query(
        r#"
        INSERT INTO insight_platform.artifacts (
            tenant_id, artifact_id, blob_id, purpose, classification,
            expected_size_bytes, expected_digest, declared_media_type,
            verified_media_type, state, metadata_schema_version, metadata,
            metadata_digest, retention_policy_revision_id, retain_until, created_by
        ) VALUES ($1, $2, $3, 'qualification_evidence', 'internal', 1, $4,
                  'application/json', 'application/json', 'ready', $5, $6, $7,
                  $8, $9, $10)
        "#,
    )
    .bind(TENANT_ID)
    .bind(POLICY_EVIDENCE_ARTIFACT_ID)
    .bind(POLICY_EVIDENCE_BLOB_ID)
    .bind(digest('4').to_string())
    .bind(evidence_metadata.schema_version)
    .bind(&evidence_metadata.value)
    .bind(&evidence_metadata.digest)
    .bind(POLICY_REVISION_ID)
    .bind(Utc::now() + Duration::days(30))
    .bind(PRINCIPAL_ID)
    .execute(pool)
    .await
    .unwrap();
    sqlx::query(
        "UPDATE insight_platform.resources SET active_version_id = $3 WHERE tenant_id = $1 AND resource_id = $2",
    )
    .bind(TENANT_ID)
    .bind(POLICY_ID)
    .bind(POLICY_REVISION_ID)
    .execute(pool)
    .await
    .unwrap();

    let policy = ExactVersionRef::new(id(POLICY_REVISION_ID), digest('5')).unwrap();
    let policy_deployment_payload = TypedPayload::new(
        1,
        &DeploymentClosure::Policy(PolicyDeploymentClosure {
            policy_revision: policy.clone(),
            applicability_digest: digest('d'),
            qualification_evidence: ArtifactRef::new(
                id(POLICY_EVIDENCE_ARTIFACT_ID),
                digest('4'),
                1,
                "application/json",
                DataClassification::Internal,
                Some("scheduling-policy.json".to_owned()),
            )
            .unwrap(),
        }),
    )
    .unwrap();
    sqlx::query(
        r#"
        INSERT INTO insight_platform.deployments (
            tenant_id, deployment_id, resource_id, resource_version_id, environment,
            bindings_digest, payload_schema_version, bindings, created_by
        ) VALUES ($1, $2, $3, $4, 'test', $5, $6, $7, $8)
        "#,
    )
    .bind(TENANT_ID)
    .bind(POLICY_DEPLOYMENT_ID)
    .bind(POLICY_ID)
    .bind(POLICY_REVISION_ID)
    .bind(&policy_deployment_payload.digest)
    .bind(policy_deployment_payload.schema_version)
    .bind(&policy_deployment_payload.value)
    .bind(PRINCIPAL_ID)
    .execute(pool)
    .await
    .unwrap();
    sqlx::query(
        "UPDATE insight_platform.resources SET active_version_id = NULL, active_deployment_id = $3 WHERE tenant_id = $1 AND resource_id = $2",
    )
    .bind(TENANT_ID)
    .bind(POLICY_ID)
    .bind(POLICY_DEPLOYMENT_ID)
    .execute(pool)
    .await
    .unwrap();
    let policy_deployment = ExactDeploymentRef::new(
        id(POLICY_DEPLOYMENT_ID),
        policy_deployment_payload.digest.parse().unwrap(),
    )
    .unwrap();
    let policy_binding = ExactPolicyBinding {
        deployment: policy_deployment.clone(),
        revision: policy.clone(),
    };
    let selection_revision =
        ExactVersionRef::new(id(SELECTION_POLICY_REVISION_ID), digest('7')).unwrap();
    let selection_deployment_payload = TypedPayload::new(
        1,
        &DeploymentClosure::Policy(PolicyDeploymentClosure {
            policy_revision: selection_revision.clone(),
            applicability_digest: digest('e'),
            qualification_evidence: ArtifactRef::new(
                id(POLICY_EVIDENCE_ARTIFACT_ID),
                digest('4'),
                1,
                "application/json",
                DataClassification::Internal,
                Some("selection-policy.json".to_owned()),
            )
            .unwrap(),
        }),
    )
    .unwrap();
    sqlx::query(
        r#"
        INSERT INTO insight_platform.deployments (
            tenant_id, deployment_id, resource_id, resource_version_id, environment,
            bindings_digest, payload_schema_version, bindings, created_by
        ) VALUES ($1, $2, $3, $4, 'test', $5, $6, $7, $8)
        "#,
    )
    .bind(TENANT_ID)
    .bind(SELECTION_POLICY_DEPLOYMENT_ID)
    .bind(SELECTION_POLICY_ID)
    .bind(SELECTION_POLICY_REVISION_ID)
    .bind(&selection_deployment_payload.digest)
    .bind(selection_deployment_payload.schema_version)
    .bind(&selection_deployment_payload.value)
    .bind(PRINCIPAL_ID)
    .execute(pool)
    .await
    .unwrap();
    let selection_policy_binding = ExactPolicyBinding {
        deployment: ExactDeploymentRef::new(
            id(SELECTION_POLICY_DEPLOYMENT_ID),
            selection_deployment_payload.digest.parse().unwrap(),
        )
        .unwrap(),
        revision: selection_revision,
    };
    let skill_deployment_payload = TypedPayload::new(
        1,
        &DeploymentClosure::Skill(SkillDeploymentClosure {
            skill_revision: ExactVersionRef::new(id(SKILL_REVISION_ID), digest('d')).unwrap(),
            requirements: vec![],
            selection_policy: selection_policy_binding.clone(),
            qualification_evidence: ArtifactRef::new(
                id(TYPED_PLAN_ARTIFACT_ID),
                typed_plan_digest.clone(),
                u64::try_from(typed_plan_bytes.len()).unwrap(),
                "application/json",
                DataClassification::Internal,
                Some("typed-plan.json".to_owned()),
            )
            .unwrap(),
        }),
    )
    .unwrap();
    sqlx::query(
        r#"
        INSERT INTO insight_platform.deployments (
            tenant_id, deployment_id, resource_id, resource_version_id, environment,
            bindings_digest, payload_schema_version, bindings, created_by
        ) VALUES ($1, $2, $3, $4, 'test', $5, $6, $7, $8)
        "#,
    )
    .bind(TENANT_ID)
    .bind(SKILL_DEPLOYMENT_ID)
    .bind(SKILL_ID)
    .bind(SKILL_REVISION_ID)
    .bind(&skill_deployment_payload.digest)
    .bind(skill_deployment_payload.schema_version)
    .bind(&skill_deployment_payload.value)
    .bind(PRINCIPAL_ID)
    .execute(pool)
    .await
    .unwrap();
    let skill_deployment = ExactDeploymentRef::new(
        id(SKILL_DEPLOYMENT_ID),
        skill_deployment_payload.digest.parse().unwrap(),
    )
    .unwrap();
    let child_closure = AgentDeploymentClosure {
        interface: ExactVersionRef::new(id(CHILD_AGENT_INTERFACE_ID), digest('8')).unwrap(),
        plan: ExactVersionRef::new(id(CHILD_AGENT_PLAN_ID), digest('9')).unwrap(),
        entry_node_id: "entry".to_owned(),
        entry_node_kind: insight_platform_contracts::PlanNodeKind::Start,
        slots: vec![],
        policies: vec![policy_binding.clone()],
        execution_profile: policy_binding.clone(),
    };
    let child_deployment_payload =
        TypedPayload::new(1, &DeploymentClosure::Agent(child_closure)).unwrap();
    sqlx::query(
        r#"
        INSERT INTO insight_platform.deployments (
            tenant_id, deployment_id, resource_id, resource_version_id, environment,
            bindings_digest, payload_schema_version, bindings, created_by
        ) VALUES ($1, $2, $3, $4, 'test', $5, $6, $7, $8)
        "#,
    )
    .bind(TENANT_ID)
    .bind(CHILD_AGENT_DEPLOYMENT_ID)
    .bind(CHILD_AGENT_ID)
    .bind(CHILD_AGENT_PLAN_ID)
    .bind(&child_deployment_payload.digest)
    .bind(child_deployment_payload.schema_version)
    .bind(&child_deployment_payload.value)
    .bind(PRINCIPAL_ID)
    .execute(pool)
    .await
    .unwrap();
    let child_deployment = ExactDeploymentRef::new(
        id(CHILD_AGENT_DEPLOYMENT_ID),
        child_deployment_payload.digest.parse().unwrap(),
    )
    .unwrap();
    let closure = AgentDeploymentClosure {
        interface: ExactVersionRef::new(id(AGENT_INTERFACE_ID), digest('6')).unwrap(),
        plan: ExactVersionRef::new(id(AGENT_PLAN_ID), typed_plan_digest).unwrap(),
        entry_node_id: "start".to_owned(),
        entry_node_kind: insight_platform_contracts::PlanNodeKind::Start,
        slots: vec![
            FrozenSlotBinding {
                slot_id: "child_worker".to_owned(),
                requirement_digest: digest('b'),
                target: FrozenSlotTarget::ChildAgent {
                    candidates: vec![child_deployment],
                    selection_policy: selection_policy_binding.clone(),
                },
                binding_digest: digest('c'),
            },
            FrozenSlotBinding {
                slot_id: "review_skill".to_owned(),
                requirement_digest: digest('d'),
                target: FrozenSlotTarget::Skill {
                    candidates: vec![skill_deployment],
                    selection_policy: selection_policy_binding,
                },
                binding_digest: digest('e'),
            },
        ],
        policies: vec![policy_binding.clone()],
        execution_profile: policy_binding,
    };
    let deployment_payload =
        TypedPayload::new(1, &DeploymentClosure::Agent(closure.clone())).unwrap();
    sqlx::query(
        r#"
        INSERT INTO insight_platform.deployments (
            tenant_id, deployment_id, resource_id, resource_version_id, environment,
            bindings_digest, payload_schema_version, bindings, created_by
        ) VALUES ($1, $2, $3, $4, 'test', $5, $6, $7, $8)
        "#,
    )
    .bind(TENANT_ID)
    .bind(AGENT_DEPLOYMENT_ID)
    .bind(AGENT_ID)
    .bind(AGENT_PLAN_ID)
    .bind(&deployment_payload.digest)
    .bind(deployment_payload.schema_version)
    .bind(&deployment_payload.value)
    .bind(PRINCIPAL_ID)
    .execute(pool)
    .await
    .unwrap();
    sqlx::query(
        "UPDATE insight_platform.resources SET active_deployment_id = $3 WHERE tenant_id = $1 AND resource_id = $2",
    )
    .bind(TENANT_ID)
    .bind(CHILD_AGENT_ID)
    .bind(CHILD_AGENT_DEPLOYMENT_ID)
    .execute(pool)
    .await
    .unwrap();
    sqlx::query(
        "UPDATE insight_platform.resources SET active_deployment_id = $3 WHERE tenant_id = $1 AND resource_id = $2",
    )
    .bind(TENANT_ID)
    .bind(AGENT_ID)
    .bind(AGENT_DEPLOYMENT_ID)
    .execute(pool)
    .await
    .unwrap();

    let deployment_digest: Sha256Digest = deployment_payload.digest.parse().unwrap();
    let bindings = RunBindingsSnapshot::build(
        ExactDeploymentRef::new(id(AGENT_DEPLOYMENT_ID), deployment_digest).unwrap(),
        PrincipalSnapshot::build(
            id(TENANT_ID),
            id(PRINCIPAL_ID),
            PrincipalKind::AgentRunner,
            PermissionSet::new(vec![
                Permission::AgentRun,
                Permission::InteractionRespond,
                Permission::RuntimeControl,
                Permission::TenantManage,
            ])
            .unwrap(),
            1,
            1,
            1,
        )
        .unwrap(),
        &closure,
    )
    .unwrap();
    (bindings, policy_deployment)
}
