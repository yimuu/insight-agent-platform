//! Read-only editing hints derived from owning Rust values. Templates are incomplete
//! authoring drafts and can never substitute for shared compilation or admission.
use insight_platform_contracts::{canonical_json, PlanNodeKind, Sha256Digest, TaskEligibilityRule};
use insight_platform_plan::*;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::collections::{BTreeMap, BTreeSet};
pub const AGENT_NODE_EDITOR_VERSION: u32 = 1;
pub const MAX_AGENT_NODE_EDITOR_BYTES: usize = 131_072;
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AgentNodeEditorNodeV1 {
    pub kind: PlanNodeKind,
    pub template: Value,
}
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AgentNodeEditorDescriptorV1 {
    pub schema_version: u32,
    pub plan_version: u32,
    pub compiler_semantic_identity: Sha256Digest,
    pub draft_only: bool,
    pub nodes: Vec<AgentNodeEditorNodeV1>,
    pub templates: BTreeMap<String, Value>,
    pub choices: BTreeMap<String, Vec<Value>>,
}
fn digest() -> Sha256Digest {
    format!("sha256:{}", "0".repeat(64))
        .parse()
        .expect("explicit unbound draft digest")
}
fn node() -> PlanNodeKey {
    PlanNodeKey::new("replace_node".into()).expect("draft name")
}
fn port() -> ExactDataPortRef {
    ExactDataPortRef::RunInput {
        schema_digest: digest(),
    }
}
fn output() -> ExactDataPortRef {
    ExactDataPortRef::NodeOutput {
        producer_node_id: node(),
        port_id: DataPortKey::new("replace_port".into()).expect("draft name"),
        schema_digest: digest(),
    }
}
fn expression() -> TypedExpressionProgram {
    TypedExpressionProgram {
        expression_version: 1,
        input_ports: vec![port()],
        instructions: vec![TypedInstruction::LoadPort { port: port() }],
        output_schema_digest: digest(),
        maximum_stack_depth: 1,
        semantic_digest: digest(),
    }
}
fn budget() -> ChildBudgetLimit {
    ChildBudgetLimit {
        maximum_duration_milliseconds: 0,
        maximum_model_tokens: 0,
        maximum_capability_calls: 0,
        maximum_artifact_bytes: 0,
        maximum_descendant_runs: 0,
    }
}
fn human() -> HumanTaskDefinition {
    let rule = TaskEligibilityRule::AnyAuthorized;
    HumanTaskDefinition::HumanWork {
        eligible_principal_rule_digest: rule.canonical_digest().unwrap(),
        eligibility_rule: Some(rule),
        safe_prompt_key: "replace_prompt".into(),
    }
}
fn human_definitions() -> Vec<HumanTaskDefinition> {
    let mut definitions = vec![human()];
    for interaction_kind in insight_platform_contracts::InteractionKind::ALL {
        let eligibility_rule = TaskEligibilityRule::AnyAuthorized;
        definitions.push(HumanTaskDefinition::Interaction {
            eligible_principal_rule_digest: eligibility_rule.canonical_digest().unwrap(),
            eligibility_rule: Some(eligibility_rule),
            interaction_kind: *interaction_kind,
            safe_prompt_key: "replace_prompt".into(),
        });
    }
    definitions
}
fn typed_template(kind: PlanNodeKind) -> RuntimeNode {
    // Exhaustive owning enum match and typed constructors make additions visible
    // at build time; there is no TypeScript node-field registry.
    match kind {
        PlanNodeKind::Start => RuntimeNode::Start { next: node() },
        PlanNodeKind::Compute => RuntimeNode::Compute {
            assignments: vec![PortAssignment {
                output_port: output(),
                expression: expression(),
            }],
            next: node(),
        },
        PlanNodeKind::Branch => RuntimeNode::Branch {
            ordered_arms: vec![BranchArm {
                when: expression(),
                target: node(),
            }],
            otherwise: node(),
        },
        PlanNodeKind::Fork => RuntimeNode::Fork {
            legs: vec![node()],
            join: node(),
        },
        PlanNodeKind::Join => RuntimeNode::Join {
            policy: JoinPolicy::AllSettled,
            quorum: None,
            remainder: None,
            next: node(),
        },
        PlanNodeKind::Map => RuntimeNode::Map {
            items: expression(),
            item_port: output(),
            body: node(),
            next: node(),
            maximum_items: 0,
            failure_policy: MapFailurePolicy::FailFast,
        },
        PlanNodeKind::Loop => RuntimeNode::Loop {
            condition: expression(),
            carried_ports: vec![LoopCarriedPort {
                body_output_port: output(),
                next_iteration_port: output(),
            }],
            body: node(),
            exit: node(),
            maximum_iterations: 0,
        },
        PlanNodeKind::ErrorBoundary => RuntimeNode::ErrorBoundary {
            body: node(),
            handlers: BTreeMap::from([("replace_failure_code".into(), node())]),
        },
        PlanNodeKind::ModelLoop => RuntimeNode::ModelLoop {
            model_slot_id: "replace_model_slot".into(),
            skill_slot_ids: vec![],
            capability_slot_ids: vec![],
            input: port(),
            model_route: None,
            output: output(),
            maximum_rounds: 0,
            maximum_capability_calls: 0,
            maximum_parallel_calls_per_round: 0,
            token_budget: 0,
            resume: node(),
        },
        PlanNodeKind::CapabilityCall => RuntimeNode::CapabilityCall {
            capability_slot_id: "replace_capability_slot".into(),
            input: port(),
            candidate_route: None,
            output: output(),
            attempt_limit: 0,
            retry_backoff_milliseconds: 0,
            resume: node(),
        },
        PlanNodeKind::ContextQuery => RuntimeNode::ContextQuery {
            context_slot_id: "replace_context_slot".into(),
            request: port(),
            result: output(),
            maximum_items: 0,
            resume: node(),
        },
        PlanNodeKind::ChildAgentCall => RuntimeNode::ChildAgentCall {
            child_agent_slot_id: "replace_agent_slot".into(),
            input: port(),
            candidate_route: None,
            output: output(),
            budget: budget(),
            cancellation_policy: ChildCancellationPolicy::CascadeAndWait,
            attempt_limit: 0,
            retry_backoff_milliseconds: 0,
            resume: node(),
        },
        PlanNodeKind::HumanTask => RuntimeNode::HumanTask {
            definition: human(),
            response: output(),
            timeout_milliseconds: 0,
            resume: node(),
        },
        PlanNodeKind::TimerWait => RuntimeNode::TimerWait {
            delay_milliseconds: 0,
            resume: node(),
        },
        PlanNodeKind::SignalWait => RuntimeNode::SignalWait {
            signal_key: "replace_signal".into(),
            payload: None,
            timeout_milliseconds: 0,
            resume: node(),
        },
        PlanNodeKind::Return => RuntimeNode::Return { value: port() },
        PlanNodeKind::Raise => RuntimeNode::Raise { failure: port() },
    }
}
fn value(value: impl Serialize) -> Value {
    serde_json::to_value(value).expect("typed editor projection")
}
fn instructions() -> Vec<TypedInstruction> {
    use TypedInstruction::*;
    let result = vec![
        LoadPort { port: port() },
        Literal {
            value: insight_platform_contracts::ClosedJsonValue::build(
                insight_platform_contracts::ClosedValueSchema::build(
                    json!({"$schema":"https://json-schema.org/draft/2020-12/schema","type":"null"}),
                )
                .unwrap()
                .canonical_digest,
                json!(null),
            )
            .unwrap(),
        },
        GetField {
            field: ExpressionFieldName::new("replace_field".into()).unwrap(),
        },
        GetIndex,
        ArrayLength,
        MakeArray { item_count: 0 },
        MakeObject {
            ordered_fields: vec![],
        },
        Equal,
        NotEqual,
        Less,
        LessOrEqual,
        Greater,
        GreaterOrEqual,
        BooleanAnd,
        BooleanOr,
        BooleanNot,
        IntegerAdd,
        IntegerSubtract,
        DecimalAdd,
        DecimalSubtract,
        StringConcat,
        Coalesce,
        Select,
    ];
    for instruction in &result {
        match instruction {
            LoadPort { port: _ }
            | Literal { value: _ }
            | GetField { field: _ }
            | GetIndex
            | ArrayLength
            | MakeArray { item_count: _ }
            | MakeObject { ordered_fields: _ }
            | Equal
            | NotEqual
            | Less
            | LessOrEqual
            | Greater
            | GreaterOrEqual
            | BooleanAnd
            | BooleanOr
            | BooleanNot
            | IntegerAdd
            | IntegerSubtract
            | DecimalAdd
            | DecimalSubtract
            | StringConcat
            | Coalesce
            | Select => {}
        }
    }
    result
}
pub fn agent_node_editor_descriptor(
) -> Result<AgentNodeEditorDescriptorV1, crate::AgentBoundaryErrorCode> {
    let joins = [
        JoinPolicy::AllSuccess,
        JoinPolicy::AllSettled,
        JoinPolicy::Quorum,
    ];
    for choice in joins {
        match choice {
            JoinPolicy::AllSuccess | JoinPolicy::AllSettled | JoinPolicy::Quorum => {}
        }
    }
    let remainders = [JoinRemainderPolicy::Cancel, JoinRemainderPolicy::Drain];
    for choice in remainders {
        match choice {
            JoinRemainderPolicy::Cancel | JoinRemainderPolicy::Drain => {}
        }
    }
    let failures = [
        MapFailurePolicy::FailFast,
        MapFailurePolicy::AllSettled,
        MapFailurePolicy::BoundedErrorCount {
            maximum_failures: 0,
        },
    ];
    for choice in failures {
        match choice {
            MapFailurePolicy::FailFast
            | MapFailurePolicy::AllSettled
            | MapFailurePolicy::BoundedErrorCount {
                maximum_failures: _,
            } => {}
        }
    }
    let ports = [port(), output()];
    for choice in &ports {
        match choice {
            ExactDataPortRef::RunInput { schema_digest: _ }
            | ExactDataPortRef::NodeOutput {
                producer_node_id: _,
                port_id: _,
                schema_digest: _,
            } => {}
        }
    }
    let cancellations = [
        ChildCancellationPolicy::CascadeAndWait,
        ChildCancellationPolicy::CascadeWithDeadline,
    ];
    for choice in cancellations {
        match choice {
            ChildCancellationPolicy::CascadeAndWait
            | ChildCancellationPolicy::CascadeWithDeadline => {}
        }
    }
    let dependencies = [
        RuntimeDependencyKind::Model,
        RuntimeDependencyKind::Capability,
        RuntimeDependencyKind::Context,
        RuntimeDependencyKind::ChildAgent,
        RuntimeDependencyKind::Skill,
    ];
    for choice in dependencies {
        match choice {
            RuntimeDependencyKind::Model
            | RuntimeDependencyKind::Capability
            | RuntimeDependencyKind::Context
            | RuntimeDependencyKind::ChildAgent
            | RuntimeDependencyKind::Skill => {}
        }
    }
    let eligibility = [
        TaskEligibilityRule::AnyAuthorized,
        TaskEligibilityRule::Creator,
        TaskEligibilityRule::ExactPrincipal {
            principal_id: "prn_018f3e20-0000-7000-8000-000000000000".parse().unwrap(),
        },
    ];
    for choice in &eligibility {
        match choice {
            TaskEligibilityRule::AnyAuthorized
            | TaskEligibilityRule::Creator
            | TaskEligibilityRule::ExactPrincipal { principal_id: _ } => {}
        }
    }
    let human_definitions = human_definitions();
    for choice in &human_definitions {
        match choice {
            HumanTaskDefinition::HumanWork {
                eligibility_rule: _,
                eligible_principal_rule_digest: _,
                safe_prompt_key: _,
            }
            | HumanTaskDefinition::Interaction {
                eligibility_rule: _,
                interaction_kind: _,
                eligible_principal_rule_digest: _,
                safe_prompt_key: _,
            } => {}
        }
    }
    let result=AgentNodeEditorDescriptorV1{schema_version:AGENT_NODE_EDITOR_VERSION,plan_version:6,compiler_semantic_identity:crate::compiler_semantic_identity(),draft_only:true,
        nodes:PlanNodeKind::ALL.iter().map(|kind|AgentNodeEditorNodeV1{kind:*kind,template:value(typed_template(*kind))}).collect(),
        templates:BTreeMap::from([("port".into(),value(port())),("output_port".into(),value(output())),("expression".into(),value(expression())),("branch_arm".into(),value(BranchArm{when:expression(),target:node()})),("loop_carried_port".into(),value(LoopCarriedPort{body_output_port:output(),next_iteration_port:output()})),("assignment".into(),value(PortAssignment{output_port:output(),expression:expression()})),("child_budget".into(),value(budget())),("human_definition".into(),value(human())),("instruction".into(),value(TypedInstruction::LoadPort{port:port()})),("dependency_slot".into(),value(RuntimeDependencySlot{kind:RuntimeDependencyKind::Model,requirement_digest:digest()})),("schema_document".into(),value(insight_platform_contracts::ClosedValueSchema::build(json!({"$schema":"https://json-schema.org/draft/2020-12/schema","type":"object","additionalProperties":false,"properties":{},"required":[]})).unwrap())),("null_schema_document".into(),value(insight_platform_contracts::ClosedValueSchema::build(json!({"$schema":"https://json-schema.org/draft/2020-12/schema","type":"null"})).unwrap())),("eligibility_rule".into(),value(TaskEligibilityRule::AnyAuthorized)),("failure_policy".into(),value(MapFailurePolicy::FailFast))]),
        choices:BTreeMap::from([("human_definition".into(),human_definitions.into_iter().map(value).collect()),("join_policy".into(),joins.into_iter().map(value).collect()),("remainder".into(),remainders.into_iter().map(value).collect()),("map_failure_policy".into(),failures.into_iter().map(value).collect()),("instruction".into(),instructions().into_iter().map(value).collect()),("port".into(),ports.into_iter().map(value).collect()),("cancellation_policy".into(),cancellations.into_iter().map(value).collect()), ("dependency_kind".into(),dependencies.into_iter().map(value).collect()), ("eligibility_rule".into(),eligibility.into_iter().map(value).collect())])};
    result.canonical_bytes()?;
    Ok(result)
}
impl AgentNodeEditorDescriptorV1 {
    pub fn canonical_bytes(&self) -> Result<Vec<u8>, crate::AgentBoundaryErrorCode> {
        use crate::AgentBoundaryErrorCode::{CompilerLimitExceeded, SourceBundleInvalid};
        if self.schema_version != AGENT_NODE_EDITOR_VERSION
            || self.plan_version != 6
            || self.compiler_semantic_identity != crate::compiler_semantic_identity()
            || !self.draft_only
            || self.nodes.len() != PlanNodeKind::ALL.len()
            || self.templates.len() > 32
            || self.choices.len() > 32
        {
            return Err(SourceBundleInvalid);
        }
        let mut budget = 0usize;
        for node in &self.nodes {
            bounded_editor_value(&node.template, 0, &mut budget)?;
        }
        for (key, template) in &self.templates {
            if key.len() > 64 {
                return Err(CompilerLimitExceeded);
            }
            bounded_editor_value(template, 0, &mut budget)?;
        }
        for (key, choices) in &self.choices {
            if key.len() > 64 || choices.len() > 64 {
                return Err(CompilerLimitExceeded);
            }
            for choice in choices {
                bounded_editor_value(choice, 0, &mut budget)?;
            }
        }
        let mut kinds = BTreeSet::new();
        for node in &self.nodes {
            if !kinds.insert(node.kind)
                || serde_json::from_value::<RuntimeNode>(node.template.clone()).is_err()
                || node.template.get("kind").and_then(Value::as_str) != Some(node.kind.as_str())
            {
                return Err(SourceBundleInvalid);
            }
        }
        let bytes = canonical_json(&value(self)).map_err(|_| SourceBundleInvalid)?;
        if bytes.len() > MAX_AGENT_NODE_EDITOR_BYTES {
            return Err(CompilerLimitExceeded);
        }
        Ok(bytes)
    }
}

fn bounded_editor_value(
    value: &Value,
    depth: usize,
    budget: &mut usize,
) -> Result<(), crate::AgentBoundaryErrorCode> {
    use crate::AgentBoundaryErrorCode::CompilerLimitExceeded;
    if depth > 32 {
        return Err(CompilerLimitExceeded);
    }
    *budget = budget.checked_add(16).ok_or(CompilerLimitExceeded)?;
    match value {
        Value::String(text) => {
            if text.len() > 4096 {
                return Err(CompilerLimitExceeded);
            }
            *budget = budget
                .checked_add(text.len())
                .ok_or(CompilerLimitExceeded)?;
        }
        Value::Object(fields) => {
            if fields.len() > 128 {
                return Err(CompilerLimitExceeded);
            }
            for (key, child) in fields {
                if key.len() > 128 {
                    return Err(CompilerLimitExceeded);
                }
                *budget = budget.checked_add(key.len()).ok_or(CompilerLimitExceeded)?;
                bounded_editor_value(child, depth + 1, budget)?;
            }
        }
        Value::Array(values) => {
            if values.len() > 4096 {
                return Err(CompilerLimitExceeded);
            }
            for child in values {
                bounded_editor_value(child, depth + 1, budget)?;
            }
        }
        _ => {}
    }
    if *budget > MAX_AGENT_NODE_EDITOR_BYTES {
        return Err(CompilerLimitExceeded);
    }
    Ok(())
}

/// Editing input only. The complete Plan and Registry compilation remain authoritative.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AgentExpressionBuildRequestV1 {
    pub schema_version: u32,
    pub input_ports: Vec<ExactDataPortRef>,
    pub instructions: Vec<TypedInstruction>,
    pub output_schema_digest: Sha256Digest,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "outcome", rename_all = "snake_case", deny_unknown_fields)]
pub enum AgentExpressionBuildResponseV1 {
    Built {
        schema_version: u32,
        expression: TypedExpressionProgram,
    },
    Rejected {
        diagnostics: Vec<crate::AgentCompilerDiagnosticV1>,
    },
}
fn expression_rejected(code: crate::AgentBoundaryErrorCode) -> AgentExpressionBuildResponseV1 {
    AgentExpressionBuildResponseV1::Rejected {
        diagnostics: vec![crate::AgentCompilerDiagnosticV1 {
            code,
            location: None,
            safe_detail: "Expression editing request was rejected by the shared compiler.".into(),
        }],
    }
}
pub fn build_expression_request_bytes(input: &[u8]) -> Vec<u8> {
    use crate::AgentBoundaryErrorCode;
    let result = crate::boundary::parse_boundary::<AgentExpressionBuildRequestV1>(input).and_then(
        |request| {
            if request.schema_version != 1 {
                return Err(AgentBoundaryErrorCode::AuthoringVersionUnsupported);
            }
            let limits = ExpressionLimits::from_profile(
                &insight_platform_contracts::checked_in_hard_limit_profile(),
            )
            .map_err(|_| AgentBoundaryErrorCode::CompilerInternal)?;
            TypedExpressionProgram::build(
                request.input_ports,
                request.instructions,
                request.output_schema_digest,
                limits,
            )
            .map_err(|error| match error {
                ExpressionError::LimitExceeded => AgentBoundaryErrorCode::CompilerLimitExceeded,
                _ => AgentBoundaryErrorCode::AgentCompileFailed,
            })
        },
    );
    let response = match result {
        Ok(expression) => AgentExpressionBuildResponseV1::Built {
            schema_version: 1,
            expression,
        },
        Err(code) => expression_rejected(code),
    };
    let bytes = serde_json::to_vec(&response).expect("closed expression response serializes");
    if bytes.len() > crate::MAX_AGENT_COMPILER_RESPONSE_BYTES {
        serde_json::to_vec(&expression_rejected(
            AgentBoundaryErrorCode::CompilerLimitExceeded,
        ))
        .expect("bounded rejection serializes")
    } else {
        bytes
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn expression_editor_rebuilds_with_owning_limits_and_strict_transport() {
        let request = AgentExpressionBuildRequestV1 {
            schema_version: 1,
            input_ports: vec![port()],
            instructions: vec![TypedInstruction::LoadPort { port: port() }],
            output_schema_digest: digest(),
        };
        let input = serde_json::to_vec(&request).unwrap();
        let response: AgentExpressionBuildResponseV1 =
            serde_json::from_slice(&build_expression_request_bytes(&input)).unwrap();
        let AgentExpressionBuildResponseV1::Built { expression, .. } = response else {
            panic!("valid expression rejected")
        };
        let limits = ExpressionLimits::from_profile(
            &insight_platform_contracts::checked_in_hard_limit_profile(),
        )
        .unwrap();
        assert_eq!(
            expression,
            TypedExpressionProgram::build(
                request.input_ports.clone(),
                request.instructions.clone(),
                request.output_schema_digest.clone(),
                limits
            )
            .unwrap()
        );
        assert_eq!(expression.maximum_stack_depth, 1);
        assert_ne!(expression.semantic_digest, digest());
        let text = String::from_utf8(input).unwrap();
        let invalid = [
            text.replacen('{', "{\"unknown\":true,", 1),
            text.replacen(
                "\"schema_version\":1",
                "\"schema_version\":1,\"schema_version\":1",
                1,
            ),
        ];
        for input in invalid {
            assert!(matches!(
                serde_json::from_slice::<AgentExpressionBuildResponseV1>(
                    &build_expression_request_bytes(input.as_bytes())
                )
                .unwrap(),
                AgentExpressionBuildResponseV1::Rejected { .. }
            ));
        }
        let mut invalid = request.clone();
        invalid.instructions = vec![TypedInstruction::BooleanNot];
        assert!(
            matches!(serde_json::from_slice::<AgentExpressionBuildResponseV1>(&build_expression_request_bytes(&serde_json::to_vec(&invalid).unwrap())).unwrap(), AgentExpressionBuildResponseV1::Rejected { diagnostics } if diagnostics[0].code == crate::AgentBoundaryErrorCode::AgentCompileFailed)
        );
        invalid.instructions =
            vec![TypedInstruction::LoadPort { port: port() }; limits.maximum_instructions + 1];
        assert!(
            matches!(serde_json::from_slice::<AgentExpressionBuildResponseV1>(&build_expression_request_bytes(&serde_json::to_vec(&invalid).unwrap())).unwrap(), AgentExpressionBuildResponseV1::Rejected { diagnostics } if diagnostics[0].code == crate::AgentBoundaryErrorCode::CompilerLimitExceeded)
        );
        assert!(
            matches!(serde_json::from_slice::<AgentExpressionBuildResponseV1>(&build_expression_request_bytes(&vec![b' '; crate::MAX_AGENT_COMPILER_REQUEST_BYTES + 1])).unwrap(), AgentExpressionBuildResponseV1::Rejected { diagnostics } if diagnostics[0].code == crate::AgentBoundaryErrorCode::CompilerLimitExceeded)
        );
    }
    #[test]
    fn editing_hints_are_typed_drafts_and_bounded_before_serialization() {
        let descriptor = agent_node_editor_descriptor().unwrap();
        assert!(descriptor.draft_only);
        assert_eq!(descriptor.nodes.len(), PlanNodeKind::ALL.len());
        for hint in &descriptor.nodes {
            let actual: RuntimeNode = serde_json::from_value(hint.template.clone()).unwrap();
            assert_eq!(serde_json::to_value(actual).unwrap(), hint.template);
        }
        assert!(descriptor.nodes.iter().any(
            |hint| hint.kind == PlanNodeKind::Loop && hint.template["maximum_iterations"] == 0
        ));
        assert!(descriptor
            .nodes
            .iter()
            .any(|hint| hint.kind == PlanNodeKind::ChildAgentCall
                && hint.template["child_agent_slot_id"] == "replace_agent_slot"));
        let definitions: Vec<HumanTaskDefinition> = descriptor.choices["human_definition"]
            .iter()
            .cloned()
            .map(|value| serde_json::from_value(value).unwrap())
            .collect();
        assert_eq!(
            definitions.len(),
            insight_platform_contracts::InteractionKind::ALL.len() + 1
        );
        for definition in definitions {
            assert_eq!(
                definition
                    .eligibility_rule()
                    .unwrap()
                    .canonical_digest()
                    .unwrap(),
                *definition.eligibility_rule_digest()
            );
        }
        let mut unknown = descriptor.clone();
        unknown.nodes[0].template["unknown_field"] = json!(true);
        assert!(unknown.canonical_bytes().is_err());
        let mut overbound = descriptor.clone();
        overbound
            .templates
            .insert("oversized".into(), json!("x".repeat(4097)));
        assert_eq!(
            overbound.canonical_bytes().unwrap_err(),
            crate::AgentBoundaryErrorCode::CompilerLimitExceeded
        );
        let mut overbound = descriptor.clone();
        overbound
            .choices
            .insert("oversized".into(), vec![json!(null); 65]);
        assert_eq!(
            overbound.canonical_bytes().unwrap_err(),
            crate::AgentBoundaryErrorCode::CompilerLimitExceeded
        );
        assert!(descriptor.canonical_bytes().unwrap().len() < MAX_AGENT_NODE_EDITOR_BYTES);
    }
}
