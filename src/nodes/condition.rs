use std::{collections::BTreeSet, sync::Arc};

use async_trait::async_trait;
use cel_interpreter::{Context as CelContext, Program as CelProgram, Value as CelValue};
use cel_parser::Parser;
use serde::Deserialize;
use serde_json::{json, Value};

use crate::{
    dsl::{
        compiled::{
            CompiledNode, NextPolicy, NodeCompilation, NodeControl, NodeEnvelopeRules, NodeOutcome,
            NodeTransition,
        },
        compiler::CompileContext,
        references::extract_cel_references,
        CompileError,
    },
    nodes::registry::{NodeExecutor, NodeType},
    runtime::{ExecutionControl, RunContext, RunError},
};

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct ConditionConfig {
    cases: Vec<ConditionCaseConfig>,
    default: String,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct ConditionCaseConfig {
    when: String,
    next: String,
}

#[derive(Debug)]
struct CompiledConditionCase {
    expression: String,
    program: CelProgram,
    next: String,
}

#[derive(Debug)]
struct CompiledCondition {
    cases: Vec<CompiledConditionCase>,
    default: String,
}

#[derive(Debug, Clone, Copy)]
pub struct ConditionNode;

impl NodeType for ConditionNode {
    fn kind(&self) -> &'static str {
        "core.condition"
    }

    fn compile(
        &self,
        node_id: &str,
        config: Value,
        _context: &mut CompileContext<'_>,
    ) -> Result<NodeCompilation, CompileError> {
        let config: ConditionConfig = serde_json::from_value(config).map_err(|error| {
            CompileError::new(
                "NODE_CONFIG_INVALID",
                format!("invalid core.condition config for node '{node_id}': {error}"),
            )
        })?;
        if config.cases.is_empty() {
            return Err(CompileError::new(
                "CONDITION_CASES_REQUIRED",
                format!("condition node '{node_id}' must define at least one case"),
            ));
        }
        if config.default.trim().is_empty() {
            return Err(CompileError::new(
                "CONDITION_DEFAULT_INVALID",
                format!("condition node '{node_id}' default must not be empty"),
            ));
        }

        let mut references = BTreeSet::new();
        let mut edges = Vec::with_capacity(config.cases.len() + 1);
        let mut cases = Vec::with_capacity(config.cases.len());
        for (index, case) in config.cases.into_iter().enumerate() {
            let expression = case.when.trim().to_string();
            if expression.is_empty() || case.next.trim().is_empty() {
                return Err(CompileError::new(
                    "CONDITION_CASE_INVALID",
                    format!(
                        "condition node '{node_id}' case {index} requires non-empty when and next"
                    ),
                ));
            }
            let parsed = Parser::default().parse(&expression).map_err(|error| {
                CompileError::new(
                    "CONDITION_EXPRESSION_INVALID",
                    format!(
                        "condition node '{node_id}' case {index} has invalid CEL expression: {error}"
                    ),
                )
            })?;
            references.extend(extract_cel_references(&parsed, node_id, index)?);
            let program = CelProgram::compile(&expression).map_err(|error| {
                CompileError::new(
                    "CONDITION_EXPRESSION_INVALID",
                    format!(
                        "condition node '{node_id}' case {index} has invalid CEL expression: {error}"
                    ),
                )
            })?;
            edges.push(case.next.clone());
            cases.push(CompiledConditionCase {
                expression,
                program,
                next: case.next,
            });
        }
        edges.push(config.default.clone());

        Ok(NodeCompilation {
            body: Arc::new(CompiledCondition {
                cases,
                default: config.default,
            }),
            edges,
            references,
            terminal: false,
            control: NodeControl::Ordinary,
            envelope: NodeEnvelopeRules {
                next: NextPolicy::Forbidden,
                allows_content_emit: false,
            },
        })
    }
}

#[async_trait]
impl NodeExecutor for ConditionNode {
    async fn execute(
        &self,
        node: &CompiledNode,
        context: &RunContext,
        control: &ExecutionControl,
    ) -> Result<NodeOutcome, RunError> {
        if let Some(reason) = control.stop_reason() {
            return Err(RunError::stopped(reason));
        }
        let body = node.body::<CompiledCondition>()?;
        let data = context.template_data();
        let Value::Object(variables) = data else {
            return Err(RunError::new(
                "CONDITION_CONTEXT_INVALID",
                "condition context must be a JSON object",
            ));
        };
        let mut cel_context = CelContext::default();
        for (name, value) in variables {
            cel_context.add_variable(&name, value).map_err(|error| {
                RunError::new(
                    "CONDITION_CONTEXT_INVALID",
                    format!("failed to prepare condition variable '{name}': {error}"),
                )
            })?;
        }

        for (index, case) in body.cases.iter().enumerate() {
            let result = case.program.execute(&cel_context).map_err(|error| {
                RunError::new(
                    "CONDITION_EVALUATION_FAILED",
                    format!(
                        "condition node '{}' failed to evaluate '{}': {error}",
                        node.id, case.expression
                    ),
                )
            })?;
            match result {
                CelValue::Bool(true) => {
                    return Ok(condition_outcome(Some(index), &case.next));
                }
                CelValue::Bool(false) => {}
                value => {
                    return Err(RunError::new(
                        "CONDITION_RESULT_NOT_BOOL",
                        format!(
                            "condition node '{}' expression '{}' returned {}, expected bool",
                            node.id,
                            case.expression,
                            value.type_of()
                        ),
                    ));
                }
            }
        }
        Ok(condition_outcome(None, &body.default))
    }
}

fn condition_outcome(matched_case: Option<usize>, next: &str) -> NodeOutcome {
    NodeOutcome {
        output: json!({"matched_case": matched_case, "next": next}),
        transition: NodeTransition::Goto(next.to_string()),
    }
}
