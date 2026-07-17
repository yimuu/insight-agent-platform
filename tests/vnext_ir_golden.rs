use std::{collections::BTreeMap, path::Path};

use insight_agent_platform::{
    dsl::vnext::{
        compiler::WorkflowCompiler,
        ir::{
            Branch, Call, IrValueType, Operation, OperationId, OperationKind, OperationRole,
            Parallel, ParameterSource, Phi, Region, RegionKind, RootReturn, Terminator,
            TypedContract, ValueDefinition, ValueId, ValueRole, WorkflowIr,
        },
        raw::{ErrorCategory, OutputFormat, ParallelSettle},
        types::ValueType,
        value::Identifier,
    },
    resources::{actions::ActionRegistry, models::ModelRegistry},
};
use serde_json::{json, Map, Value};

const SOURCE: &str = r#"
api_version: insight.agent/v2
kind: agent
metadata:
  id: normalized_fixture
  name: Normalized Fixture
  description: Deterministic Region SSA snapshot.
schema_dialect: https://json-schema.org/draft/2020-12/schema
input:
  schema:
    type: object
    required: [question]
    properties:
      question: {type: string}
    additionalProperties: false
output:
  data_schema:
    type: object
    required: [answer]
    properties:
      answer: {type: string}
    additionalProperties: false
workflow:
  steps:
    - kind: parallel
      id: candidates
      with:
        question: {from: input.question}
      settle: all
      max_concurrency: 2
      branches:
        left:
          output_schema: {type: string}
          result:
            return: {from: scope.question}
        right:
          output_schema: {type: string}
          result:
            return: {literal: fallback}

    - kind: switch
      id: selected
      with:
        candidates: {from: steps.candidates.output}
      output_schema: {type: string}
      cases:
        - id: left_nonempty
          when:
            cel: "scope.candidates.left != ''"
          result:
            return: {from: scope.candidates.left}
      default:
        id: fallback
        result:
          return: {from: scope.candidates.right}
  result:
    return:
      data:
        object:
          answer: {from: steps.selected.output}
"#;

#[test]
fn normalized_region_ssa_has_a_stable_test_owned_serialization() {
    let compiler = WorkflowCompiler::new(ModelRegistry::default(), ActionRegistry::default());
    let root = Path::new(env!("CARGO_MANIFEST_DIR"));
    let first = compiler.compile_source(root, SOURCE).unwrap();
    let second = compiler.compile_source(root, SOURCE).unwrap();
    let first = serde_json::to_string_pretty(&workflow_snapshot(&first.ir)).unwrap();
    let second = serde_json::to_string_pretty(&workflow_snapshot(&second.ir)).unwrap();

    assert_eq!(
        first, second,
        "repeated lowering must normalize identically"
    );
    assert_eq!(
        first,
        include_str!("fixtures/vnext_normalized_ir.golden.json").trim_end()
    );
}

/// Test-only serialization deliberately spells out every IR enum. This keeps the
/// production IR free to remain non-Serialize and avoids unstable Debug output.
fn workflow_snapshot(ir: &WorkflowIr) -> Value {
    json!({
        "metadata": {
            "id": ir.metadata.id.as_str(),
            "name": ir.metadata.name,
            "description": ir.metadata.description,
        },
        "input": contract_snapshot(&ir.input),
        "output": contract_snapshot(&ir.output),
        "prompts": Value::Object(ir.prompts.iter().map(|(name, prompt)| {
            (name.as_str().to_string(), json!({"text": prompt.text}))
        }).collect()),
        "errors": Value::Object(ir.errors.iter().map(|(name, error)| {
            let category = match error.category {
                ErrorCategory::Workflow => "workflow",
            };
            (name.as_str().to_string(), json!({
                "category": category,
                "code": error.code,
                "public_message": error.public_message,
            }))
        }).collect()),
        "root": region_snapshot(&ir.root),
    })
}

fn contract_snapshot(contract: &TypedContract) -> Value {
    json!({
        "schema": contract.schema,
        "value_type": value_type_snapshot(&contract.value_type),
    })
}

fn region_snapshot(region: &Region) -> Value {
    let kind = match &region.kind {
        RegionKind::Workflow => json!({"kind": "workflow"}),
        RegionKind::ParallelBranch { name } => {
            json!({"kind": "parallel_branch", "name": name.as_str()})
        }
        RegionKind::SwitchArm { name, is_default } => json!({
            "kind": "switch_arm",
            "name": name.as_str(),
            "is_default": is_default,
        }),
    };
    json!({
        "id": region.id.path().as_str(),
        "kind": kind,
        "parameters": region.parameters.iter().map(|parameter| json!({
            "name": parameter.name.as_str(),
            "value": value_definition_snapshot(&parameter.value),
            "source": parameter_source_snapshot(&parameter.source),
        })).collect::<Vec<_>>(),
        "operations": region.operations.iter().map(operation_snapshot).collect::<Vec<_>>(),
        "result": contract_snapshot(&region.result),
        "terminator": region.terminator.as_ref().map(terminator_snapshot),
    })
}

fn parameter_source_snapshot(source: &ParameterSource) -> Value {
    match source {
        ParameterSource::WorkflowInput => json!({"kind": "workflow_input"}),
        ParameterSource::RunMetadata => json!({"kind": "run_metadata"}),
        ParameterSource::Capture { source } => {
            json!({"kind": "capture", "source": value_id(source)})
        }
    }
}

fn operation_snapshot(operation: &Operation) -> Value {
    json!({
        "id": operation_id(&operation.id),
        "output": value_definition_snapshot(&operation.output),
        "operation": operation_kind_snapshot(&operation.kind),
    })
}

fn operation_kind_snapshot(kind: &OperationKind) -> Value {
    match kind {
        OperationKind::Const { value } => json!({"kind": "const", "value": value}),
        OperationKind::Project { source, path } => json!({
            "kind": "project",
            "source": value_id(source),
            "path": path.as_str(),
        }),
        OperationKind::Object { fields } => json!({
            "kind": "object",
            "fields": value_id_string_map(fields),
        }),
        OperationKind::Array { items } => json!({
            "kind": "array",
            "items": items.iter().map(value_id).collect::<Vec<_>>(),
        }),
        OperationKind::Template { text, bindings } => json!({
            "kind": "template",
            "text": text,
            "bindings": value_id_identifier_map(bindings),
        }),
        OperationKind::Prompt { prompt } => {
            json!({"kind": "prompt", "prompt": prompt.as_str()})
        }
        OperationKind::Call(call) => call_snapshot(call),
        OperationKind::Parallel(parallel) => parallel_snapshot(parallel),
        OperationKind::Branch(branch) => branch_snapshot(branch),
        OperationKind::Phi(phi) => phi_snapshot(phi),
    }
}

fn call_snapshot(call: &Call) -> Value {
    json!({
        "kind": "call",
        "uses": call.uses,
        "inputs": value_id_identifier_map(&call.inputs),
        "config": call.config,
    })
}

fn parallel_snapshot(parallel: &Parallel) -> Value {
    let settle = match parallel.settle {
        ParallelSettle::All => "all",
        ParallelSettle::AllSettled => "all_settled",
    };
    json!({
        "kind": "parallel",
        "inputs": value_id_identifier_map(&parallel.inputs),
        "settle": settle,
        "max_concurrency": parallel.max_concurrency,
        "branches": Value::Object(parallel.branches.iter().map(|(name, region)| {
            (name.as_str().to_string(), region_snapshot(region))
        }).collect()),
    })
}

fn branch_snapshot(branch: &Branch) -> Value {
    json!({
        "kind": "branch",
        "inputs": value_id_identifier_map(&branch.inputs),
        "cases": branch.cases.iter().map(|case| json!({
            "id": case.id.as_str(),
            "predicate": case.predicate.source,
            "region": region_snapshot(&case.region),
        })).collect::<Vec<_>>(),
        "default": {
            "id": branch.default.id.as_str(),
            "region": region_snapshot(&branch.default.region),
        },
    })
}

fn phi_snapshot(phi: &Phi) -> Value {
    json!({
        "kind": "phi",
        "branch": operation_id(&phi.branch),
        "token": value_id(&phi.token),
        "incomings": phi.incomings.iter().map(|region| region.path().as_str()).collect::<Vec<_>>(),
    })
}

fn terminator_snapshot(terminator: &Terminator) -> Value {
    match terminator {
        Terminator::RegionYield { value } => {
            json!({"kind": "region_yield", "value": value_id(value)})
        }
        Terminator::WorkflowReturn(root) => root_return_snapshot(root),
        Terminator::Raise { error } => {
            json!({"kind": "raise", "error": error.as_str()})
        }
    }
}

fn root_return_snapshot(root: &RootReturn) -> Value {
    let format = root.format.map(|format| match format {
        OutputFormat::Text => "text",
        OutputFormat::Markdown => "markdown",
    });
    json!({
        "kind": "workflow_return",
        "content": root.content.as_ref().map(value_id),
        "format": format,
        "data": value_id(&root.data),
    })
}

fn value_definition_snapshot(definition: &ValueDefinition) -> Value {
    let value_type = match &definition.value_type {
        IrValueType::Data(value_type) => {
            json!({"kind": "data", "value_type": value_type_snapshot(value_type)})
        }
        IrValueType::Control { result_type } => json!({
            "kind": "control",
            "result_type": value_type_snapshot(result_type),
        }),
    };
    json!({"id": value_id(&definition.id), "type": value_type})
}

fn value_type_snapshot(value_type: &ValueType) -> Value {
    match value_type {
        ValueType::Never => json!({"kind": "never"}),
        ValueType::Any => json!({"kind": "any"}),
        ValueType::Null => json!({"kind": "null"}),
        ValueType::Boolean => json!({"kind": "boolean"}),
        ValueType::Integer => json!({"kind": "integer"}),
        ValueType::Number => json!({"kind": "number"}),
        ValueType::String => json!({"kind": "string"}),
        ValueType::Literal(value) => json!({"kind": "literal", "value": value}),
        ValueType::Array(array) => json!({
            "kind": "array",
            "items": value_type_snapshot(&array.items),
            "min_items": array.min_items,
        }),
        ValueType::Object(object) => json!({
            "kind": "object",
            "properties": Value::Object(object.properties.iter().map(|(name, property)| {
                (name.clone(), json!({
                    "required": property.required,
                    "value_type": value_type_snapshot(&property.value_type),
                }))
            }).collect()),
            "additional_properties": object.additional_properties.as_ref().map(|value_type| {
                value_type_snapshot(value_type)
            }),
        }),
        ValueType::Union(variants) => json!({
            "kind": "union",
            "variants": variants.iter().map(value_type_snapshot).collect::<Vec<_>>(),
        }),
    }
}

fn operation_id(id: &OperationId) -> String {
    let role = match id.role() {
        OperationRole::Authored => "authored".to_string(),
        OperationRole::Expression(ordinal) => format!("expression:{ordinal}"),
        OperationRole::Phi => "phi".to_string(),
    };
    format!("{}#{role}", id.path().as_str())
}

fn value_id(id: &ValueId) -> String {
    let role = match id.role() {
        ValueRole::Parameter(ordinal) => format!("parameter:{ordinal}"),
        ValueRole::AuthoredOutput => "authored_output".to_string(),
        ValueRole::ExpressionOutput(ordinal) => format!("expression_output:{ordinal}"),
        ValueRole::BranchControl => "branch_control".to_string(),
        ValueRole::PhiOutput => "phi_output".to_string(),
    };
    format!("{}#{role}", id.path().as_str())
}

fn value_id_identifier_map(values: &BTreeMap<Identifier, ValueId>) -> Value {
    Value::Object(
        values
            .iter()
            .map(|(name, value)| (name.as_str().to_string(), json!(value_id(value))))
            .collect::<Map<_, _>>(),
    )
}

fn value_id_string_map(values: &BTreeMap<String, ValueId>) -> Value {
    Value::Object(
        values
            .iter()
            .map(|(name, value)| (name.clone(), json!(value_id(value))))
            .collect::<Map<_, _>>(),
    )
}
