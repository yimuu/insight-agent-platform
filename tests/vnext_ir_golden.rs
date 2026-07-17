use std::{collections::BTreeMap, path::Path};

use async_trait::async_trait;
use insight_agent_platform::{
    dsl::vnext::{
        compiler::WorkflowCompiler,
        ir::{
            Branch, Call, ErrorCategory, IrValueType, Operation, OperationId, OperationKind,
            OperationRole, OutputFormat, Parallel, ParallelSettle, ParameterSource, Phi, Region,
            RegionKind, RootReturn, Terminator, TypedContract, ValueDefinition, ValueId, ValueRole,
            WorkflowIr,
        },
        plan::{
            CallPlan, CompiledContentAtom, MessageSourcePlan, PlannedRole, PlannedTemplate,
            TemplateProfileVersion, TemplateProvenance, ValidatedResponseContract,
        },
        types::ValueType,
        value::Identifier,
    },
    resources::{
        actions::ActionRegistry,
        builtin_actions::TextMetricsAction,
        models::{ChatModel, ChatRequest, ChatStream, ModelCapability, ModelRegistry},
    },
    runtime::RunError,
};
use serde_json::{json, Map, Value};

const SOURCE: &str = r#"
api_version: insight.agent/v2
kind: agent
metadata:
  id: normalized_fixture
  name: Normalized Fixture
  description: Deterministic Region SSA snapshot.
types:
  Answer:
    fields:
      answer: string
inputs:
  question: string
output: Answer
prompts:
  system:
    inline: You are a concise analysis assistant.
workflow:
  steps:
    - type: llm
      id: analyze
      model: golden_chat
      messages:
        - role: system
          content:
            - text: system
        - role: user
          content:
            - text: $question
      response: string

    - type: action
      id: metrics
      call: example.text_metrics
      inputs:
        text: $analyze

    - type: parallel
      id: candidates
      inputs:
        question: $analyze
      settle: all
      max_concurrency: 2
      branches:
        left:
          output: string
          result:
            return: $question
        right:
          output: string
          result:
            return: fallback

    - type: switch
      id: selected
      inputs:
        candidates: $candidates
      output: string
      cases:
        - id: left_nonempty
          when:
            cel: "scope.candidates.left != ''"
          result:
            return: $candidates.left
      default:
        id: fallback
        result:
          return: $candidates.right
  result:
    return:
      answer: $selected
"#;

#[derive(Debug, Clone, Copy)]
struct GoldenChatModel;

#[async_trait]
impl ChatModel for GoldenChatModel {
    fn capabilities(&self) -> std::collections::BTreeSet<ModelCapability> {
        std::collections::BTreeSet::new()
    }

    fn validate_parameters(
        &self,
        parameters: &Value,
    ) -> Result<(), insight_agent_platform::dsl::CompileError> {
        if parameters.is_object() {
            Ok(())
        } else {
            Err(insight_agent_platform::dsl::CompileError::new(
                "GOLDEN_MODEL_PARAMETERS_INVALID",
                "golden model parameters must be an object",
            ))
        }
    }

    async fn stream_chat(&self, _request: ChatRequest) -> Result<ChatStream, RunError> {
        panic!("golden IR compilation must not invoke the model")
    }
}

#[test]
fn normalized_region_ssa_has_a_stable_test_owned_serialization() {
    let mut models = ModelRegistry::default();
    models.register("golden_chat", GoldenChatModel).unwrap();
    let mut actions = ActionRegistry::default();
    actions.register(TextMetricsAction).unwrap();
    let compiler = WorkflowCompiler::new(models, actions);
    let root = Path::new(env!("CARGO_MANIFEST_DIR"));
    let first = compiler.compile_source(root, SOURCE).unwrap();
    let second = compiler.compile_source(root, SOURCE).unwrap();
    let first = serde_json::to_string_pretty(&workflow_snapshot(&first.ir)).unwrap();
    let second = serde_json::to_string_pretty(&workflow_snapshot(&second.ir)).unwrap();

    assert_eq!(
        first, second,
        "repeated lowering must normalize identically"
    );
    let golden = root.join("tests/fixtures/vnext_normalized_ir.golden.json");
    if std::env::var_os("UPDATE_GOLDEN").is_some() {
        std::fs::write(&golden, format!("{first}\n")).unwrap();
    }
    assert_eq!(first, std::fs::read_to_string(golden).unwrap().trim_end());
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
            (name.as_str().to_string(), json!({
                "provenance": template_provenance_snapshot(&prompt.provenance),
                "profile": template_profile(prompt.profile_version),
                "source": prompt.compiled.source(),
                "slots": prompt.compiled.slots().iter().map(Identifier::as_str).collect::<Vec<_>>(),
            }))
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
        OperationKind::Call(call) => call_snapshot(call),
        OperationKind::Parallel(parallel) => parallel_snapshot(parallel),
        OperationKind::Branch(branch) => branch_snapshot(branch),
        OperationKind::Phi(phi) => phi_snapshot(phi),
    }
}

fn call_snapshot(call: &Call) -> Value {
    json!({
        "kind": "call",
        "target": call.target.operation_type(),
        "inputs": value_id_identifier_map(&call.inputs),
        "plan": call_plan_snapshot(&call.plan),
    })
}

fn call_plan_snapshot(plan: &CallPlan) -> Value {
    match plan {
        CallPlan::Llm(plan) => json!({
            "kind": "llm",
            "model": plan.model.as_str(),
            "local_inputs": value_id_identifier_map(&plan.local_inputs),
            "message_sources": plan.message_sources.iter().map(message_source_snapshot).collect::<Vec<_>>(),
            "templates": Value::Object(plan.templates.iter().map(|(id, template)| {
                (id.as_str().to_string(), planned_template_snapshot(template))
            }).collect()),
            "parameters": plan.parameters.value(),
            "response": response_snapshot(&plan.response),
            "output_contract": contract_snapshot(&plan.output_contract),
            "capabilities": plan.capabilities.iter().map(model_capability).collect::<Vec<_>>(),
            "limits": {
                "max_messages": plan.limits.max_messages,
                "max_message_bytes": plan.limits.max_message_bytes,
                "max_image_url_bytes": plan.limits.max_image_url_bytes,
                "max_request_bytes": plan.limits.max_request_bytes,
                "max_template_context_bytes": plan.limits.max_template_context_bytes,
                "max_template_output_bytes": plan.limits.max_template_output_bytes,
            },
        }),
        CallPlan::Action(plan) => json!({
            "kind": "action",
            "identity": {
                "id": plan.action_id,
                "version": plan.descriptor_version.to_string(),
                "descriptor_hash": plan.descriptor_hash,
            },
            "input_object": value_id(&plan.input_object),
            "input_contract": contract_snapshot(&plan.input_contract),
            "output_contract": contract_snapshot(&plan.output_contract),
        }),
    }
}

fn message_source_snapshot(source: &MessageSourcePlan) -> Value {
    match source {
        MessageSourcePlan::Authored { role, content } => json!({
            "kind": "authored",
            "role": planned_role(*role),
            "content": content.iter().map(content_atom_snapshot).collect::<Vec<_>>(),
        }),
        MessageSourcePlan::Dynamic {
            source,
            value,
            proven_shape,
        } => json!({
            "kind": "dynamic",
            "source": source.as_str(),
            "value": value_id(value),
            "proof": {"requires_vision": proven_shape.requires_vision},
        }),
    }
}

fn planned_role(role: PlannedRole) -> &'static str {
    match role {
        PlannedRole::System => "system",
        PlannedRole::User => "user",
        PlannedRole::Assistant => "assistant",
    }
}

fn content_atom_snapshot(atom: &CompiledContentAtom) -> Value {
    match atom {
        CompiledContentAtom::Template {
            template_id,
            bindings,
        } => json!({
            "kind": "template",
            "template_id": template_id.as_str(),
            "bindings": value_id_identifier_map(bindings),
        }),
        CompiledContentAtom::RuntimeText { value } => json!({
            "kind": "runtime_text",
            "value": value_id(value),
        }),
        CompiledContentAtom::Image { value } => json!({
            "kind": "image",
            "value": value_id(value),
        }),
    }
}

fn planned_template_snapshot(template: &PlannedTemplate) -> Value {
    json!({
        "provenance": template_provenance_snapshot(&template.provenance),
        "profile": template_profile(template.profile_version),
        "source": template.compiled.source(),
        "slots": template.compiled.slots().iter().map(Identifier::as_str).collect::<Vec<_>>(),
        "slot_signature": Value::Object(template.slot_signature.iter().map(|(name, value_type)| {
            (name.as_str().to_string(), value_type_snapshot(value_type))
        }).collect()),
    })
}

fn template_provenance_snapshot(provenance: &TemplateProvenance) -> Value {
    match provenance {
        TemplateProvenance::Catalog {
            prompt_id,
            asset_hash,
        } => json!({
            "kind": "catalog",
            "prompt_id": prompt_id.as_str(),
            "asset_hash": asset_hash,
        }),
        TemplateProvenance::Inline {
            dsl_path,
            source_hash,
        } => json!({
            "kind": "inline",
            "dsl_path": dsl_path.to_string(),
            "source_hash": source_hash,
        }),
    }
}

fn template_profile(profile: TemplateProfileVersion) -> &'static str {
    profile.as_str()
}

fn response_snapshot(response: &ValidatedResponseContract) -> Value {
    match response {
        ValidatedResponseContract::Text => json!({"format": "text"}),
        ValidatedResponseContract::Json { data } => json!({
            "format": "json",
            "data": contract_snapshot(data),
        }),
    }
}

fn model_capability(capability: &ModelCapability) -> &'static str {
    match capability {
        ModelCapability::JsonObjectOutput => "json_object_output",
        ModelCapability::JsonSchemaOutput => "json_schema_output",
        ModelCapability::Vision => "vision",
    }
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
