//! Owning data-only prompt workflow authoring contract; lowered to the current RuntimePlan.
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::BTreeMap;

pub const WORKFLOW_VERSION: u32 = 1;
pub const MAX_WORKFLOW_STEPS: usize = 32;
pub const MAX_TEMPLATE_BYTES: usize = 65_536;
pub const MAX_RENDERED_PROMPT_BYTES: usize = 262_144;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PromptWorkflowV1 {
    pub version: u32,
    pub steps: Vec<PromptStepV1>,
    pub output: BTreeMap<String, String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PromptStepV1 {
    pub id: String,
    pub prompt: String,
    /// Inline public object schema, validated with the existing ClosedJsonSchema owner.
    pub output: Value,
}

use super::{AgentCompilerError, ModelLoopCompilerLimits};
use insight_platform_contracts::{
    ClosedJsonSchema, ClosedJsonValue, ClosedValueSchema, Sha256Digest, MAX_MODEL_JSON_BYTES,
};
use insight_platform_plan::{
    DataPortKey, ExactDataPortRef, ExpressionFieldName, ExpressionLimits, PortAssignment,
    RuntimeNode, RuntimePlan, TypedExpressionProgram, TypedInstruction,
};
use serde_json::json;
use std::collections::BTreeSet;

fn invalid() -> AgentCompilerError {
    AgentCompilerError::manifest("workflow template, reference, schema or capacity is invalid")
}

pub fn parse(bytes: &[u8]) -> Result<PromptWorkflowV1, AgentCompilerError> {
    if bytes.is_empty() || bytes.len() > super::MAX_AGENT_MANIFEST_BYTES {
        return Err(invalid());
    }
    let text = std::str::from_utf8(bytes).map_err(|_| invalid())?;
    if text.contains('\r') || text.contains('\0') {
        return Err(invalid());
    }
    super::validate_yaml_subset(text)?;
    let value: PromptWorkflowV1 = yaml_serde::from_str(text).map_err(|_| invalid())?;
    if value.version != WORKFLOW_VERSION
        || value.steps.is_empty()
        || value.steps.len() > MAX_WORKFLOW_STEPS
    {
        return Err(invalid());
    }
    let mut ids = BTreeSet::new();
    for step in &value.steps {
        if !identifier(&step.id) || !ids.insert(&step.id) {
            return Err(invalid());
        }
        template_shape(&step.prompt)?;
        ClosedJsonSchema::build(step.output.clone()).map_err(|_| invalid())?;
    }
    for template in value.output.values() {
        template_shape(template)?;
    }
    Ok(value)
}
fn identifier(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 64
        && value.as_bytes()[0].is_ascii_alphabetic()
        && value
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b == b'_')
}
fn template_shape(value: &str) -> Result<(), AgentCompilerError> {
    if value.is_empty() || value.len() > MAX_TEMPLATE_BYTES || value.contains('\0') {
        return Err(invalid());
    }
    super::reject_sensitive_literal(value)
}
#[derive(Clone, Copy, Debug)]
struct Bound {
    bytes: u64,
    chars: u64,
}
impl Bound {
    fn add(self, right: Self) -> Result<Self, AgentCompilerError> {
        let result = Self {
            bytes: self.bytes.checked_add(right.bytes).ok_or_else(invalid)?,
            chars: self.chars.checked_add(right.chars).ok_or_else(invalid)?,
        };
        if result.bytes > MAX_RENDERED_PROMPT_BYTES as u64 {
            return Err(invalid());
        }
        Ok(result)
    }
}
fn string_bound(value: &Value) -> Result<Bound, AgentCompilerError> {
    if value.get("type").and_then(Value::as_str) != Some("string") {
        return Err(invalid());
    }
    let chars = value
        .get("maxLength")
        .and_then(Value::as_u64)
        .ok_or_else(invalid)?;
    let bytes = chars.checked_mul(4).ok_or_else(invalid)?;
    Ok(Bound {
        chars,
        bytes: value
            .get("x-platform-max-bytes")
            .and_then(Value::as_u64)
            .map_or(bytes, |v| v.min(bytes)),
    })
}
fn property<'a>(schema: &'a Value, path: &[&str]) -> Result<&'a Value, AgentCompilerError> {
    let mut value = schema;
    for field in path {
        if field.is_empty()
            || value.get("type").and_then(Value::as_str) != Some("object")
            || !value
                .get("required")
                .and_then(Value::as_array)
                .is_some_and(|items| items.iter().any(|v| v.as_str() == Some(*field)))
        {
            return Err(invalid());
        }
        value = value
            .get("properties")
            .and_then(|v| v.get(*field))
            .ok_or_else(invalid)?;
    }
    Ok(value)
}
fn port(id: &str, schema: &ClosedJsonSchema) -> ExactDataPortRef {
    ExactDataPortRef::NodeOutput {
        producer_node_id: super::node_key(id),
        port_id: DataPortKey::new("response".into()).expect("static port"),
        schema_digest: schema.canonical_digest.clone(),
    }
}
struct Builder<'a> {
    input: &'a ClosedJsonSchema,
    previous: BTreeMap<String, ClosedJsonSchema>,
    documents: BTreeMap<Sha256Digest, ClosedValueSchema>,
}
impl<'a> Builder<'a> {
    fn new(
        input: &'a ClosedJsonSchema,
        output: &ClosedJsonSchema,
    ) -> Result<Self, AgentCompilerError> {
        input.validate().map_err(|_| invalid())?;
        output.validate().map_err(|_| invalid())?;
        let mut result = Self {
            input,
            previous: BTreeMap::new(),
            documents: BTreeMap::new(),
        };
        result.schema(input)?;
        result.schema(output)?;
        Ok(result)
    }
    fn schema(&mut self, value: &ClosedJsonSchema) -> Result<(), AgentCompilerError> {
        self.documents.insert(
            value.canonical_digest.clone(),
            ClosedValueSchema::try_from(value.clone()).map_err(|_| invalid())?,
        );
        Ok(())
    }
    fn literal(&mut self, text: &str) -> Result<TypedInstruction, AgentCompilerError> {
        let schema = ClosedValueSchema::build(json!({"$schema":"https://json-schema.org/draft/2020-12/schema","type":"string","minLength":0,"maxLength":text.chars().count().max(1),"x-platform-max-bytes":text.len().max(1)})).map_err(|_| invalid())?;
        let value = ClosedJsonValue::build(schema.canonical_digest.clone(), json!(text))
            .map_err(|_| invalid())?;
        self.documents
            .insert(schema.canonical_digest.clone(), schema);
        Ok(TypedInstruction::Literal { value })
    }
    fn reference(
        &self,
        text: &str,
    ) -> Result<(ExactDataPortRef, Vec<ExpressionFieldName>, Bound), AgentCompilerError> {
        let parts = text.trim().split('.').collect::<Vec<_>>();
        let (source, schema, fields) = match parts.as_slice() {
            ["input", rest @ ..] if !rest.is_empty() => (
                ExactDataPortRef::RunInput {
                    schema_digest: self.input.canonical_digest.clone(),
                },
                &self.input.schema,
                rest,
            ),
            ["nodes", id, "output", rest @ ..] if !rest.is_empty() => {
                let schema = self.previous.get(*id).ok_or_else(invalid)?;
                (port(&format!("step.{id}"), schema), &schema.schema, rest)
            }
            _ => return Err(invalid()),
        };
        let bound = string_bound(property(schema, fields)?)?;
        let fields = fields
            .iter()
            .map(|s| {
                if s.chars().any(char::is_whitespace) || s.contains(['{', '}']) {
                    return Err(invalid());
                }
                ExpressionFieldName::new((*s).into()).map_err(|_| invalid())
            })
            .collect::<Result<Vec<_>, _>>()?;
        Ok((source, fields, bound))
    }
    fn template(
        &mut self,
        text: &str,
    ) -> Result<(Vec<TypedInstruction>, BTreeSet<ExactDataPortRef>, Bound), AgentCompilerError>
    {
        template_shape(text)?;
        let mut instructions = vec![self.literal("")?];
        let mut ports = BTreeSet::new();
        let mut bound = Bound { bytes: 0, chars: 0 };
        let mut tail = text;
        while !tail.is_empty() {
            if let Some(begin) = tail.find("{{") {
                let literal = &tail[..begin];
                if literal.contains("}}") {
                    return Err(invalid());
                }
                if !literal.is_empty() {
                    instructions.push(self.literal(literal)?);
                    instructions.push(TypedInstruction::StringConcat);
                    bound = bound.add(Bound {
                        bytes: literal.len() as u64,
                        chars: literal.chars().count() as u64,
                    })?;
                }
                let rest = &tail[begin + 2..];
                let end = rest.find("}}").ok_or_else(invalid)?;
                let (source, fields, size) = self.reference(&rest[..end])?;
                ports.insert(source.clone());
                instructions.push(TypedInstruction::LoadPort { port: source });
                instructions.extend(
                    fields
                        .into_iter()
                        .map(|field| TypedInstruction::GetField { field }),
                );
                instructions.push(TypedInstruction::StringConcat);
                bound = bound.add(size)?;
                tail = &rest[end + 2..];
            } else {
                if tail.contains("}}") {
                    return Err(invalid());
                }
                instructions.push(self.literal(tail)?);
                instructions.push(TypedInstruction::StringConcat);
                bound = bound.add(Bound {
                    bytes: tail.len() as u64,
                    chars: tail.chars().count() as u64,
                })?;
                break;
            }
        }
        Ok((instructions, ports, bound))
    }
    fn object(
        &mut self,
        templates: &BTreeMap<String, String>,
        schema: &ClosedJsonSchema,
    ) -> Result<TypedExpressionProgram, AgentCompilerError> {
        let properties = schema
            .schema
            .get("properties")
            .and_then(Value::as_object)
            .ok_or_else(invalid)?;
        let required = schema
            .schema
            .get("required")
            .and_then(Value::as_array)
            .ok_or_else(invalid)?;
        if properties.len() != templates.len()
            || required.len() != templates.len()
            || templates.is_empty()
        {
            return Err(invalid());
        }
        let mut code = Vec::new();
        let mut ports = BTreeSet::new();
        let mut fields = Vec::new();
        let mut json_bytes = 2u64;
        for (name, template) in templates {
            let target = property(&schema.schema, &[name.as_str()])?;
            let limit = string_bound(target)?;
            let (instructions, inputs, bound) = self.template(template)?;
            if bound.bytes > limit.bytes || bound.chars > limit.chars {
                return Err(invalid());
            }
            json_bytes = json_bytes
                .checked_add((serde_json::to_string(name).map_err(|_| invalid())?.len() as u64) + 4)
                .and_then(|v| v.checked_add(bound.bytes.min(bound.chars).checked_mul(6)?))
                .ok_or_else(invalid)?;
            if json_bytes > MAX_MODEL_JSON_BYTES as u64 {
                return Err(invalid());
            }
            code.extend(instructions);
            ports.extend(inputs);
            fields.push(ExpressionFieldName::new(name.clone()).map_err(|_| invalid())?);
        }
        code.push(TypedInstruction::MakeObject {
            ordered_fields: fields,
        });
        TypedExpressionProgram::build(
            ports.into_iter().collect(),
            code,
            schema.canonical_digest.clone(),
            ExpressionLimits::ABSOLUTE,
        )
        .map_err(|_| invalid())
    }
    fn prompt(
        &mut self,
        text: &str,
    ) -> Result<(ClosedJsonSchema, TypedExpressionProgram), AgentCompilerError> {
        let (_, _, bound) = self.template(text)?;
        let schema = ClosedJsonSchema::build(json!({"$schema":"https://json-schema.org/draft/2020-12/schema","type":"object","additionalProperties":false,"properties":{"prompt":{"type":"string","minLength":0,"maxLength":bound.chars.max(1),"x-platform-max-bytes":bound.bytes.max(1)}},"required":["prompt"]})).map_err(|_| invalid())?;
        self.schema(&schema)?;
        let expression = self.object(&BTreeMap::from([("prompt".into(), text.into())]), &schema)?;
        Ok((schema, expression))
    }
}

pub(crate) fn lower(
    bytes: &[u8],
    input: &ClosedJsonSchema,
    output: &ClosedJsonSchema,
    contract: &Sha256Digest,
    requirement: &Sha256Digest,
    limits: ModelLoopCompilerLimits,
) -> Result<RuntimePlan, AgentCompilerError> {
    let workflow = parse(bytes)?;
    let mut builder = Builder::new(input, output)?;
    let mut plan = super::model_chat_plan(
        contract,
        &input.canonical_digest,
        &output.canonical_digest,
        requirement,
        limits,
    );
    plan.nodes.clear();
    plan.nodes.insert(
        super::node_key("start"),
        RuntimeNode::Start {
            next: super::node_key(&format!("prompt.{}", workflow.steps[0].id)),
        },
    );
    for (index, step) in workflow.steps.iter().enumerate() {
        let prompt_id = format!("prompt.{}", step.id);
        let model_id = format!("step.{}", step.id);
        let (prompt_schema, expression) = builder.prompt(&step.prompt).map_err(|error| {
            AgentCompilerError::manifest(format!(
                "workflow /steps/{index}/prompt: {}",
                error.detail()
            ))
        })?;
        let response_schema =
            ClosedJsonSchema::build(step.output.clone()).map_err(|_| invalid())?;
        builder.schema(&response_schema)?;
        plan.nodes.insert(
            super::node_key(&prompt_id),
            RuntimeNode::Compute {
                assignments: vec![PortAssignment {
                    output_port: port(&prompt_id, &prompt_schema),
                    expression,
                }],
                next: super::node_key(&model_id),
            },
        );
        let next = workflow
            .steps
            .get(index + 1)
            .map(|s| format!("prompt.{}", s.id))
            .unwrap_or_else(|| "result".into());
        plan.nodes.insert(
            super::node_key(&model_id),
            RuntimeNode::ModelLoop {
                model_slot_id: super::PRIMARY_MODEL_SLOT_ID.into(),
                skill_slot_ids: vec![],
                capability_slot_ids: vec![],
                input: port(&prompt_id, &prompt_schema),
                model_route: None,
                output: port(&model_id, &response_schema),
                maximum_rounds: limits.maximum_rounds,
                maximum_capability_calls: limits.maximum_capability_calls,
                maximum_parallel_calls_per_round: limits.maximum_parallel_calls_per_round,
                token_budget: limits.token_budget,
                resume: super::node_key(&next),
            },
        );
        builder.previous.insert(step.id.clone(), response_schema);
    }
    let expression = builder.object(&workflow.output, output).map_err(|error| {
        AgentCompilerError::manifest(format!("workflow /output: {}", error.detail()))
    })?;
    plan.nodes.insert(
        super::node_key("result"),
        RuntimeNode::Compute {
            assignments: vec![PortAssignment {
                output_port: port("result", output),
                expression,
            }],
            next: super::node_key("finish"),
        },
    );
    plan.nodes.insert(
        super::node_key("finish"),
        RuntimeNode::Return {
            value: port("result", output),
        },
    );
    plan.schema_documents = builder.documents;
    Ok(plan)
}
pub(crate) fn lower_chat(
    prompt: &str,
    input: &ClosedJsonSchema,
    output: &ClosedJsonSchema,
    contract: &Sha256Digest,
    requirement: &Sha256Digest,
    limits: ModelLoopCompilerLimits,
) -> Result<RuntimePlan, AgentCompilerError> {
    let mut builder = Builder::new(input, output)?;
    let (schema, expression) = builder.prompt(prompt).map_err(|error| {
        AgentCompilerError::manifest(format!("agent /spec/prompt: {}", error.detail()))
    })?;
    let mut plan = super::model_chat_plan(
        contract,
        &input.canonical_digest,
        &output.canonical_digest,
        requirement,
        limits,
    );
    plan.nodes.insert(
        super::node_key("start"),
        RuntimeNode::Start {
            next: super::node_key("prompt.model"),
        },
    );
    plan.nodes.insert(
        super::node_key("prompt.model"),
        RuntimeNode::Compute {
            assignments: vec![PortAssignment {
                output_port: port("prompt.model", &schema),
                expression,
            }],
            next: super::node_key("model"),
        },
    );
    if let Some(RuntimeNode::ModelLoop { input, .. }) =
        plan.nodes.get_mut(&super::node_key("model"))
    {
        *input = port("prompt.model", &schema);
    }
    plan.schema_documents = builder.documents;
    Ok(plan)
}

#[cfg(test)]
mod tests {
    use super::*;
    fn schema(name: &str, size: usize) -> ClosedJsonSchema {
        ClosedJsonSchema::build(json!({"$schema":"https://json-schema.org/draft/2020-12/schema","type":"object","additionalProperties":false,"properties":{name:{"type":"string","minLength":0,"maxLength":size,"x-platform-max-bytes":size}},"required":[name]})).unwrap()
    }
    fn limits() -> ModelLoopCompilerLimits {
        ModelLoopCompilerLimits {
            maximum_rounds: 1,
            maximum_capability_calls: 0,
            maximum_parallel_calls_per_round: 0,
            token_budget: 200,
        }
    }
    fn digest() -> Sha256Digest {
        format!("sha256:{}", "a".repeat(64)).parse().unwrap()
    }
    fn fixture() -> (Vec<u8>, ClosedJsonSchema, ClosedJsonSchema) {
        let input = schema("question", 32);
        let output = schema("answer", 128);
        let data=serde_json::to_vec(&json!({"version":1,"steps":[{"id":"first","prompt":"Question: {{ input.question }}","output":schema("answer",32).schema},{"id":"second","prompt":"Review {{ nodes.first.output.answer }} for {{ input.question }}","output":schema("answer",64).schema}],"output":{"answer":"Final: {{ nodes.second.output.answer }}"}})).unwrap();
        (data, input, output)
    }
    fn evaluate(
        plan: &RuntimePlan,
        id: &str,
        values: &BTreeMap<ExactDataPortRef, ClosedJsonValue>,
    ) -> ClosedJsonValue {
        let RuntimeNode::Compute { assignments, .. } = &plan.nodes[&super::super::node_key(id)]
        else {
            panic!("expected compute")
        };
        let expression = &assignments[0].expression;
        let selected = values
            .iter()
            .filter(|(port, _)| expression.input_ports.contains(port))
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect();
        let value = expression
            .evaluate(&selected, ExpressionLimits::ABSOLUTE)
            .unwrap();
        plan.schema_documents[&value.schema_digest]
            .validate_literal_shape(&value.value)
            .unwrap();
        value
    }
    #[test]
    fn workflow_actual_expressions_use_prior_outputs_once_and_preserve_step_limits() {
        let (bytes, input, output) = fixture();
        let plan = lower(&bytes, &input, &output, &digest(), &digest(), limits()).unwrap();
        let mut values = BTreeMap::from([(
            ExactDataPortRef::RunInput {
                schema_digest: input.canonical_digest.clone(),
            },
            ClosedJsonValue::build(
                input.canonical_digest.clone(),
                json!({"question":"中文 {{ input.other }}"}),
            )
            .unwrap(),
        )]);
        assert_eq!(
            evaluate(&plan, "prompt.first", &values).value,
            json!({"prompt":"Question: 中文 {{ input.other }}"})
        );
        let first = schema("answer", 32);
        values.insert(
            port("step.first", &first),
            ClosedJsonValue::build(
                first.canonical_digest.clone(),
                json!({"answer":"actual first output"}),
            )
            .unwrap(),
        );
        assert_eq!(
            evaluate(&plan, "prompt.second", &values).value,
            json!({"prompt":"Review actual first output for 中文 {{ input.other }}"})
        );
        let second = schema("answer", 64);
        values.insert(
            port("step.second", &second),
            ClosedJsonValue::build(
                second.canonical_digest.clone(),
                json!({"answer":"actual second output"}),
            )
            .unwrap(),
        );
        assert_eq!(
            evaluate(&plan, "result", &values).value,
            json!({"answer":"Final: actual second output"})
        );
        for id in ["step.first", "step.second"] {
            let RuntimeNode::ModelLoop {
                token_budget,
                maximum_rounds,
                ..
            } = &plan.nodes[&super::super::node_key(id)]
            else {
                panic!()
            };
            assert_eq!((*token_budget, *maximum_rounds), (200, 1));
        }
    }
    #[test]
    fn workflow_rejects_forward_optional_nonstring_and_malformed_references() {
        let (bytes, input, output) = fixture();
        for prompt in [
            "{{ nodes.second.output.answer }}",
            "{{ nodes.first.output.answer }}",
            "{{ input.missing }}",
            "{{ input.question",
            "input.question }}",
            "{{ input.question + 1 }}",
            "{{ {{ input.question }}",
            "{{ input }}",
        ] {
            let mut data: Value = serde_json::from_slice(&bytes).unwrap();
            data["steps"][0]["prompt"] = json!(prompt);
            assert!(
                lower(
                    &serde_json::to_vec(&data).unwrap(),
                    &input,
                    &output,
                    &digest(),
                    &digest(),
                    limits()
                )
                .is_err(),
                "{prompt}"
            );
        }
        let mut optional = input.schema.clone();
        optional["required"] = json!([]);
        let optional = ClosedJsonSchema::build(optional).unwrap();
        assert!(lower(&bytes, &optional, &output, &digest(), &digest(), limits()).is_err());
        let mut number = input.schema.clone();
        number["properties"]["question"] = json!({"type":"integer","minimum":0,"maximum":20});
        let number = ClosedJsonSchema::build(number).unwrap();
        assert!(lower(&bytes, &number, &output, &digest(), &digest(), limits()).is_err());
    }
    #[test]
    fn workflow_nested_required_strings_and_unicode_bounds() {
        let leaf = schema("text", 16);
        let input=ClosedJsonSchema::build(json!({"$schema":"https://json-schema.org/draft/2020-12/schema","type":"object","additionalProperties":false,"properties":{"nested":leaf.schema},"required":["nested"]})).unwrap();
        let output = schema("answer", 32);
        let plan = lower_chat(
            "中{{ input.nested.text }}",
            &input,
            &output,
            &digest(),
            &digest(),
            limits(),
        )
        .unwrap();
        let values = BTreeMap::from([(
            ExactDataPortRef::RunInput {
                schema_digest: input.canonical_digest.clone(),
            },
            ClosedJsonValue::build(
                input.canonical_digest.clone(),
                json!({"nested":{"text":"文"}}),
            )
            .unwrap(),
        )]);
        assert_eq!(
            evaluate(&plan, "prompt.model", &values).value,
            json!({"prompt":"中文"})
        );
        assert!(lower_chat(
            "{{ nodes.first.output.answer }}",
            &input,
            &output,
            &digest(),
            &digest(),
            limits()
        )
        .is_err());
    }
    #[test]
    fn workflow_rejects_shape_collisions_and_capacity_without_truncating() {
        let (bytes, input, output) = fixture();
        let base: Value = serde_json::from_slice(&bytes).unwrap();
        for id in ["first", "a.b", "_bad", "1bad", ""] {
            let mut data = base.clone();
            data["steps"][1]["id"] = json!(id);
            assert!(parse(&serde_json::to_vec(&data).unwrap()).is_err());
        }
        for field in ["extra", "wrong"] {
            let mut data = base.clone();
            data["output"][field] = json!("x");
            assert!(lower(
                &serde_json::to_vec(&data).unwrap(),
                &input,
                &output,
                &digest(),
                &digest(),
                limits()
            )
            .is_err());
        }
        assert!(lower_chat(
            &"x".repeat(MAX_TEMPLATE_BYTES + 1),
            &input,
            &output,
            &digest(),
            &digest(),
            limits()
        )
        .is_err());
        let large = schema("question", MAX_RENDERED_PROMPT_BYTES);
        assert!(lower_chat(
            "x{{ input.question }}",
            &large,
            &output,
            &digest(),
            &digest(),
            limits()
        )
        .is_err());
        assert!(
            lower_chat(
                "{{ input.question }}",
                &large,
                &output,
                &digest(),
                &digest(),
                limits()
            )
            .is_err(),
            "JSON escaping may exceed closed value byte limit"
        );
        assert!(parse(b"{\"version\":1,\"version\":1,\"steps\":[],\"output\":{}}").is_err());
        assert!(parse(b"version: 1\nsteps: &a []\noutput: *a\n").is_err());
    }
}
