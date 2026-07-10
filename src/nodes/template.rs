use std::{collections::BTreeMap, sync::Arc};

use async_trait::async_trait;
use serde::Deserialize;
use serde_json::Value;

use crate::{
    dsl::{
        compiled::{
            CompiledNode, NextPolicy, NodeCompilation, NodeControl, NodeEnvelopeRules, NodeOutcome,
            NodeTransition,
        },
        compiler::{CompileContext, TemplateProgram},
        CompileError, EmitPolicy,
    },
    nodes::registry::{NodeExecutor, NodeType},
    runtime::{ExecutionControl, RunContext, RunError},
};

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct TemplateConfig {
    value: Value,
}

#[derive(Debug)]
pub enum CompiledTemplateValue {
    String(TemplateProgram),
    Array(Vec<CompiledTemplateValue>),
    Object(BTreeMap<String, CompiledTemplateValue>),
    Literal(Value),
}

impl CompiledTemplateValue {
    pub(crate) fn compile(
        value: Value,
        node_id: &str,
        path: &str,
        context: &mut CompileContext<'_>,
    ) -> Result<Self, CompileError> {
        match value {
            Value::String(source) => context
                .compile_inline_template(node_id, path, &source)
                .map(Self::String),
            Value::Array(values) => values
                .into_iter()
                .enumerate()
                .map(|(index, value)| {
                    Self::compile(value, node_id, &format!("{path}[{index}]"), context)
                })
                .collect::<Result<Vec<_>, _>>()
                .map(Self::Array),
            Value::Object(values) => values
                .into_iter()
                .map(|(key, value)| {
                    let compiled =
                        Self::compile(value, node_id, &format!("{path}.{key}"), context)?;
                    Ok((key, compiled))
                })
                .collect::<Result<BTreeMap<_, _>, _>>()
                .map(Self::Object),
            literal => Ok(Self::Literal(literal)),
        }
    }

    pub(crate) fn render(&self, context: &RunContext, data: &Value) -> Result<Value, RunError> {
        match self {
            Self::String(program) => context
                .templates()
                .render(&program.name, data)
                .map(Value::String)
                .map_err(|error| {
                    RunError::new(
                        "TEMPLATE_RENDER_FAILED",
                        format!("failed to render template '{}': {error}", program.name),
                    )
                }),
            Self::Array(values) => values
                .iter()
                .map(|value| value.render(context, data))
                .collect::<Result<Vec<_>, _>>()
                .map(Value::Array),
            Self::Object(values) => values
                .iter()
                .map(|(key, value)| Ok((key.clone(), value.render(context, data)?)))
                .collect::<Result<serde_json::Map<_, _>, _>>()
                .map(Value::Object),
            Self::Literal(value) => Ok(value.clone()),
        }
    }

    pub(crate) fn references(&self) -> std::collections::BTreeSet<String> {
        match self {
            Self::String(program) => program.references.clone(),
            Self::Array(values) => values
                .iter()
                .flat_map(Self::references)
                .collect::<std::collections::BTreeSet<_>>(),
            Self::Object(values) => values
                .values()
                .flat_map(Self::references)
                .collect::<std::collections::BTreeSet<_>>(),
            Self::Literal(_) => std::collections::BTreeSet::new(),
        }
    }

    fn is_string(&self) -> bool {
        matches!(self, Self::String(_))
    }
}

#[derive(Debug, Clone, Copy)]
pub struct TemplateNode;

impl NodeType for TemplateNode {
    fn kind(&self) -> &'static str {
        "core.template"
    }

    fn compile(
        &self,
        node_id: &str,
        config: Value,
        context: &mut CompileContext<'_>,
    ) -> Result<NodeCompilation, CompileError> {
        let config: TemplateConfig = serde_json::from_value(config).map_err(|error| {
            CompileError::new(
                "NODE_CONFIG_INVALID",
                format!("invalid core.template config for node '{node_id}': {error}"),
            )
        })?;
        let value = CompiledTemplateValue::compile(config.value, node_id, "value", context)?;
        let references = value.references();
        let allows_content_emit = value.is_string();
        Ok(NodeCompilation {
            body: Arc::new(value),
            edges: Vec::new(),
            references,
            terminal: false,
            control: NodeControl::Ordinary,
            envelope: NodeEnvelopeRules {
                next: NextPolicy::Required,
                allows_content_emit,
            },
        })
    }
}

#[async_trait]
impl NodeExecutor for TemplateNode {
    async fn execute(
        &self,
        node: &CompiledNode,
        context: &RunContext,
        control: &ExecutionControl,
    ) -> Result<NodeOutcome, RunError> {
        if let Some(reason) = control.stop_reason() {
            return Err(RunError::stopped(reason));
        }
        let value = node
            .body::<CompiledTemplateValue>()?
            .render(context, &context.template_data())?;
        if node.emit == EmitPolicy::Content {
            let content = value.as_str().ok_or_else(|| {
                RunError::new(
                    "NODE_OUTPUT_TYPE_MISMATCH",
                    format!("node '{}' must render a string for emit: content", node.id),
                )
            })?;
            control.emit_content(content).await?;
        }
        Ok(NodeOutcome {
            output: value,
            transition: NodeTransition::Next,
        })
    }
}
