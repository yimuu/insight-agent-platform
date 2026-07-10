use std::{collections::BTreeSet, sync::Arc};

use async_trait::async_trait;
use serde::Deserialize;
use serde_json::{json, Value};

use crate::{
    dsl::{
        compiled::{
            CompiledNode, NextPolicy, NodeCompilation, NodeEnvelopeRules, NodeOutcome,
            NodeTransition, RunOutput,
        },
        compiler::{CompileContext, TemplateProgram},
        CompileError,
    },
    nodes::{
        registry::{NodeExecutor, NodeType},
        template::CompiledTemplateValue,
    },
    runtime::{ExecutionControl, RunContext, RunError},
};

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct OutputConfig {
    content: Option<TemplateSource>,
    format: Option<OutputFormat>,
    data: Option<Value>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct TemplateSource {
    template: String,
}

#[derive(Debug, Clone, Copy, Deserialize)]
#[serde(rename_all = "snake_case")]
enum OutputFormat {
    Text,
    Markdown,
}

impl OutputFormat {
    fn as_str(self) -> &'static str {
        match self {
            Self::Text => "text",
            Self::Markdown => "markdown",
        }
    }
}

#[derive(Debug)]
struct CompiledOutput {
    content: Option<TemplateProgram>,
    format: Option<OutputFormat>,
    data: Option<CompiledTemplateValue>,
}

#[derive(Debug, Clone, Copy)]
pub struct OutputNode;

impl NodeType for OutputNode {
    fn kind(&self) -> &'static str {
        "core.output"
    }

    fn compile(
        &self,
        node_id: &str,
        config: Value,
        context: &mut CompileContext<'_>,
    ) -> Result<NodeCompilation, CompileError> {
        let config: OutputConfig = serde_json::from_value(config).map_err(|error| {
            CompileError::new(
                "NODE_CONFIG_INVALID",
                format!("invalid core.output config for node '{node_id}': {error}"),
            )
        })?;
        if config.content.is_none() && config.data.is_none() {
            return Err(CompileError::new(
                "OUTPUT_VALUE_REQUIRED",
                format!("output node '{node_id}' requires content or data"),
            ));
        }
        if config.content.is_some() && config.format.is_none() {
            return Err(CompileError::new(
                "OUTPUT_FORMAT_REQUIRED",
                format!("output node '{node_id}' requires format when content is present"),
            ));
        }
        if config.content.is_none() && config.format.is_some() {
            return Err(CompileError::new(
                "OUTPUT_FORMAT_WITHOUT_CONTENT",
                format!("output node '{node_id}' cannot define format without content"),
            ));
        }

        let content = config
            .content
            .map(|source| context.compile_inline_template(node_id, "content", &source.template))
            .transpose()?;
        let data = config
            .data
            .map(|value| CompiledTemplateValue::compile(value, node_id, "data", context))
            .transpose()?;
        let mut references = BTreeSet::new();
        if let Some(content) = &content {
            references.extend(content.references.iter().cloned());
        }
        if let Some(data) = &data {
            references.extend(data.references());
        }

        Ok(NodeCompilation {
            body: Arc::new(CompiledOutput {
                content,
                format: config.format,
                data,
            }),
            edges: Vec::new(),
            references,
            terminal: true,
            envelope: NodeEnvelopeRules {
                next: NextPolicy::Forbidden,
                allows_content_emit: false,
            },
        })
    }
}

#[async_trait]
impl NodeExecutor for OutputNode {
    async fn execute(
        &self,
        node: &CompiledNode,
        context: &RunContext,
        control: &ExecutionControl,
    ) -> Result<NodeOutcome, RunError> {
        if let Some(reason) = control.stop_reason() {
            return Err(RunError::stopped(reason));
        }
        let body = node.body::<CompiledOutput>()?;
        let template_data = context.template_data();
        let content = body
            .content
            .as_ref()
            .map(|template| {
                context
                    .templates()
                    .render(&template.name, &template_data)
                    .map_err(|error| {
                        RunError::new(
                            "TEMPLATE_RENDER_FAILED",
                            format!(
                                "failed to render output template '{}': {error}",
                                template.name
                            ),
                        )
                    })
            })
            .transpose()?;
        let data = body
            .data
            .as_ref()
            .map(|data| data.render(context, &template_data))
            .transpose()?
            .unwrap_or(Value::Null);
        let output = RunOutput {
            content,
            format: body.format.map(|format| format.as_str().to_string()),
            data,
        };

        Ok(NodeOutcome {
            output: json!({
                "content": output.content,
                "format": output.format,
                "data": output.data,
            }),
            transition: NodeTransition::Complete(output),
        })
    }
}
