use std::{collections::BTreeSet, sync::Arc};

use async_trait::async_trait;
use serde::Deserialize;
use serde_json::{json, Value};

use crate::{
    dsl::{
        compiled::{
            CompiledNode, NextPolicy, NodeCompilation, NodeControl, NodeEnvelopeRules, NodeOutcome,
            NodeTransition,
        },
        compiler::{CompileContext, TemplateProgram},
        CompileError,
    },
    nodes::{
        registry::{NodeExecutor, NodeType},
        template::CompiledTemplateValue,
    },
    outcome::{EndOutcomeKind, RunOutput, TerminalOutcome, WorkflowError},
    runtime::{ExecutionControl, RunContext, RunError},
};

#[derive(Debug, Deserialize)]
#[serde(tag = "outcome", rename_all = "snake_case", deny_unknown_fields)]
enum EndConfig {
    Success {
        content: Option<TemplateSource>,
        format: Option<OutputFormat>,
        data: Option<Value>,
    },
    Failure {
        code: String,
        message: String,
    },
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
enum CompiledEnd {
    Success {
        content: Option<TemplateProgram>,
        format: Option<OutputFormat>,
        data: Option<CompiledTemplateValue>,
    },
    Failure(WorkflowError),
}

#[derive(Debug, Clone, Copy)]
pub struct EndNode;

#[derive(Debug, Clone, Copy)]
pub struct BranchEndNode;

impl NodeType for EndNode {
    fn kind(&self) -> &'static str {
        "core.end"
    }

    fn compile(
        &self,
        node_id: &str,
        config: Value,
        context: &mut CompileContext<'_>,
    ) -> Result<NodeCompilation, CompileError> {
        compile_end(self.kind(), node_id, config, context, |outcome| {
            NodeControl::End { outcome }
        })
    }
}

impl NodeType for BranchEndNode {
    fn kind(&self) -> &'static str {
        "core.branch_end"
    }

    fn compile(
        &self,
        node_id: &str,
        config: Value,
        context: &mut CompileContext<'_>,
    ) -> Result<NodeCompilation, CompileError> {
        compile_end(self.kind(), node_id, config, context, |outcome| {
            NodeControl::BranchEnd { outcome }
        })
    }
}

fn compile_end(
    kind: &str,
    node_id: &str,
    config: Value,
    context: &mut CompileContext<'_>,
    control: impl FnOnce(EndOutcomeKind) -> NodeControl,
) -> Result<NodeCompilation, CompileError> {
    let config: EndConfig = serde_json::from_value(config).map_err(|error| {
        CompileError::new(
            "NODE_CONFIG_INVALID",
            format!("invalid {kind} config for node '{node_id}': {error}"),
        )
    })?;

    let (compiled, references, outcome) = match config {
        EndConfig::Success {
            content,
            format,
            data,
        } => {
            if content.is_none() && data.is_none() {
                return Err(CompileError::new(
                    "END_VALUE_REQUIRED",
                    format!("end node '{node_id}' requires content or data for success"),
                ));
            }
            if content.is_some() && format.is_none() {
                return Err(CompileError::new(
                    "END_FORMAT_REQUIRED",
                    format!("end node '{node_id}' requires format when content is present"),
                ));
            }
            if content.is_none() && format.is_some() {
                return Err(CompileError::new(
                    "END_FORMAT_WITHOUT_CONTENT",
                    format!("end node '{node_id}' cannot define format without content"),
                ));
            }

            let content = content
                .map(|source| context.compile_inline_template(node_id, "content", &source.template))
                .transpose()?;
            let data = data
                .map(|value| CompiledTemplateValue::compile(value, node_id, "data", context))
                .transpose()?;
            let mut references = BTreeSet::new();
            if let Some(content) = &content {
                references.extend(content.references.iter().cloned());
            }
            if let Some(data) = &data {
                references.extend(data.references());
            }
            (
                CompiledEnd::Success {
                    content,
                    format,
                    data,
                },
                references,
                EndOutcomeKind::Success,
            )
        }
        EndConfig::Failure { code, message } => {
            if !valid_workflow_code(&code) {
                return Err(CompileError::new(
                    "END_FAILURE_CODE_INVALID",
                    format!("end node '{node_id}' has an invalid workflow failure code"),
                ));
            }
            if !valid_workflow_message(&message) {
                return Err(CompileError::new(
                    "END_FAILURE_MESSAGE_INVALID",
                    format!("end node '{node_id}' has an invalid workflow failure message"),
                ));
            }
            (
                CompiledEnd::Failure(WorkflowError { code, message }),
                BTreeSet::new(),
                EndOutcomeKind::Failure,
            )
        }
    };

    Ok(NodeCompilation {
        body: Arc::new(compiled),
        edges: Vec::new(),
        references,
        control: control(outcome),
        envelope: NodeEnvelopeRules {
            next: NextPolicy::Forbidden,
            allows_content_emit: false,
        },
    })
}

fn valid_workflow_code(code: &str) -> bool {
    let Some(suffix) = code.strip_prefix("WORKFLOW_") else {
        return false;
    };
    let mut chars = suffix.chars();
    matches!(chars.next(), Some('A'..='Z'))
        && chars.all(|ch| ch.is_ascii_uppercase() || ch.is_ascii_digit() || ch == '_')
        && code.len() <= 64
}

fn valid_workflow_message(message: &str) -> bool {
    !message.trim().is_empty()
        && message.len() <= 256
        && !message.chars().any(char::is_control)
        && !message.contains("{{")
        && !message.contains("}}")
}

fn render_run_output(
    content: &Option<TemplateProgram>,
    format: Option<OutputFormat>,
    data: Option<&CompiledTemplateValue>,
    context: &RunContext,
) -> Result<RunOutput, RunError> {
    let template_data = context.template_data();
    let content = content
        .as_ref()
        .map(|template| {
            context
                .templates()
                .render(&template.name, &template_data)
                .map_err(|error| {
                    RunError::new(
                        "TEMPLATE_RENDER_FAILED",
                        format!("failed to render end template '{}': {error}", template.name),
                    )
                })
        })
        .transpose()?;
    let data = data
        .map(|data| data.render(context, &template_data))
        .transpose()?
        .unwrap_or(Value::Null);
    Ok(RunOutput {
        content,
        format: format.map(|format| format.as_str().to_string()),
        data,
    })
}

#[async_trait]
impl NodeExecutor for EndNode {
    async fn execute(
        &self,
        node: &CompiledNode,
        context: &RunContext,
        control: &ExecutionControl,
    ) -> Result<NodeOutcome, RunError> {
        execute_end(node, context, control)
    }
}

#[async_trait]
impl NodeExecutor for BranchEndNode {
    async fn execute(
        &self,
        node: &CompiledNode,
        context: &RunContext,
        control: &ExecutionControl,
    ) -> Result<NodeOutcome, RunError> {
        execute_end(node, context, control)
    }
}

fn execute_end(
    node: &CompiledNode,
    context: &RunContext,
    control: &ExecutionControl,
) -> Result<NodeOutcome, RunError> {
    if let Some(reason) = control.stop_reason() {
        return Err(RunError::stopped(reason));
    }
    match node.body::<CompiledEnd>()? {
        CompiledEnd::Success {
            content,
            format,
            data,
        } => {
            let output = render_run_output(content, *format, data.as_ref(), context)?;
            Ok(NodeOutcome {
                output: json!({"outcome":"success", "output":&output}),
                transition: NodeTransition::End(TerminalOutcome::Success { output }),
            })
        }
        CompiledEnd::Failure(error) => Ok(NodeOutcome {
            output: json!({"outcome":"failure", "error":{
                "kind":"workflow", "code":&error.code, "message":&error.message
            }}),
            transition: NodeTransition::End(TerminalOutcome::Failure {
                error: error.clone(),
            }),
        }),
    }
}
