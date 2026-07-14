use std::sync::Arc;

use async_trait::async_trait;
use serde::Deserialize;
use serde_json::Value;
use tokio::time::sleep;

use crate::{
    dsl::{
        compiled::{
            CompiledNode, NextPolicy, NodeCompilation, NodeControl, NodeEnvelopeRules, NodeOutcome,
            NodeTransition,
        },
        compiler::CompileContext,
        CompileError, EmitPolicy,
    },
    nodes::{
        registry::{NodeExecutor, NodeType},
        template::CompiledTemplateValue,
    },
    resources::actions::{ActionContext, RegisteredAction},
    runtime::{ExecutionControl, RunContext, RunError},
};

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct ActionConfig {
    action: String,
    input: Value,
}

struct CompiledAction {
    action: Arc<RegisteredAction>,
    input: CompiledTemplateValue,
}

#[derive(Debug, Clone, Copy)]
pub struct ActionNode;

impl NodeType for ActionNode {
    fn kind(&self) -> &'static str {
        "core.action"
    }

    fn compile(
        &self,
        node_id: &str,
        config: Value,
        context: &mut CompileContext<'_>,
    ) -> Result<NodeCompilation, CompileError> {
        let config: ActionConfig = serde_json::from_value(config).map_err(|error| {
            CompileError::new(
                "NODE_CONFIG_INVALID",
                format!("invalid core.action config for node '{node_id}': {error}"),
            )
        })?;
        let action = context.actions().resolve(&config.action)?;
        let input = CompiledTemplateValue::compile(config.input, node_id, "input", context)?;
        if let Some(static_input) = input.static_value() {
            action
                .validate_input(&static_input)
                .map_err(|error| CompileError::new(error.code(), error.message().to_string()))?;
        }
        let references = input.references();
        let allows_content_emit = action.descriptor().streams_content;

        Ok(NodeCompilation {
            body: Arc::new(CompiledAction { action, input }),
            edges: Vec::new(),
            references,
            control: NodeControl::Ordinary,
            envelope: NodeEnvelopeRules {
                next: NextPolicy::Required,
                allows_content_emit,
            },
        })
    }
}

#[async_trait]
impl NodeExecutor for ActionNode {
    async fn execute(
        &self,
        node: &CompiledNode,
        context: &RunContext,
        control: &ExecutionControl,
    ) -> Result<NodeOutcome, RunError> {
        if let Some(reason) = control.stop_reason() {
            return Err(RunError::stopped(reason));
        }
        let body = node.body::<CompiledAction>()?;
        let input = body.input.render(context, &context.template_data())?;
        let action_control = control
            .clone()
            .with_content_enabled(node.emit == EmitPolicy::Content);
        let action_context = ActionContext::new(
            context.metadata().run_id.clone(),
            node.id.clone(),
            action_control,
        );
        let call = body.action.call(input, action_context);
        tokio::pin!(call);
        let output = tokio::select! {
            result = &mut call => result?,
            _ = control.stopped() => return Err(stopped_error(control)),
            _ = sleep(control.remaining()) => return Err(RunError::timeout()),
        };

        Ok(NodeOutcome {
            output,
            transition: NodeTransition::Next,
        })
    }
}

fn stopped_error(control: &ExecutionControl) -> RunError {
    control
        .stop_reason()
        .map(RunError::stopped)
        .unwrap_or_else(|| RunError::new("RUN_STOPPED", "run stopped"))
}
