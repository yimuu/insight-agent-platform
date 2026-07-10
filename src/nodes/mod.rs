pub mod action;
pub mod chat;
pub mod condition;
pub mod fork;
pub mod join;
pub mod output;
pub mod registry;
pub mod template;

use crate::dsl::CompileError;

use self::{
    action::ActionNode,
    chat::ChatNode,
    condition::ConditionNode,
    fork::ForkNode,
    join::JoinNode,
    output::OutputNode,
    registry::{NodeExecutorRegistry, NodeTypeRegistry},
    template::TemplateNode,
};

pub fn default_node_registries() -> Result<(NodeTypeRegistry, NodeExecutorRegistry), CompileError> {
    let mut types = NodeTypeRegistry::default();
    types.register(TemplateNode)?;
    types.register(ChatNode)?;
    types.register(ActionNode)?;
    types.register(ConditionNode)?;
    types.register(OutputNode)?;
    types.register(ForkNode)?;
    types.register(JoinNode)?;

    let mut executors = NodeExecutorRegistry::default();
    executors.register(TemplateNode)?;
    executors.register(ChatNode)?;
    executors.register(ActionNode)?;
    executors.register(ConditionNode)?;
    executors.register(OutputNode)?;
    executors.register(ForkNode)?;
    executors.register(JoinNode)?;

    Ok((types, executors))
}
