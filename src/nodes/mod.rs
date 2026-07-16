pub mod action;
pub mod chat;
pub mod condition;
pub mod end;
pub mod fork;
pub mod join;
pub mod registry;
pub mod select;
pub mod template;

use crate::dsl::CompileError;

use self::{
    action::ActionNode,
    chat::ChatNode,
    condition::ConditionNode,
    end::{BranchEndNode, EndNode},
    fork::ForkNode,
    join::JoinNode,
    registry::{NodeExecutorRegistry, NodeTypeRegistry},
    select::SelectNode,
    template::TemplateNode,
};

pub fn default_node_registries() -> Result<(NodeTypeRegistry, NodeExecutorRegistry), CompileError> {
    let mut types = NodeTypeRegistry::default();
    types.register(TemplateNode)?;
    types.register(ChatNode)?;
    types.register(ActionNode)?;
    types.register(ConditionNode)?;
    types.register(BranchEndNode)?;
    types.register(EndNode)?;
    types.register(ForkNode)?;
    types.register(JoinNode)?;
    types.register(SelectNode)?;

    let mut executors = NodeExecutorRegistry::default();
    executors.register(TemplateNode)?;
    executors.register(ChatNode)?;
    executors.register(ActionNode)?;
    executors.register(ConditionNode)?;
    executors.register(BranchEndNode)?;
    executors.register(EndNode)?;
    executors.register(ForkNode)?;
    executors.register(JoinNode)?;
    executors.register(SelectNode)?;

    Ok((types, executors))
}
