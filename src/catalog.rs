use std::{collections::BTreeSet, path::Path, sync::Arc, time::Duration};

use crate::{
    dsl::{compiler::AgentCompiler, CompileError},
    nodes::registry::NodeTypeRegistry,
    resources::{actions::ActionRegistry, models::ModelRegistry},
    runtime::CompiledAgentRegistry,
};

pub fn compile_enabled_agents(
    directory: &Path,
    enabled: &BTreeSet<String>,
    node_types: NodeTypeRegistry,
    models: ModelRegistry,
    actions: ActionRegistry,
    default_node_timeout: Duration,
) -> Result<CompiledAgentRegistry, CompileError> {
    if !directory.is_dir() {
        return Err(CompileError::new(
            "AGENTS_DIRECTORY_INVALID",
            format!("agents directory '{}' does not exist", directory.display()),
        ));
    }
    let compiler = AgentCompiler::new(node_types, models, actions, default_node_timeout);
    let mut agents = Vec::with_capacity(enabled.len());
    for agent_id in enabled {
        validate_agent_directory_name(agent_id)?;
        let agent = compiler.compile_dir(&directory.join(agent_id))?;
        if agent.id != *agent_id {
            return Err(CompileError::new(
                "AGENT_ID_MISMATCH",
                format!(
                    "enabled agent directory '{agent_id}' declares id '{}'",
                    agent.id
                ),
            ));
        }
        agents.push(Arc::new(agent));
    }
    CompiledAgentRegistry::new(agents)
}

fn validate_agent_directory_name(agent_id: &str) -> Result<(), CompileError> {
    let path = Path::new(agent_id);
    let mut components = path.components();
    let valid = matches!(components.next(), Some(std::path::Component::Normal(value)) if value == agent_id)
        && components.next().is_none();
    if !valid {
        return Err(CompileError::new(
            "AGENT_ID_INVALID",
            format!("enabled agent id '{agent_id}' must be one directory name"),
        ));
    }
    Ok(())
}
