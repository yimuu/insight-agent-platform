use std::{collections::BTreeMap, collections::BTreeSet, path::Path, sync::Arc};

use crate::dsl::{
    vnext::compiler::{CompiledWorkflow, WorkflowCompiler},
    CompileError,
};

/// Immutable catalog of fully compiled vNext workflows.
///
/// A compiled workflow owns both its verified IR and the exact leaf-operation
/// registry used to compile it, so production code cannot accidentally pair an
/// IR with a different executor set.
#[derive(Clone, Default)]
pub struct AgentCatalog {
    workflows: BTreeMap<String, Arc<CompiledWorkflow>>,
}

impl AgentCatalog {
    pub fn new(workflows: Vec<Arc<CompiledWorkflow>>) -> Result<Self, CompileError> {
        let mut catalog = Self::default();
        for workflow in workflows {
            let id = workflow.ir.metadata.id.as_str().to_string();
            if catalog.workflows.insert(id.clone(), workflow).is_some() {
                return Err(CompileError::new(
                    "DUPLICATE_AGENT",
                    format!("compiled agent '{id}' is already registered"),
                ));
            }
        }
        Ok(catalog)
    }

    pub fn get(&self, agent_id: &str) -> Option<Arc<CompiledWorkflow>> {
        self.workflows.get(agent_id).cloned()
    }

    pub fn list(&self) -> impl Iterator<Item = &Arc<CompiledWorkflow>> {
        self.workflows.values()
    }

    pub fn ids(&self) -> impl Iterator<Item = &str> {
        self.workflows.keys().map(String::as_str)
    }
}

pub fn compile_enabled_agents(
    directory: &Path,
    enabled: &BTreeSet<String>,
    compiler: &WorkflowCompiler,
) -> Result<AgentCatalog, CompileError> {
    if !directory.is_dir() {
        return Err(CompileError::new(
            "AGENTS_DIRECTORY_INVALID",
            format!("agents directory '{}' does not exist", directory.display()),
        ));
    }
    let mut workflows = Vec::with_capacity(enabled.len());
    for agent_id in enabled {
        validate_agent_directory_name(agent_id)?;
        let workflow = compiler.compile_dir(&directory.join(agent_id))?;
        let declared_id = workflow.ir.metadata.id.as_str();
        if declared_id != agent_id {
            return Err(CompileError::new(
                "AGENT_ID_MISMATCH",
                format!("enabled agent directory '{agent_id}' declares id '{declared_id}'"),
            ));
        }
        workflows.push(Arc::new(workflow));
    }
    AgentCatalog::new(workflows)
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
