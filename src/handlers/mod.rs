pub mod examples;

use std::collections::{BTreeMap, BTreeSet};

use crate::{
    agent::{config::StepKind, loader::LoadedAgent},
    code::registry::CodeRegistry,
    error::AppError,
};

type RegisterHandler = fn(&mut CodeRegistry);

#[derive(Default)]
pub struct CodeHandlerCatalog {
    handlers: BTreeMap<&'static str, RegisterHandler>,
}

impl CodeHandlerCatalog {
    pub fn register(&mut self, name: &'static str, register: RegisterHandler) {
        self.handlers.insert(name, register);
    }

    pub fn build_registry_for<'a, I>(&self, names: I) -> Result<CodeRegistry, AppError>
    where
        I: IntoIterator<Item = &'a str>,
    {
        let mut registry = CodeRegistry::default();
        for name in names {
            let register = self.handlers.get(name).ok_or_else(|| {
                AppError::Config(format!("code handler '{name}' is not registered"))
            })?;
            register(&mut registry);
        }
        Ok(registry)
    }

    pub fn build_all(&self) -> CodeRegistry {
        let mut registry = CodeRegistry::default();
        for register in self.handlers.values() {
            register(&mut registry);
        }
        registry
    }
}

pub fn default_code_catalog() -> CodeHandlerCatalog {
    let mut catalog = CodeHandlerCatalog::default();
    examples::register(&mut catalog);
    catalog
}

pub fn code_registry_for_agents(agents: &[LoadedAgent]) -> Result<CodeRegistry, AppError> {
    let handler_names = agents
        .iter()
        .flat_map(|agent| agent.config.steps.iter())
        .filter(|step| step.kind == StepKind::Code)
        .filter_map(|step| step.handler.as_deref())
        .collect::<BTreeSet<_>>();

    default_code_catalog().build_registry_for(handler_names)
}

pub fn default_code_registry() -> CodeRegistry {
    default_code_catalog().build_all()
}
