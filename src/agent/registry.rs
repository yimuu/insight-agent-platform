use std::collections::BTreeMap;

use crate::{agent::loader::LoadedAgent, error::AppError};

#[derive(Debug, Clone)]
pub struct AgentRegistry {
    agents: BTreeMap<String, LoadedAgent>,
}

impl AgentRegistry {
    pub fn new(agents: Vec<LoadedAgent>) -> Result<Self, AppError> {
        let mut by_id = BTreeMap::new();

        for agent in agents {
            let id = agent.config.id.clone();
            if by_id.insert(id.clone(), agent).is_some() {
                return Err(AppError::Config(format!("duplicate agent id '{id}'")));
            }
        }

        Ok(Self { agents: by_id })
    }

    pub fn list(&self) -> impl Iterator<Item = &LoadedAgent> {
        self.agents.values()
    }

    pub fn get(&self, id: &str) -> Option<&LoadedAgent> {
        self.agents.get(id)
    }
}
