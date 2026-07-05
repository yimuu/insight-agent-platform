use std::{
    collections::{BTreeMap, HashSet},
    fs,
    path::{Path, PathBuf},
};

use jsonschema::JSONSchema;

use crate::{
    agent::config::{AgentConfig, StepKind},
    error::AppError,
};

#[derive(Debug, Clone)]
pub struct LoadedAgent {
    pub root: PathBuf,
    pub config: AgentConfig,
    pub prompts: BTreeMap<String, String>,
}

pub fn load_agents(root: impl AsRef<Path>) -> Result<Vec<LoadedAgent>, AppError> {
    let root = root.as_ref();
    if !root.exists() {
        return Ok(Vec::new());
    }

    let mut agents = Vec::new();
    for entry in fs::read_dir(root).map_err(|err| AppError::Config(err.to_string()))? {
        let entry = entry.map_err(|err| AppError::Config(err.to_string()))?;
        if !entry
            .file_type()
            .map_err(|err| AppError::Config(err.to_string()))?
            .is_dir()
        {
            continue;
        }

        let agent_root = entry.path();
        let config_path = agent_root.join("agent.yaml");
        if !config_path.exists() {
            continue;
        }

        agents.push(load_agent_dir(&agent_root)?);
    }

    Ok(agents)
}

fn load_agent_dir(agent_root: &Path) -> Result<LoadedAgent, AppError> {
    let yaml = fs::read_to_string(agent_root.join("agent.yaml"))
        .map_err(|err| AppError::Config(format!("failed to read agent.yaml: {err}")))?;
    let config: AgentConfig = serde_yaml::from_str(&yaml)
        .map_err(|err| AppError::Config(format!("invalid agent yaml: {err}")))?;
    validate_agent_config(agent_root, &config)?;

    let mut prompts = BTreeMap::new();
    for (name, rel_path) in &config.prompts {
        let path = resolve_inside(agent_root, rel_path)?;
        let body = fs::read_to_string(&path)
            .map_err(|err| AppError::Config(format!("failed to read prompt {name}: {err}")))?;
        prompts.insert(name.clone(), body);
    }

    Ok(LoadedAgent {
        root: agent_root.to_path_buf(),
        config,
        prompts,
    })
}

fn validate_agent_config(agent_root: &Path, config: &AgentConfig) -> Result<(), AppError> {
    JSONSchema::compile(&config.input.schema).map_err(|err| {
        AppError::Config(format!(
            "invalid input schema for agent '{}': {err}",
            config.id
        ))
    })?;

    let mut step_ids = HashSet::new();

    for step in &config.steps {
        if !step_ids.insert(step.id.clone()) {
            return Err(AppError::Config(format!("duplicate step id '{}'", step.id)));
        }

        if step.prompt_ref.is_some() && step.prompt.is_some() {
            return Err(AppError::Config(format!(
                "step '{}' prompt_ref and prompt are mutually exclusive",
                step.id
            )));
        }

        if step.system_prompt_ref.is_some() && step.system_prompt.is_some() {
            return Err(AppError::Config(format!(
                "step '{}' system_prompt_ref and system_prompt are mutually exclusive",
                step.id
            )));
        }

        if let Some(image_input) = &step.image_input {
            if step.kind != StepKind::Llm {
                return Err(AppError::Config(format!(
                    "step '{}' image_input is only supported on llm steps",
                    step.id
                )));
            }
            if image_input != "input.images" {
                return Err(AppError::Config(format!(
                    "step '{}' unsupported image_input '{}'",
                    step.id, image_input
                )));
            }
        }

        match step.kind {
            StepKind::Prompt | StepKind::Text => {
                if step.prompt_source().is_none() {
                    let kind = match step.kind {
                        StepKind::Prompt => "prompt",
                        StepKind::Text => "text",
                        _ => unreachable!(),
                    };
                    return Err(AppError::Config(format!(
                        "step '{}' type '{kind}' requires prompt or prompt_ref",
                        step.id,
                    )));
                }
            }
            StepKind::Llm => {
                if step.prompt_source().is_none() {
                    return Err(AppError::Config(format!(
                        "step '{}' type 'llm' requires prompt or prompt_ref",
                        step.id
                    )));
                }
            }
            StepKind::Tool => {
                if step.tool.is_none() {
                    return Err(AppError::Config(format!(
                        "step '{}' type 'tool' requires tool",
                        step.id
                    )));
                }
            }
            StepKind::Code => {
                if step.handler.is_none() {
                    return Err(AppError::Config(format!(
                        "step '{}' type 'code' requires handler",
                        step.id
                    )));
                }
            }
            StepKind::Condition => {
                if step.cases.is_empty() && step.default.is_none() {
                    return Err(AppError::Config(format!(
                        "step '{}' type 'condition' requires cases or default",
                        step.id
                    )));
                }
            }
        }

        for case in &step.cases {
            if case.when.trim().is_empty() {
                return Err(AppError::Config(format!(
                    "step '{}' condition case requires when",
                    step.id
                )));
            }
            if !step_ids.contains(&case.goto) && !config.steps.iter().any(|s| s.id == case.goto) {
                return Err(AppError::Config(format!(
                    "step '{}' condition case has unknown goto '{}'",
                    step.id, case.goto
                )));
            }
        }

        if let Some(default) = &step.default {
            if !step_ids.contains(default) && !config.steps.iter().any(|s| s.id == *default) {
                return Err(AppError::Config(format!(
                    "step '{}' condition has unknown default '{}'",
                    step.id, default
                )));
            }
        }

        if let Some(prompt_ref) = &step.prompt_ref {
            if !config.prompts.contains_key(prompt_ref) {
                return Err(AppError::Config(format!(
                    "step '{}' unknown prompt_ref '{}'",
                    step.id, prompt_ref
                )));
            }
        }

        if let Some(prompt_ref) = &step.system_prompt_ref {
            if !config.prompts.contains_key(prompt_ref) {
                return Err(AppError::Config(format!(
                    "step '{}' unknown system_prompt_ref '{}'",
                    step.id, prompt_ref
                )));
            }
        }
    }

    for rel_path in config.prompts.values() {
        resolve_inside(agent_root, rel_path)?;
    }

    Ok(())
}

fn resolve_inside(agent_root: &Path, rel_path: &str) -> Result<PathBuf, AppError> {
    let root = agent_root
        .canonicalize()
        .map_err(|err| AppError::Config(format!("invalid agent directory: {err}")))?;
    let path = agent_root.join(rel_path);
    let canonical = path
        .canonicalize()
        .map_err(|err| AppError::Config(format!("invalid prompt path '{rel_path}': {err}")))?;

    if !canonical.starts_with(&root) {
        return Err(AppError::Config(format!(
            "prompt path '{rel_path}' must stay inside agent directory"
        )));
    }

    Ok(canonical)
}
