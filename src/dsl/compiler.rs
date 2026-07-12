use std::{
    collections::{BTreeMap, BTreeSet},
    fs,
    path::Path,
    sync::Arc,
    time::Duration,
};

use handlebars::{no_escape, Handlebars, Template};
use sha2::{Digest, Sha256};

use crate::{
    nodes::registry::NodeTypeRegistry,
    resources::{actions::ActionRegistry, models::ModelRegistry},
    schema::compile_schema,
};

use super::{
    compiled::{CompiledAgent, CompiledNode, NextPolicy},
    graph::{validate_graph_structure, validate_references},
    parse_raw_agent,
    plan::compile_execution_plan,
    references::{extract_handlebars_references, handlebars_static_text, validate_node_id},
    CompileError, EmitPolicy,
};

#[derive(Debug, Clone)]
pub struct TemplateProgram {
    pub name: String,
    pub references: BTreeSet<String>,
    pub static_value: Option<String>,
}

pub struct CompileContext<'a> {
    models: &'a ModelRegistry,
    actions: &'a ActionRegistry,
    templates: Handlebars<'static>,
    prompts: BTreeMap<String, String>,
    template_index: usize,
}

impl<'a> CompileContext<'a> {
    pub fn new(models: &'a ModelRegistry, actions: &'a ActionRegistry) -> Self {
        let mut templates = Handlebars::new();
        templates.set_strict_mode(true);
        templates.register_escape_fn(no_escape);
        Self {
            models,
            actions,
            templates,
            prompts: BTreeMap::new(),
            template_index: 0,
        }
    }

    fn for_agent(
        root: &Path,
        prompt_paths: &BTreeMap<String, String>,
        models: &'a ModelRegistry,
        actions: &'a ActionRegistry,
    ) -> Result<Self, CompileError> {
        let mut context = Self::new(models, actions);
        let canonical_root = root.canonicalize().map_err(|error| {
            CompileError::new(
                "AGENT_PATH_INVALID",
                format!("invalid agent directory '{}': {error}", root.display()),
            )
        })?;
        for (name, relative) in prompt_paths {
            let path = root.join(relative);
            let canonical = path.canonicalize().map_err(|error| {
                CompileError::new(
                    "PROMPT_PATH_INVALID",
                    format!("invalid prompt path '{relative}': {error}"),
                )
            })?;
            if !canonical.starts_with(&canonical_root) {
                return Err(CompileError::new(
                    "PROMPT_PATH_ESCAPE",
                    format!("prompt path '{relative}' must stay inside the agent directory"),
                ));
            }
            let body = fs::read_to_string(&canonical).map_err(|error| {
                CompileError::new(
                    "PROMPT_READ_FAILED",
                    format!("failed to read prompt '{name}': {error}"),
                )
            })?;
            context.prompts.insert(name.clone(), body);
        }
        Ok(context)
    }

    pub fn models(&self) -> &ModelRegistry {
        self.models
    }

    pub fn actions(&self) -> &ActionRegistry {
        self.actions
    }

    pub fn templates(&self) -> &Handlebars<'static> {
        &self.templates
    }

    pub fn compile_inline_template(
        &mut self,
        owner: &str,
        field: &str,
        source: &str,
    ) -> Result<TemplateProgram, CompileError> {
        self.template_index += 1;
        let name = format!("{owner}.{field}.{}", self.template_index);
        let template = Template::compile(source).map_err(|error| {
            CompileError::new(
                "TEMPLATE_INVALID",
                format!("invalid template '{owner}.{field}': {error}"),
            )
        })?;
        let references = extract_handlebars_references(&template, owner, field)?;
        let static_value = handlebars_static_text(&template);
        self.templates.register_template(&name, template);
        Ok(TemplateProgram {
            name,
            references,
            static_value,
        })
    }

    pub fn compile_prompt_ref(
        &mut self,
        owner: &str,
        field: &str,
        prompt_ref: &str,
    ) -> Result<TemplateProgram, CompileError> {
        let source = self.prompts.get(prompt_ref).cloned().ok_or_else(|| {
            CompileError::new(
                "PROMPT_REF_NOT_FOUND",
                format!("prompt reference '{prompt_ref}' is not defined"),
            )
        })?;
        self.compile_inline_template(owner, field, &source)
    }

    fn resolved_prompts(&self) -> &BTreeMap<String, String> {
        &self.prompts
    }

    pub fn into_templates(self) -> Handlebars<'static> {
        self.templates
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CompileLimits {
    pub max_fork_branches: usize,
}

pub struct AgentCompiler {
    node_types: NodeTypeRegistry,
    models: ModelRegistry,
    actions: ActionRegistry,
    default_node_timeout: Duration,
    limits: CompileLimits,
}

impl AgentCompiler {
    pub fn new(
        node_types: NodeTypeRegistry,
        models: ModelRegistry,
        actions: ActionRegistry,
        default_node_timeout: Duration,
        limits: CompileLimits,
    ) -> Self {
        Self {
            node_types,
            models,
            actions,
            default_node_timeout,
            limits,
        }
    }

    pub fn limits(&self) -> CompileLimits {
        self.limits
    }

    pub fn compile_dir(&self, root: &Path) -> Result<CompiledAgent, CompileError> {
        let yaml_path = root.join("agent.yaml");
        let yaml = fs::read_to_string(&yaml_path).map_err(|error| {
            CompileError::new(
                "AGENT_READ_FAILED",
                format!("failed to read '{}': {error}", yaml_path.display()),
            )
        })?;
        let raw = parse_raw_agent(&yaml)?;
        for node_id in raw.nodes.keys() {
            validate_node_id(node_id)?;
        }
        let input_schema = Arc::new(compile_schema(&raw.input.schema).map_err(|error| {
            CompileError::new(
                "INPUT_SCHEMA_INVALID",
                format!("agent '{}' input schema is invalid: {error}", raw.id),
            )
        })?);
        let mut context =
            CompileContext::for_agent(root, &raw.prompts, &self.models, &self.actions)?;
        let mut nodes = BTreeMap::new();

        for (node_id, raw_node) in &raw.nodes {
            let node_type = self.node_types.resolve(&raw_node.kind)?;
            let compilation = node_type.compile(node_id, raw_node.config.clone(), &mut context)?;
            let mut edges = compilation.edges;
            match compilation.envelope.next {
                NextPolicy::Required => {
                    let next = raw_node.next.as_ref().ok_or_else(|| {
                        CompileError::new(
                            "NODE_NEXT_REQUIRED",
                            format!("node '{node_id}' requires next"),
                        )
                    })?;
                    edges.push(next.clone());
                }
                NextPolicy::Forbidden if raw_node.next.is_some() => {
                    return Err(CompileError::new(
                        "NODE_NEXT_FORBIDDEN",
                        format!("node '{node_id}' does not allow next"),
                    ));
                }
                NextPolicy::Forbidden => {}
            }
            if raw_node.emit == EmitPolicy::Content && !compilation.envelope.allows_content_emit {
                return Err(CompileError::new(
                    "NODE_EMIT_UNSUPPORTED",
                    format!("node '{node_id}' does not support emit: content"),
                ));
            }
            nodes.insert(
                node_id.clone(),
                CompiledNode {
                    id: node_id.clone(),
                    kind: raw_node.kind.clone(),
                    next: raw_node.next.clone(),
                    emit: raw_node.emit,
                    timeout: raw_node
                        .timeout
                        .map(|timeout| timeout.get())
                        .unwrap_or(self.default_node_timeout),
                    body: compilation.body,
                    edges,
                    references: compilation.references,
                    terminal: compilation.terminal,
                    control: compilation.control,
                },
            );
        }

        validate_graph_structure(&raw.entry, &nodes)?;
        let execution_plan = compile_execution_plan(&raw.entry, &nodes, self.limits)?;
        validate_references(&raw.entry, &nodes, &execution_plan)?;
        let version_hash = agent_hash(&raw, context.resolved_prompts())?;
        Ok(CompiledAgent {
            id: raw.id,
            name: raw.name,
            description: raw.description,
            version_hash,
            input_schema,
            entry: raw.entry,
            nodes,
            execution_plan,
            templates: Arc::new(context.into_templates()),
        })
    }
}

fn agent_hash(
    raw: &super::RawAgent,
    prompts: &BTreeMap<String, String>,
) -> Result<String, CompileError> {
    let mut hasher = Sha256::new();
    let raw = serde_json::to_vec(raw).map_err(|error| {
        CompileError::new(
            "AGENT_HASH_FAILED",
            format!("failed to normalize agent config: {error}"),
        )
    })?;
    hasher.update(raw);
    for (name, body) in prompts {
        hasher.update(name.as_bytes());
        hasher.update([0]);
        hasher.update(body.as_bytes());
        hasher.update([0]);
    }
    Ok(format!("sha256:{:x}", hasher.finalize()))
}
