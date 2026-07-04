# Config-Driven Agent Platform Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build the first runnable Rust service for platform-owned, config-driven agents with SSE streaming runs.

**Architecture:** The service uses `axum` for HTTP, loads built-in agents from `agents/*/agent.yaml`, executes fixed step types in an event-driven runner, and streams `RunEvent` values as SSE. Model calls go through an OpenAI-compatible client behind a trait, and tools are resolved from an internal registry.

**Tech Stack:** Rust 2021, `tokio`, `axum`, `serde`, `serde_yaml`, `serde_json`, `reqwest`, `async-trait`, `thiserror`, `uuid`, `chrono`, `chrono-tz`, `handlebars`, `jsonschema`, `tokio-stream`.

## Global Constraints

- Agents are implemented inside the platform and defined by local configuration.
- External clients do not create or modify agents through API calls.
- API exposes agent discovery and agent run usage only.
- Agent config lives at `agents/<agent_id>/agent.yaml`.
- A single agent can define multiple named prompts.
- Long prompts should live in Markdown files under the agent directory.
- Short glue prompts can be inline YAML.
- `prompt_ref` and `prompt` are mutually exclusive.
- `system_prompt_ref` and `system_prompt` are mutually exclusive.
- Prompt file paths must resolve inside the agent directory.
- Model provider defaults come from `OPENAI_API_KEY`, `OPENAI_BASE_URL`, and `OPENAI_DEFAULT_MODEL`.
- API keys must not be stored in YAML or committed.
- First version step types are `prompt`, `llm`, and `tool`.
- First version built-in tools are `current_time` and restricted `http_get`.
- Runtime events are streamed with Server-Sent Events.
- Invalid agent configuration fails startup.
- Invalid request input returns HTTP `400` before opening SSE.
- Runtime failures after SSE starts emit an `error` event and close the stream.

---

## File Structure

- `Cargo.toml`: crate metadata and dependencies.
- `.gitignore`: Rust build output and local env files.
- `.env.example`: documents required environment variables without secrets.
- `agents/researcher/agent.yaml`: sample built-in agent.
- `agents/researcher/prompts/system.md`: sample system prompt.
- `agents/researcher/prompts/planner.md`: sample planning prompt.
- `agents/researcher/prompts/final.md`: sample final answer prompt.
- `src/main.rs`: process entry point, config loading, app startup.
- `src/lib.rs`: library module exports for tests.
- `src/config.rs`: platform environment config.
- `src/error.rs`: shared error types and HTTP response mapping.
- `src/api/mod.rs`: API module exports.
- `src/api/routes.rs`: axum routes and handlers.
- `src/api/sse.rs`: `RunEvent` to SSE conversion.
- `src/agent/mod.rs`: agent module exports.
- `src/agent/config.rs`: deserializable agent and step config structs.
- `src/agent/loader.rs`: agent directory loader and validation.
- `src/agent/registry.rs`: in-memory `AgentRegistry`.
- `src/engine/mod.rs`: engine module exports.
- `src/engine/context.rs`: `RunContext` and step output storage.
- `src/engine/event.rs`: `RunEvent` and event payloads.
- `src/engine/runner.rs`: step execution orchestration.
- `src/model/mod.rs`: model module exports.
- `src/model/types.rs`: chat request, message, and stream delta types.
- `src/model/openai.rs`: OpenAI-compatible streaming client.
- `src/prompt/mod.rs`: prompt module exports.
- `src/prompt/renderer.rs`: handlebars template rendering.
- `src/prompt/store.rs`: named prompt storage.
- `src/tools/mod.rs`: tool module exports.
- `src/tools/registry.rs`: `Tool` trait and registry.
- `src/tools/current_time.rs`: `current_time` tool.
- `src/tools/http_get.rs`: restricted `http_get` tool.
- `tests/agent_loader.rs`: config loading validation tests.
- `tests/prompt_renderer.rs`: template rendering tests.
- `tests/runner.rs`: runner tests with fake model and tools.
- `tests/api.rs`: HTTP and SSE route tests.

---

### Task 1: Rust Project Skeleton and Shared Types

**Files:**
- Create: `Cargo.toml`
- Create: `.gitignore`
- Create: `.env.example`
- Create: `src/lib.rs`
- Create: `src/main.rs`
- Create: `src/config.rs`
- Create: `src/error.rs`

**Interfaces:**
- Produces: `PlatformConfig::from_env() -> Result<PlatformConfig, AppError>`
- Produces: `AppError` enum used by the agent, engine, model, tool, and API modules.
- Produces: module exports for `api`, `agent`, `engine`, `model`, `prompt`, and `tools`.

- [ ] **Step 1: Create project metadata and dependency list**

Add `Cargo.toml`:

```toml
[package]
name = "insight-agent-platform"
version = "0.1.0"
edition = "2021"

[dependencies]
async-trait = "0.1"
axum = "0.7"
bytes = "1"
chrono = { version = "0.4", features = ["serde"] }
chrono-tz = "0.10"
futures = "0.3"
handlebars = "6"
jsonschema = "0.18"
reqwest = { version = "0.12", features = ["json", "stream", "rustls-tls"], default-features = false }
serde = { version = "1", features = ["derive"] }
serde_json = "1"
serde_yaml = "0.9"
thiserror = "1"
tokio = { version = "1", features = ["macros", "rt-multi-thread", "signal"] }
tokio-stream = "0.1"
tower-http = { version = "0.6", features = ["trace", "cors"] }
tracing = "0.1"
tracing-subscriber = { version = "0.3", features = ["env-filter"] }
uuid = { version = "1", features = ["v4", "serde"] }

[dev-dependencies]
tower = "0.5"
tempfile = "3"
```

Add `.gitignore`:

```gitignore
/target
.env
*.log
```

Add `.env.example`:

```text
OPENAI_API_KEY=
OPENAI_BASE_URL=https://dashscope.aliyuncs.com/compatible-mode/v1
OPENAI_DEFAULT_MODEL=qwen3.6-flash
AGENTS_DIR=agents
BIND_ADDR=127.0.0.1:3000
```

- [ ] **Step 2: Add library modules and configuration**

Add `src/lib.rs`:

```rust
pub mod agent;
pub mod api;
pub mod config;
pub mod engine;
pub mod error;
pub mod model;
pub mod prompt;
pub mod tools;
```

Add `src/config.rs`:

```rust
use std::{env, net::SocketAddr, path::PathBuf};

use crate::error::AppError;

#[derive(Debug, Clone)]
pub struct PlatformConfig {
    pub bind_addr: SocketAddr,
    pub agents_dir: PathBuf,
    pub openai_api_key: String,
    pub openai_base_url: String,
    pub openai_default_model: String,
}

impl PlatformConfig {
    pub fn from_env() -> Result<Self, AppError> {
        let bind_addr = env::var("BIND_ADDR")
            .unwrap_or_else(|_| "127.0.0.1:3000".to_string())
            .parse()
            .map_err(|err| AppError::Config(format!("invalid BIND_ADDR: {err}")))?;

        let agents_dir = env::var("AGENTS_DIR").unwrap_or_else(|_| "agents".to_string());
        let openai_api_key = env::var("OPENAI_API_KEY")
            .map_err(|_| AppError::Config("OPENAI_API_KEY is required".to_string()))?;
        let openai_base_url = env::var("OPENAI_BASE_URL")
            .unwrap_or_else(|_| "https://dashscope.aliyuncs.com/compatible-mode/v1".to_string());
        let openai_default_model =
            env::var("OPENAI_DEFAULT_MODEL").unwrap_or_else(|_| "qwen3.6-flash".to_string());

        Ok(Self {
            bind_addr,
            agents_dir: PathBuf::from(agents_dir),
            openai_api_key,
            openai_base_url,
            openai_default_model,
        })
    }
}
```

Add `src/error.rs`:

```rust
use axum::{http::StatusCode, response::{IntoResponse, Response}, Json};
use serde_json::json;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum AppError {
    #[error("configuration error: {0}")]
    Config(String),
    #[error("input error: {0}")]
    Input(String),
    #[error("run error: {0}")]
    Run(String),
    #[error("not found: {0}")]
    NotFound(String),
    #[error("upstream error: {0}")]
    Upstream(String),
}

impl IntoResponse for AppError {
    fn into_response(self) -> Response {
        let (status, code, message) = match self {
            AppError::Config(message) => (StatusCode::INTERNAL_SERVER_ERROR, "config_error", message),
            AppError::Input(message) => (StatusCode::BAD_REQUEST, "input_error", message),
            AppError::Run(message) => (StatusCode::INTERNAL_SERVER_ERROR, "run_error", message),
            AppError::NotFound(message) => (StatusCode::NOT_FOUND, "not_found", message),
            AppError::Upstream(message) => (StatusCode::BAD_GATEWAY, "upstream_error", message),
        };
        (status, Json(json!({ "error": { "code": code, "message": message } }))).into_response()
    }
}
```

Add `src/main.rs`:

```rust
use insight_agent_platform::{config::PlatformConfig, error::AppError};

#[tokio::main]
async fn main() -> Result<(), AppError> {
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .init();

    let config = PlatformConfig::from_env()?;
    tracing::info!(bind_addr = %config.bind_addr, "starting insight agent platform");
    Ok(())
}
```

- [ ] **Step 3: Verify skeleton builds**

Run:

```bash
cargo check
```

Expected: success with all modules missing errors at first because module files are not present.

- [ ] **Step 4: Add initial module files to satisfy exports**

Create module root files:

```text
src/api/mod.rs
src/agent/mod.rs
src/engine/mod.rs
src/model/mod.rs
src/prompt/mod.rs
src/tools/mod.rs
```

Each file content:

```rust
//! Module root.
```

- [ ] **Step 5: Verify skeleton compiles**

Run:

```bash
cargo check
```

Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add Cargo.toml .gitignore .env.example src
git commit -m "chore: scaffold rust service"
```

---

### Task 2: Agent Config Types and Loader

**Files:**
- Modify: `src/agent/mod.rs`
- Create: `src/agent/config.rs`
- Create: `src/agent/loader.rs`
- Create: `src/agent/registry.rs`
- Create: `tests/agent_loader.rs`

**Interfaces:**
- Consumes: `AppError`.
- Produces: `AgentConfig`, `StepConfig`, `StepKind`, `PromptSource`, `ModelConfig`.
- Produces: `load_agents(root: impl AsRef<Path>) -> Result<Vec<LoadedAgent>, AppError>`.
- Produces: `AgentRegistry::new(Vec<LoadedAgent>) -> Result<Self, AppError>`.

- [ ] **Step 1: Write failing loader tests**

Add `tests/agent_loader.rs` with tests for successful load, duplicate step IDs, missing prompt refs, mutually exclusive prompt fields, and path traversal:

```rust
use std::{fs, path::Path};

use insight_agent_platform::agent::loader::load_agents;

fn write_file(path: &Path, body: &str) {
    fs::create_dir_all(path.parent().unwrap()).unwrap();
    fs::write(path, body).unwrap();
}

#[test]
fn loads_agent_with_multiple_prompt_files() {
    let dir = tempfile::tempdir().unwrap();
    let agent = dir.path().join("researcher");
    write_file(&agent.join("prompts/system.md"), "You are helpful.");
    write_file(&agent.join("prompts/final.md"), "Answer {{ input.question }}");
    write_file(&agent.join("agent.yaml"), r#"
id: researcher
name: Researcher
description: Test agent
model:
  provider: openai_compatible
prompts:
  system: prompts/system.md
  final: prompts/final.md
input:
  schema:
    type: object
steps:
  - id: answer
    type: llm
    system_prompt_ref: system
    prompt_ref: final
"#);

    let agents = load_agents(dir.path()).unwrap();
    assert_eq!(agents.len(), 1);
    assert_eq!(agents[0].config.id, "researcher");
    assert_eq!(agents[0].prompts.get("system").unwrap(), "You are helpful.");
}

#[test]
fn rejects_duplicate_step_ids() {
    let dir = tempfile::tempdir().unwrap();
    let agent = dir.path().join("bad");
    write_file(&agent.join("agent.yaml"), r#"
id: bad
name: Bad
model:
  provider: openai_compatible
input:
  schema:
    type: object
steps:
  - id: same
    type: prompt
    prompt: one
  - id: same
    type: prompt
    prompt: two
"#);

    let err = load_agents(dir.path()).unwrap_err().to_string();
    assert!(err.contains("duplicate step id"));
}

#[test]
fn rejects_prompt_ref_and_inline_prompt_together() {
    let dir = tempfile::tempdir().unwrap();
    let agent = dir.path().join("bad");
    write_file(&agent.join("prompts/a.md"), "hello");
    write_file(&agent.join("agent.yaml"), r#"
id: bad
name: Bad
model:
  provider: openai_compatible
prompts:
  a: prompts/a.md
input:
  schema:
    type: object
steps:
  - id: answer
    type: prompt
    prompt_ref: a
    prompt: inline
"#);

    let err = load_agents(dir.path()).unwrap_err().to_string();
    assert!(err.contains("prompt_ref and prompt are mutually exclusive"));
}

#[test]
fn rejects_missing_prompt_ref() {
    let dir = tempfile::tempdir().unwrap();
    let agent = dir.path().join("bad");
    write_file(&agent.join("agent.yaml"), r#"
id: bad
name: Bad
model:
  provider: openai_compatible
input:
  schema:
    type: object
steps:
  - id: answer
    type: prompt
    prompt_ref: missing
"#);

    let err = load_agents(dir.path()).unwrap_err().to_string();
    assert!(err.contains("unknown prompt_ref"));
}

#[test]
fn rejects_prompt_path_outside_agent_directory() {
    let dir = tempfile::tempdir().unwrap();
    write_file(&dir.path().join("secret.md"), "secret");
    let agent = dir.path().join("bad");
    write_file(&agent.join("agent.yaml"), r#"
id: bad
name: Bad
model:
  provider: openai_compatible
prompts:
  secret: ../secret.md
input:
  schema:
    type: object
steps:
  - id: answer
    type: prompt
    prompt_ref: secret
"#);

    let err = load_agents(dir.path()).unwrap_err().to_string();
    assert!(err.contains("must stay inside agent directory"));
}
```

- [ ] **Step 2: Run tests and verify they fail**

Run:

```bash
cargo test --test agent_loader
```

Expected: FAIL because `agent::loader` does not exist.

- [ ] **Step 3: Implement config structs**

Update `src/agent/mod.rs`:

```rust
pub mod config;
pub mod loader;
pub mod registry;
```

Add `src/agent/config.rs`:

```rust
use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};
use serde_json::Value;

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct AgentConfig {
    pub id: String,
    pub name: String,
    #[serde(default)]
    pub description: String,
    pub model: ModelConfig,
    pub input: InputConfig,
    #[serde(default)]
    pub prompts: BTreeMap<String, String>,
    pub steps: Vec<StepConfig>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct ModelConfig {
    pub provider: String,
    #[serde(default)]
    pub model: Option<String>,
    #[serde(default)]
    pub temperature: Option<f32>,
    #[serde(default)]
    pub max_tokens: Option<u32>,
    #[serde(default)]
    pub options: Value,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct InputConfig {
    pub schema: Value,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct StepConfig {
    pub id: String,
    #[serde(rename = "type")]
    pub kind: StepKind,
    #[serde(default)]
    pub prompt_ref: Option<String>,
    #[serde(default)]
    pub prompt: Option<String>,
    #[serde(default)]
    pub system_prompt_ref: Option<String>,
    #[serde(default)]
    pub system_prompt: Option<String>,
    #[serde(default)]
    pub stream: bool,
    #[serde(default)]
    pub tool: Option<String>,
    #[serde(default)]
    pub args: Value,
}

#[derive(Debug, Clone, Copy, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum StepKind {
    Prompt,
    Llm,
    Tool,
}
```

- [ ] **Step 4: Implement loader and registry**

Add `src/agent/loader.rs`:

```rust
use std::{
    collections::{BTreeMap, HashSet},
    fs,
    path::{Path, PathBuf},
};

use crate::{agent::config::AgentConfig, error::AppError};

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
        if !entry.file_type().map_err(|err| AppError::Config(err.to_string()))?.is_dir() {
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
```

Add `src/agent/registry.rs`:

```rust
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
```

- [ ] **Step 5: Run loader tests**

Run:

```bash
cargo test --test agent_loader
```

Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add src/agent tests/agent_loader.rs
git commit -m "feat: load agent configuration"
```

---

### Task 3: Prompt Store and Renderer

**Files:**
- Modify: `src/prompt/mod.rs`
- Create: `src/prompt/store.rs`
- Create: `src/prompt/renderer.rs`
- Create: `tests/prompt_renderer.rs`

**Interfaces:**
- Consumes: `LoadedAgent.prompts`.
- Produces: `PromptStore::resolve_ref(&self, name: &str) -> Result<&str, AppError>`.
- Produces: `PromptRenderer::render(&self, template: &str, data: &serde_json::Value) -> Result<String, AppError>`.

- [ ] **Step 1: Write failing renderer tests**

Add `tests/prompt_renderer.rs`:

```rust
use serde_json::json;

use insight_agent_platform::prompt::renderer::PromptRenderer;

#[test]
fn renders_input_and_step_values() {
    let renderer = PromptRenderer::new();
    let out = renderer
        .render(
            "Question: {{ input.question }} Plan: {{ steps.plan.output }}",
            &json!({
                "input": { "question": "What is Rust?" },
                "steps": { "plan": { "output": "Explain ownership." } }
            }),
        )
        .unwrap();

    assert_eq!(out, "Question: What is Rust? Plan: Explain ownership.");
}

#[test]
fn fails_for_missing_variable() {
    let renderer = PromptRenderer::new();
    let err = renderer.render("{{ input.missing }}", &json!({ "input": {} })).unwrap_err();
    assert!(err.to_string().contains("prompt render error"));
}
```

- [ ] **Step 2: Run test and verify it fails**

Run:

```bash
cargo test --test prompt_renderer
```

Expected: FAIL because prompt renderer modules do not exist.

- [ ] **Step 3: Implement prompt modules**

Update `src/prompt/mod.rs`:

```rust
pub mod renderer;
pub mod store;
```

Add `src/prompt/store.rs`:

```rust
use std::collections::BTreeMap;

use crate::error::AppError;

#[derive(Debug, Clone)]
pub struct PromptStore {
    prompts: BTreeMap<String, String>,
}

impl PromptStore {
    pub fn new(prompts: BTreeMap<String, String>) -> Self {
        Self { prompts }
    }

    pub fn resolve_ref(&self, name: &str) -> Result<&str, AppError> {
        self.prompts
            .get(name)
            .map(String::as_str)
            .ok_or_else(|| AppError::Config(format!("unknown prompt ref '{name}'")))
    }
}
```

Add `src/prompt/renderer.rs`:

```rust
use handlebars::{Handlebars, RenderErrorReason};
use serde_json::Value;

use crate::error::AppError;

#[derive(Debug, Clone)]
pub struct PromptRenderer {
    handlebars: Handlebars<'static>,
}

impl PromptRenderer {
    pub fn new() -> Self {
        let mut handlebars = Handlebars::new();
        handlebars.set_strict_mode(true);
        Self { handlebars }
    }

    pub fn render(&self, template: &str, data: &Value) -> Result<String, AppError> {
        self.handlebars
            .render_template(template, data)
            .map_err(|err| match err.reason() {
                RenderErrorReason::MissingVariable(_) => {
                    AppError::Run(format!("prompt render error: {err}"))
                }
                _ => AppError::Run(format!("prompt render error: {err}")),
            })
    }
}

impl Default for PromptRenderer {
    fn default() -> Self {
        Self::new()
    }
}
```

- [ ] **Step 4: Run renderer tests**

Run:

```bash
cargo test --test prompt_renderer
```

Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add src/prompt tests/prompt_renderer.rs
git commit -m "feat: render prompt templates"
```

---

### Task 4: Engine Events, Context, and Prompt Step

**Files:**
- Modify: `src/engine/mod.rs`
- Create: `src/engine/event.rs`
- Create: `src/engine/context.rs`
- Create: `src/engine/runner.rs`
- Create: `src/model/mod.rs`
- Create: `src/model/types.rs`
- Create: `src/tools/mod.rs`
- Create: `src/tools/registry.rs`
- Create: `tests/runner.rs`

**Interfaces:**
- Consumes: `LoadedAgent`, `PromptRenderer`, `PromptStore`.
- Produces: `RunEvent`.
- Produces: `RunEngine::run(&self, agent: LoadedAgent, input: Value) -> impl Stream<Item = RunEvent>`.
- Produces initial `ModelClient` and `ToolRegistry` traits used by the runner and expanded by model/tool tasks.

- [ ] **Step 1: Write failing prompt-step runner test**

Add `tests/runner.rs`:

```rust
use futures::StreamExt;
use serde_json::json;

use insight_agent_platform::{
    agent::{config::{AgentConfig, InputConfig, ModelConfig, StepConfig, StepKind}, loader::LoadedAgent},
    engine::{event::RunEventKind, runner::RunEngine},
    model::types::FakeModelClient,
    tools::registry::ToolRegistry,
};

#[tokio::test]
async fn prompt_step_renders_and_completes_run() {
    let agent = LoadedAgent {
        root: std::path::PathBuf::from("agents/test"),
        prompts: Default::default(),
        config: AgentConfig {
            id: "test".to_string(),
            name: "Test".to_string(),
            description: String::new(),
            model: ModelConfig {
                provider: "openai_compatible".to_string(),
                model: Some("fake".to_string()),
                temperature: None,
                max_tokens: None,
                options: serde_json::Value::Null,
            },
            input: InputConfig { schema: json!({"type":"object"}) },
            prompts: Default::default(),
            steps: vec![StepConfig {
                id: "hello".to_string(),
                kind: StepKind::Prompt,
                prompt_ref: None,
                prompt: Some("Hello {{ input.name }}".to_string()),
                system_prompt_ref: None,
                system_prompt: None,
                stream: false,
                tool: None,
                args: serde_json::Value::Null,
            }],
        },
    };

    let engine = RunEngine::new(FakeModelClient::new(vec![]), ToolRegistry::default());
    let events: Vec<_> = engine.run(agent, json!({"name":"Ada"})).collect().await;

    assert!(events.iter().any(|event| event.kind == RunEventKind::RunStarted));
    assert!(events.iter().any(|event| event.kind == RunEventKind::StepStarted));
    assert!(events.iter().any(|event| event.kind == RunEventKind::StepCompleted));
    let completed = events.iter().find(|event| event.kind == RunEventKind::RunCompleted).unwrap();
    assert_eq!(completed.payload["output"], "Hello Ada");
}
```

- [ ] **Step 2: Run test and verify it fails**

Run:

```bash
cargo test --test runner prompt_step_renders_and_completes_run
```

Expected: FAIL because engine/model/tool modules are incomplete.

- [ ] **Step 3: Implement event and context types**

Update `src/engine/mod.rs`:

```rust
pub mod context;
pub mod event;
pub mod runner;
```

Add `src/engine/event.rs`:

```rust
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::Value;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RunEventKind {
    RunStarted,
    StepStarted,
    TokenDelta,
    ToolCallStarted,
    ToolCallCompleted,
    StepCompleted,
    RunCompleted,
    Error,
}

impl RunEventKind {
    pub fn as_sse_name(self) -> &'static str {
        match self {
            Self::RunStarted => "run_started",
            Self::StepStarted => "step_started",
            Self::TokenDelta => "token_delta",
            Self::ToolCallStarted => "tool_call_started",
            Self::ToolCallCompleted => "tool_call_completed",
            Self::StepCompleted => "step_completed",
            Self::RunCompleted => "run_completed",
            Self::Error => "error",
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RunEvent {
    pub kind: RunEventKind,
    pub run_id: String,
    pub agent_id: String,
    pub step_id: Option<String>,
    pub timestamp: DateTime<Utc>,
    pub payload: Value,
}
```

Add `src/engine/context.rs`:

```rust
use std::collections::BTreeMap;

use chrono::{DateTime, Utc};
use serde_json::{json, Value};

#[derive(Debug, Clone)]
pub struct RunContext {
    pub run_id: String,
    pub agent_id: String,
    pub started_at: DateTime<Utc>,
    pub input: Value,
    pub step_outputs: BTreeMap<String, Value>,
}

impl RunContext {
    pub fn template_data(&self) -> Value {
        let steps = self
            .step_outputs
            .iter()
            .map(|(id, output)| (id.clone(), json!({ "output": output })))
            .collect::<serde_json::Map<_, _>>();

        json!({
            "run": {
                "id": self.run_id,
                "agent_id": self.agent_id,
                "started_at": self.started_at,
            },
            "input": self.input,
            "steps": steps,
        })
    }

    pub fn set_step_output(&mut self, step_id: &str, output: Value) {
        self.step_outputs.insert(step_id.to_string(), output);
    }
}
```

- [ ] **Step 4: Add model and tool foundations**

Update `src/model/mod.rs`:

```rust
pub mod openai;
pub mod types;
```

Add `src/model/types.rs`:

```rust
use async_trait::async_trait;
use futures::{stream, Stream};
use serde::{Deserialize, Serialize};
use std::pin::Pin;

use crate::error::AppError;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ChatMessage {
    pub role: String,
    pub content: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ChatRequest {
    pub model: String,
    pub messages: Vec<ChatMessage>,
    pub temperature: Option<f32>,
    pub max_tokens: Option<u32>,
}

pub type ChatStream = Pin<Box<dyn Stream<Item = Result<String, AppError>> + Send>>;

#[async_trait]
pub trait ModelClient: Clone + Send + Sync + 'static {
    async fn stream_chat(&self, request: ChatRequest) -> Result<ChatStream, AppError>;
}

#[derive(Debug, Clone)]
pub struct FakeModelClient {
    chunks: Vec<String>,
}

impl FakeModelClient {
    pub fn new(chunks: Vec<&str>) -> Self {
        Self { chunks: chunks.into_iter().map(str::to_string).collect() }
    }
}

#[async_trait]
impl ModelClient for FakeModelClient {
    async fn stream_chat(&self, _request: ChatRequest) -> Result<ChatStream, AppError> {
        let chunks = self.chunks.clone();
        Ok(Box::pin(stream::iter(chunks.into_iter().map(Ok))))
    }
}
```

Add `src/model/openai.rs`:

```rust
//! OpenAI-compatible model client.
```

Update `src/tools/mod.rs`:

```rust
pub mod current_time;
pub mod http_get;
pub mod registry;
```

Add `src/tools/registry.rs`:

```rust
use std::{collections::BTreeMap, sync::Arc};

use async_trait::async_trait;
use serde_json::Value;

use crate::error::AppError;

#[derive(Debug, Clone)]
pub struct ToolContext {
    pub run_id: String,
}

#[async_trait]
pub trait Tool: Send + Sync {
    fn name(&self) -> &'static str;
    async fn call(&self, args: Value, ctx: ToolContext) -> Result<Value, AppError>;
}

#[derive(Clone, Default)]
pub struct ToolRegistry {
    tools: BTreeMap<String, Arc<dyn Tool>>,
}

impl ToolRegistry {
    pub fn register<T: Tool + 'static>(&mut self, tool: T) {
        self.tools.insert(tool.name().to_string(), Arc::new(tool));
    }

    pub fn get(&self, name: &str) -> Option<Arc<dyn Tool>> {
        self.tools.get(name).cloned()
    }
}
```

Add initial tool module files:

```rust
// src/tools/current_time.rs
//! Current time tool.
```

```rust
// src/tools/http_get.rs
//! Restricted HTTP GET tool.
```

- [ ] **Step 5: Implement prompt step runner**

Add `src/engine/runner.rs`:

```rust
use std::collections::BTreeMap;

use chrono::Utc;
use futures::{stream, Stream};
use serde_json::{json, Value};
use tokio_stream::StreamExt;
use uuid::Uuid;

use crate::{
    agent::{config::{StepConfig, StepKind}, loader::LoadedAgent},
    engine::{context::RunContext, event::{RunEvent, RunEventKind}},
    error::AppError,
    model::types::ModelClient,
    prompt::{renderer::PromptRenderer, store::PromptStore},
    tools::registry::ToolRegistry,
};

#[derive(Clone)]
pub struct RunEngine<M: ModelClient> {
    model: M,
    tools: ToolRegistry,
    renderer: PromptRenderer,
}

impl<M: ModelClient> RunEngine<M> {
    pub fn new(model: M, tools: ToolRegistry) -> Self {
        Self {
            model,
            tools,
            renderer: PromptRenderer::new(),
        }
    }

    pub fn run(&self, agent: LoadedAgent, input: Value) -> impl Stream<Item = RunEvent> {
        let engine = self.clone();
        stream::unfold(Some((engine, agent, input)), |state| async move {
            let (engine, agent, input) = state?;
            let events = engine.run_collect(agent, input).await;
            Some((stream::iter(events), None))
        })
        .flatten()
    }

    async fn run_collect(&self, agent: LoadedAgent, input: Value) -> Vec<RunEvent> {
        let run_id = format!("run_{}", Uuid::new_v4());
        let mut ctx = RunContext {
            run_id: run_id.clone(),
            agent_id: agent.config.id.clone(),
            started_at: Utc::now(),
            input,
            step_outputs: BTreeMap::new(),
        };
        let mut events = vec![self.event(&ctx, None, RunEventKind::RunStarted, json!({}))];
        let store = PromptStore::new(agent.prompts.clone());

        for step in &agent.config.steps {
            events.push(self.event(&ctx, Some(&step.id), RunEventKind::StepStarted, json!({
                "step_type": step.kind
            })));
            match self.execute_step(step, &agent.config.model, &store, &mut ctx, &mut events).await {
                Ok(output) => {
                    ctx.set_step_output(&step.id, output.clone());
                    events.push(self.event(&ctx, Some(&step.id), RunEventKind::StepCompleted, json!({
                        "output": output
                    })));
                }
                Err(err) => {
                    events.push(self.event(&ctx, Some(&step.id), RunEventKind::Error, json!({
                        "message": err.to_string()
                    })));
                    return events;
                }
            }
        }

        let output = agent.config.steps
            .last()
            .and_then(|step| ctx.step_outputs.get(&step.id))
            .cloned()
            .unwrap_or(Value::Null);
        events.push(self.event(&ctx, None, RunEventKind::RunCompleted, json!({ "output": output })));
        events
    }

    async fn execute_step(
        &self,
        step: &StepConfig,
        model_config: &crate::agent::config::ModelConfig,
        store: &PromptStore,
        ctx: &mut RunContext,
        _events: &mut Vec<RunEvent>,
    ) -> Result<Value, AppError> {
        match step.kind {
            StepKind::Prompt => {
                let template = resolve_prompt(step.prompt.as_deref(), step.prompt_ref.as_deref(), store)?;
                let rendered = self.renderer.render(template, &ctx.template_data())?;
                Ok(Value::String(rendered))
            }
            StepKind::Llm => Err(AppError::Run(format!(
                "llm step '{}' is not available until the model task is completed for provider '{}'",
                step.id, model_config.provider
            ))),
            StepKind::Tool => Err(AppError::Run(format!(
                "tool step '{}' is not available until the tool registry task is completed",
                step.id
            ))),
        }
    }

    fn event(&self, ctx: &RunContext, step_id: Option<&str>, kind: RunEventKind, payload: Value) -> RunEvent {
        RunEvent {
            kind,
            run_id: ctx.run_id.clone(),
            agent_id: ctx.agent_id.clone(),
            step_id: step_id.map(str::to_string),
            timestamp: Utc::now(),
            payload,
        }
    }
}

fn resolve_prompt<'a>(
    inline: Option<&'a str>,
    prompt_ref: Option<&str>,
    store: &'a PromptStore,
) -> Result<&'a str, AppError> {
    if let Some(inline) = inline {
        return Ok(inline);
    }
    if let Some(prompt_ref) = prompt_ref {
        return store.resolve_ref(prompt_ref);
    }
    Err(AppError::Config("step requires prompt or prompt_ref".to_string()))
}
```

- [ ] **Step 6: Run runner prompt test**

Run:

```bash
cargo test --test runner prompt_step_renders_and_completes_run
```

Expected: PASS.

- [ ] **Step 7: Commit**

```bash
git add src/engine src/model src/tools tests/runner.rs
git commit -m "feat: execute prompt steps"
```

---

### Task 5: LLM Step and OpenAI-Compatible Streaming Client

**Files:**
- Modify: `src/engine/runner.rs`
- Modify: `src/model/openai.rs`
- Modify: `src/model/types.rs`
- Modify: `tests/runner.rs`

**Interfaces:**
- Consumes: `ModelClient::stream_chat`.
- Produces: `llm` step execution with `token_delta` events and final accumulated output.
- Produces: `OpenAiModelClient::new(api_key: String, base_url: String, default_model: String) -> Self`.

- [ ] **Step 1: Add failing LLM runner test**

Append to `tests/runner.rs`:

```rust
#[tokio::test]
async fn llm_step_streams_token_delta_events_and_final_output() {
    let agent = LoadedAgent {
        root: std::path::PathBuf::from("agents/test"),
        prompts: Default::default(),
        config: AgentConfig {
            id: "test".to_string(),
            name: "Test".to_string(),
            description: String::new(),
            model: ModelConfig {
                provider: "openai_compatible".to_string(),
                model: Some("fake".to_string()),
                temperature: Some(0.2),
                max_tokens: None,
                options: serde_json::Value::Null,
            },
            input: InputConfig { schema: json!({"type":"object"}) },
            prompts: Default::default(),
            steps: vec![StepConfig {
                id: "answer".to_string(),
                kind: StepKind::Llm,
                prompt_ref: None,
                prompt: Some("Answer {{ input.question }}".to_string()),
                system_prompt_ref: None,
                system_prompt: Some("You are concise.".to_string()),
                stream: true,
                tool: None,
                args: serde_json::Value::Null,
            }],
        },
    };

    let engine = RunEngine::new(FakeModelClient::new(vec!["Hel", "lo"]), ToolRegistry::default());
    let events: Vec<_> = engine.run(agent, json!({"question":"Q"})).collect().await;

    let deltas: Vec<_> = events
        .iter()
        .filter(|event| event.kind == RunEventKind::TokenDelta)
        .map(|event| event.payload["delta"].as_str().unwrap().to_string())
        .collect();
    assert_eq!(deltas, vec!["Hel", "lo"]);

    let completed = events.iter().find(|event| event.kind == RunEventKind::RunCompleted).unwrap();
    assert_eq!(completed.payload["output"], "Hello");
}
```

- [ ] **Step 2: Run test and verify it fails**

Run:

```bash
cargo test --test runner llm_step_streams_token_delta_events_and_final_output
```

Expected: FAIL with `llm step 'answer' is not available until the model task is completed`.

- [ ] **Step 3: Implement LLM step in runner**

In `src/engine/runner.rs`, replace the `StepKind::Llm` match arm with:

```rust
StepKind::Llm => {
    let user_template = resolve_prompt(step.prompt.as_deref(), step.prompt_ref.as_deref(), store)?;
    let user = self.renderer.render(user_template, &ctx.template_data())?;
    let mut messages = Vec::new();
    if step.system_prompt.is_some() || step.system_prompt_ref.is_some() {
        let system_template = resolve_prompt(
            step.system_prompt.as_deref(),
            step.system_prompt_ref.as_deref(),
            store,
        )?;
        let system = self.renderer.render(system_template, &ctx.template_data())?;
        messages.push(crate::model::types::ChatMessage {
            role: "system".to_string(),
            content: system,
        });
    }
    messages.push(crate::model::types::ChatMessage {
        role: "user".to_string(),
        content: user,
    });
    let request = crate::model::types::ChatRequest {
        model: model_config.model.clone().unwrap_or_default(),
        messages,
        temperature: model_config.temperature,
        max_tokens: model_config.max_tokens,
    };
    let mut stream = self.model.stream_chat(request).await?;
    let mut output = String::new();
    while let Some(chunk) = stream.next().await {
        let delta = chunk?;
        output.push_str(&delta);
        _events.push(self.event(ctx, Some(&step.id), RunEventKind::TokenDelta, json!({
            "delta": delta
        })));
    }
    Ok(Value::String(output))
}
```

Then change `execute_step` signature parameter from `_events` to `events`, and update `_events.push` to `events.push`.

- [ ] **Step 4: Run LLM runner test**

Run:

```bash
cargo test --test runner llm_step_streams_token_delta_events_and_final_output
```

Expected: PASS.

- [ ] **Step 5: Implement OpenAI-compatible client**

Add `src/model/openai.rs`:

```rust
use async_trait::async_trait;
use bytes::Bytes;
use futures::{StreamExt, TryStreamExt};
use reqwest::Client;
use serde::{Deserialize, Serialize};

use crate::{
    error::AppError,
    model::types::{ChatRequest, ChatStream, ModelClient},
};

#[derive(Debug, Clone)]
pub struct OpenAiModelClient {
    client: Client,
    api_key: String,
    base_url: String,
    default_model: String,
}

impl OpenAiModelClient {
    pub fn new(api_key: String, base_url: String, default_model: String) -> Self {
        Self {
            client: Client::new(),
            api_key,
            base_url: base_url.trim_end_matches('/').to_string(),
            default_model,
        }
    }
}

#[async_trait]
impl ModelClient for OpenAiModelClient {
    async fn stream_chat(&self, request: ChatRequest) -> Result<ChatStream, AppError> {
        let body = OpenAiRequest {
            model: if request.model.is_empty() { self.default_model.clone() } else { request.model },
            messages: request.messages,
            temperature: request.temperature,
            max_tokens: request.max_tokens,
            stream: true,
        };

        let response = self.client
            .post(format!("{}/chat/completions", self.base_url))
            .bearer_auth(&self.api_key)
            .json(&body)
            .send()
            .await
            .map_err(|err| AppError::Upstream(format!("model request failed: {err}")))?;

        if !response.status().is_success() {
            return Err(AppError::Upstream(format!("model returned status {}", response.status())));
        }

        let stream = response
            .bytes_stream()
            .map_err(|err| AppError::Upstream(format!("model stream failed: {err}")))
            .map_ok(parse_sse_bytes)
            .map(|item| match item {
                Ok(chunks) => Ok(chunks.join("")),
                Err(err) => Err(err),
            });

        Ok(Box::pin(stream))
    }
}

#[derive(Debug, Serialize)]
struct OpenAiRequest {
    model: String,
    messages: Vec<crate::model::types::ChatMessage>,
    temperature: Option<f32>,
    max_tokens: Option<u32>,
    stream: bool,
}

#[derive(Debug, Deserialize)]
struct OpenAiChunk {
    choices: Vec<OpenAiChoice>,
}

#[derive(Debug, Deserialize)]
struct OpenAiChoice {
    delta: OpenAiDelta,
}

#[derive(Debug, Deserialize)]
struct OpenAiDelta {
    content: Option<String>,
}

fn parse_sse_bytes(bytes: Bytes) -> Vec<String> {
    let text = String::from_utf8_lossy(&bytes);
    text.lines()
        .filter_map(|line| line.strip_prefix("data: "))
        .filter(|line| *line != "[DONE]")
        .filter_map(|line| serde_json::from_str::<OpenAiChunk>(line).ok())
        .flat_map(|chunk| chunk.choices.into_iter())
        .filter_map(|choice| choice.delta.content)
        .collect()
}
```

- [ ] **Step 6: Run all runner tests and cargo check**

Run:

```bash
cargo test --test runner
cargo check
```

Expected: PASS.

- [ ] **Step 7: Commit**

```bash
git add src/engine/runner.rs src/model tests/runner.rs
git commit -m "feat: stream llm steps"
```

---

### Task 6: Built-In Tools and Tool Step

**Files:**
- Modify: `src/tools/current_time.rs`
- Modify: `src/tools/http_get.rs`
- Modify: `src/tools/registry.rs`
- Modify: `src/engine/runner.rs`
- Modify: `tests/runner.rs`

**Interfaces:**
- Consumes: `ToolRegistry`.
- Produces: `default_tool_registry() -> ToolRegistry`.
- Produces: tool step execution with `tool_call_started` and `tool_call_completed`.

- [ ] **Step 1: Add failing tool-step test**

Append to `tests/runner.rs`:

```rust
use insight_agent_platform::tools::current_time::CurrentTimeTool;

#[tokio::test]
async fn tool_step_emits_tool_events_and_stores_output() {
    let agent = LoadedAgent {
        root: std::path::PathBuf::from("agents/test"),
        prompts: Default::default(),
        config: AgentConfig {
            id: "test".to_string(),
            name: "Test".to_string(),
            description: String::new(),
            model: ModelConfig {
                provider: "openai_compatible".to_string(),
                model: Some("fake".to_string()),
                temperature: None,
                max_tokens: None,
                options: serde_json::Value::Null,
            },
            input: InputConfig { schema: json!({"type":"object"}) },
            prompts: Default::default(),
            steps: vec![StepConfig {
                id: "now".to_string(),
                kind: StepKind::Tool,
                prompt_ref: None,
                prompt: None,
                system_prompt_ref: None,
                system_prompt: None,
                stream: false,
                tool: Some("current_time".to_string()),
                args: json!({"timezone":"Asia/Shanghai"}),
            }],
        },
    };

    let mut tools = ToolRegistry::default();
    tools.register(CurrentTimeTool);
    let engine = RunEngine::new(FakeModelClient::new(vec![]), tools);
    let events: Vec<_> = engine.run(agent, json!({})).collect().await;

    assert!(events.iter().any(|event| event.kind == RunEventKind::ToolCallStarted));
    assert!(events.iter().any(|event| event.kind == RunEventKind::ToolCallCompleted));
    let completed = events.iter().find(|event| event.kind == RunEventKind::RunCompleted).unwrap();
    assert_eq!(completed.payload["output"]["timezone"], "Asia/Shanghai");
}
```

- [ ] **Step 2: Run test and verify it fails**

Run:

```bash
cargo test --test runner tool_step_emits_tool_events_and_stores_output
```

Expected: FAIL because `CurrentTimeTool` and tool step execution are not implemented.

- [ ] **Step 3: Implement current_time and http_get tools**

Add `src/tools/current_time.rs`:

```rust
use async_trait::async_trait;
use chrono::Utc;
use chrono_tz::Tz;
use serde_json::{json, Value};

use crate::{error::AppError, tools::registry::{Tool, ToolContext}};

#[derive(Debug, Clone, Copy)]
pub struct CurrentTimeTool;

#[async_trait]
impl Tool for CurrentTimeTool {
    fn name(&self) -> &'static str {
        "current_time"
    }

    async fn call(&self, args: Value, _ctx: ToolContext) -> Result<Value, AppError> {
        let timezone = args
            .get("timezone")
            .and_then(Value::as_str)
            .unwrap_or("UTC");
        let tz: Tz = timezone
            .parse()
            .map_err(|_| AppError::Run(format!("invalid timezone '{timezone}'")))?;
        let now = Utc::now().with_timezone(&tz);
        Ok(json!({
            "timezone": timezone,
            "iso8601": now.to_rfc3339(),
        }))
    }
}
```

Add `src/tools/http_get.rs`:

```rust
use async_trait::async_trait;
use serde_json::{json, Value};
use std::time::Duration;

use crate::{error::AppError, tools::registry::{Tool, ToolContext}};

#[derive(Debug, Clone)]
pub struct HttpGetTool {
    client: reqwest::Client,
    max_bytes: usize,
}

impl Default for HttpGetTool {
    fn default() -> Self {
        Self {
            client: reqwest::Client::builder()
                .timeout(Duration::from_secs(10))
                .build()
                .expect("valid reqwest client"),
            max_bytes: 256 * 1024,
        }
    }
}

#[async_trait]
impl Tool for HttpGetTool {
    fn name(&self) -> &'static str {
        "http_get"
    }

    async fn call(&self, args: Value, _ctx: ToolContext) -> Result<Value, AppError> {
        let url = args
            .get("url")
            .and_then(Value::as_str)
            .ok_or_else(|| AppError::Run("http_get requires string arg 'url'".to_string()))?;
        let parsed = reqwest::Url::parse(url)
            .map_err(|err| AppError::Run(format!("invalid url: {err}")))?;
        if parsed.scheme() != "https" {
            return Err(AppError::Run("http_get only allows https URLs".to_string()));
        }

        let response = self.client
            .get(parsed)
            .send()
            .await
            .map_err(|err| AppError::Run(format!("http_get failed: {err}")))?;
        let status = response.status().as_u16();
        let bytes = response
            .bytes()
            .await
            .map_err(|err| AppError::Run(format!("http_get read failed: {err}")))?;
        if bytes.len() > self.max_bytes {
            return Err(AppError::Run("http_get response too large".to_string()));
        }
        let body = String::from_utf8_lossy(&bytes).to_string();
        Ok(json!({ "status": status, "body": body }))
    }
}
```

Update `src/tools/registry.rs`:

```rust
pub fn default_tool_registry() -> ToolRegistry {
    let mut registry = ToolRegistry::default();
    registry.register(crate::tools::current_time::CurrentTimeTool);
    registry.register(crate::tools::http_get::HttpGetTool::default());
    registry
}
```

- [ ] **Step 4: Implement tool step execution**

In `src/engine/runner.rs`, replace the `StepKind::Tool` match arm with:

```rust
StepKind::Tool => {
    let tool_name = step
        .tool
        .as_deref()
        .ok_or_else(|| AppError::Config(format!("tool step '{}' requires tool", step.id)))?;
    let tool = self
        .tools
        .get(tool_name)
        .ok_or_else(|| AppError::Run(format!("unknown tool '{tool_name}'")))?;
    events.push(self.event(ctx, Some(&step.id), RunEventKind::ToolCallStarted, json!({
        "tool": tool_name
    })));
    let output = tool.call(
        step.args.clone(),
        crate::tools::registry::ToolContext { run_id: ctx.run_id.clone() },
    ).await?;
    events.push(self.event(ctx, Some(&step.id), RunEventKind::ToolCallCompleted, json!({
        "tool": tool_name,
        "output": output
    })));
    Ok(output)
}
```

- [ ] **Step 5: Run tool test**

Run:

```bash
cargo test --test runner tool_step_emits_tool_events_and_stores_output
```

Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add src/tools src/engine/runner.rs tests/runner.rs
git commit -m "feat: execute tool steps"
```

---

### Task 7: HTTP API and SSE Encoding

**Files:**
- Modify: `src/api/mod.rs`
- Create: `src/api/routes.rs`
- Create: `src/api/sse.rs`
- Modify: `src/main.rs`
- Create: `tests/api.rs`

**Interfaces:**
- Consumes: `AgentRegistry`, `RunEngine`, `RunEvent`.
- Produces: `build_router(state: AppState<M>) -> Router`.
- Produces: `/health`, `/v1/agents`, `/v1/agents/{agent_id}`, `/v1/agents/{agent_id}/runs/stream`.

- [ ] **Step 1: Add failing API tests**

Add `tests/api.rs`:

```rust
use axum::{body::Body, http::{Request, StatusCode}};
use serde_json::json;
use tower::ServiceExt;

use insight_agent_platform::{
    agent::{config::{AgentConfig, InputConfig, ModelConfig, StepConfig, StepKind}, loader::LoadedAgent, registry::AgentRegistry},
    api::routes::{build_router, AppState},
    engine::runner::RunEngine,
    model::types::FakeModelClient,
    tools::registry::ToolRegistry,
};

fn app() -> axum::Router {
    let agent = LoadedAgent {
        root: std::path::PathBuf::from("agents/test"),
        prompts: Default::default(),
        config: AgentConfig {
            id: "test".to_string(),
            name: "Test".to_string(),
            description: "Test agent".to_string(),
            model: ModelConfig {
                provider: "openai_compatible".to_string(),
                model: Some("fake".to_string()),
                temperature: None,
                max_tokens: None,
                options: serde_json::Value::Null,
            },
            input: InputConfig { schema: json!({"type":"object"}) },
            prompts: Default::default(),
            steps: vec![StepConfig {
                id: "hello".to_string(),
                kind: StepKind::Prompt,
                prompt_ref: None,
                prompt: Some("Hello {{ input.name }}".to_string()),
                system_prompt_ref: None,
                system_prompt: None,
                stream: false,
                tool: None,
                args: serde_json::Value::Null,
            }],
        },
    };
    let registry = AgentRegistry::new(vec![agent]).unwrap();
    let engine = RunEngine::new(FakeModelClient::new(vec![]), ToolRegistry::default());
    build_router(AppState { registry, engine })
}

#[tokio::test]
async fn lists_agents_without_prompt_contents() {
    let response = app()
        .oneshot(Request::builder().uri("/v1/agents").body(Body::empty()).unwrap())
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
}

#[tokio::test]
async fn streams_agent_run_as_sse() {
    let response = app()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/v1/agents/test/runs/stream")
                .header("content-type", "application/json")
                .body(Body::from(r#"{"input":{"name":"Ada"}}"#))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(
        response.headers().get("content-type").unwrap().to_str().unwrap(),
        "text/event-stream"
    );
}
```

- [ ] **Step 2: Run API tests and verify they fail**

Run:

```bash
cargo test --test api
```

Expected: FAIL because API modules do not exist.

- [ ] **Step 3: Implement SSE conversion**

Update `src/api/mod.rs`:

```rust
pub mod routes;
pub mod sse;
```

Add `src/api/sse.rs`:

```rust
use axum::response::sse::Event;

use crate::{engine::event::RunEvent, error::AppError};

pub fn encode_event(event: RunEvent) -> Result<Event, AppError> {
    let name = event.kind.as_sse_name();
    let data = serde_json::to_string(&event)
        .map_err(|err| AppError::Run(format!("failed to encode sse event: {err}")))?;
    Ok(Event::default().event(name).data(data))
}
```

- [ ] **Step 4: Implement routes**

Add `src/api/routes.rs`:

```rust
use std::{convert::Infallible, sync::Arc};

use axum::{
    extract::{Path, State},
    response::{sse::Sse, IntoResponse},
    routing::{get, post},
    Json, Router,
};
use futures::{Stream, StreamExt};
use serde::Deserialize;
use serde_json::{json, Value};

use crate::{
    agent::registry::AgentRegistry,
    api::sse::encode_event,
    engine::runner::RunEngine,
    error::AppError,
    model::types::ModelClient,
};

#[derive(Clone)]
pub struct AppState<M: ModelClient> {
    pub registry: AgentRegistry,
    pub engine: RunEngine<M>,
}

pub fn build_router<M: ModelClient>(state: AppState<M>) -> Router {
    Router::new()
        .route("/health", get(health))
        .route("/v1/agents", get(list_agents::<M>))
        .route("/v1/agents/:agent_id", get(get_agent::<M>))
        .route("/v1/agents/:agent_id/runs/stream", post(run_agent_stream::<M>))
        .with_state(Arc::new(state))
}

async fn health() -> Json<Value> {
    Json(json!({ "status": "ok" }))
}

async fn list_agents<M: ModelClient>(State(state): State<Arc<AppState<M>>>) -> Json<Value> {
    let agents: Vec<_> = state.registry.list().map(|agent| {
        json!({
            "id": agent.config.id,
            "name": agent.config.name,
            "description": agent.config.description,
            "input_schema": agent.config.input.schema,
        })
    }).collect();
    Json(Value::Array(agents))
}

async fn get_agent<M: ModelClient>(
    State(state): State<Arc<AppState<M>>>,
    Path(agent_id): Path<String>,
) -> Result<Json<Value>, AppError> {
    let agent = state
        .registry
        .get(&agent_id)
        .ok_or_else(|| AppError::NotFound(format!("agent '{agent_id}' not found")))?;
    Ok(Json(json!({
        "id": agent.config.id,
        "name": agent.config.name,
        "description": agent.config.description,
        "input_schema": agent.config.input.schema,
    })))
}

#[derive(Debug, Deserialize)]
struct RunRequest {
    input: Value,
}

async fn run_agent_stream<M: ModelClient>(
    State(state): State<Arc<AppState<M>>>,
    Path(agent_id): Path<String>,
    Json(request): Json<RunRequest>,
) -> Result<impl IntoResponse, AppError> {
    let agent = state
        .registry
        .get(&agent_id)
        .ok_or_else(|| AppError::NotFound(format!("agent '{agent_id}' not found")))?
        .clone();

    let stream = state
        .engine
        .run(agent, request.input)
        .map(|event| Ok::<_, Infallible>(encode_event(event).expect("event encoding should not fail")));

    Ok(Sse::new(stream))
}
```

- [ ] **Step 5: Wire main to load agents and start server**

Replace `src/main.rs` with:

```rust
use insight_agent_platform::{
    agent::{loader::load_agents, registry::AgentRegistry},
    api::routes::{build_router, AppState},
    config::PlatformConfig,
    engine::runner::RunEngine,
    error::AppError,
    model::openai::OpenAiModelClient,
    tools::registry::default_tool_registry,
};

#[tokio::main]
async fn main() -> Result<(), AppError> {
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .init();

    let config = PlatformConfig::from_env()?;
    let agents = load_agents(&config.agents_dir)?;
    let registry = AgentRegistry::new(agents)?;
    let model = OpenAiModelClient::new(
        config.openai_api_key,
        config.openai_base_url,
        config.openai_default_model,
    );
    let engine = RunEngine::new(model, default_tool_registry());
    let app = build_router(AppState { registry, engine });

    let listener = tokio::net::TcpListener::bind(config.bind_addr)
        .await
        .map_err(|err| AppError::Config(format!("failed to bind server: {err}")))?;
    tracing::info!(bind_addr = %config.bind_addr, "server listening");
    axum::serve(listener, app)
        .await
        .map_err(|err| AppError::Run(format!("server error: {err}")))?;
    Ok(())
}
```

- [ ] **Step 6: Run API tests**

Run:

```bash
cargo test --test api
```

Expected: PASS.

- [ ] **Step 7: Commit**

```bash
git add src/api src/main.rs tests/api.rs
git commit -m "feat: expose agent streaming api"
```

---

### Task 8: Input Schema Validation and Sample Agent

**Files:**
- Modify: `src/api/routes.rs`
- Create: `agents/researcher/agent.yaml`
- Create: `agents/researcher/prompts/system.md`
- Create: `agents/researcher/prompts/planner.md`
- Create: `agents/researcher/prompts/final.md`
- Modify: `tests/api.rs`

**Interfaces:**
- Consumes: `agent.config.input.schema`.
- Produces: HTTP `400` for invalid input before SSE starts.
- Produces: runnable sample agent.

- [ ] **Step 1: Add failing invalid-input API test**

Append to `tests/api.rs`:

```rust
#[tokio::test]
async fn invalid_input_returns_400_before_sse() {
    let response = app()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/v1/agents/test/runs/stream")
                .header("content-type", "application/json")
                .body(Body::from(r#"{"input":"not-object"}"#))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
}
```

Then update the test app schema in `tests/api.rs` from:

```rust
InputConfig { schema: json!({"type":"object"}) }
```

to:

```rust
InputConfig { schema: json!({"type":"object","required":["name"],"properties":{"name":{"type":"string"}}}) }
```

- [ ] **Step 2: Run test and verify it fails**

Run:

```bash
cargo test --test api invalid_input_returns_400_before_sse
```

Expected: FAIL because route does not validate input.

- [ ] **Step 3: Add input schema validation**

In `src/api/routes.rs`, before creating the stream in `run_agent_stream`, add:

```rust
let compiled = jsonschema::JSONSchema::compile(&agent.config.input.schema)
    .map_err(|err| AppError::Config(format!("invalid input schema for agent '{agent_id}': {err}")))?;
if let Err(errors) = compiled.validate(&request.input) {
    let messages: Vec<String> = errors.map(|err| err.to_string()).collect();
    return Err(AppError::Input(format!("input validation failed: {}", messages.join("; "))));
}
```

- [ ] **Step 4: Add sample researcher agent**

Add `agents/researcher/agent.yaml`:

```yaml
id: researcher
name: Research Assistant
description: Research and answer questions with a plan-first flow.

model:
  provider: openai_compatible
  model: qwen3.6-flash
  temperature: 0.3

input:
  schema:
    type: object
    required: [question]
    properties:
      question:
        type: string

prompts:
  system: prompts/system.md
  planner: prompts/planner.md
  final: prompts/final.md

steps:
  - id: plan
    type: llm
    system_prompt_ref: system
    prompt_ref: planner
    stream: true

  - id: now
    type: tool
    tool: current_time
    args:
      timezone: Asia/Shanghai

  - id: answer
    type: llm
    system_prompt_ref: system
    prompt_ref: final
    stream: true
```

Add `agents/researcher/prompts/system.md`:

```markdown
You are a precise research assistant. Answer in Chinese unless the user asks for another language.
```

Add `agents/researcher/prompts/planner.md`:

```markdown
Create a concise plan for answering this question:

{{ input.question }}
```

Add `agents/researcher/prompts/final.md`:

```markdown
Question:
{{ input.question }}

Plan:
{{ steps.plan.output }}

Current time:
{{ steps.now.output.iso8601 }}

Write the final answer.
```

- [ ] **Step 5: Run API tests and load sample agent**

Run:

```bash
cargo test --test api
cargo test --test agent_loader
```

Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add src/api/routes.rs tests/api.rs agents/researcher
git commit -m "feat: validate input and add sample agent"
```

---

### Task 9: End-to-End Verification and Documentation

**Files:**
- Create: `README.md`
- Modify: existing files only if verification finds defects.

**Interfaces:**
- Consumes: all previous tasks.
- Produces: documented local run commands and curl examples.

- [ ] **Step 1: Add README**

Add `README.md`:

```markdown
# Insight Agent Platform

Rust service for platform-owned, config-driven agents.

## Configuration

Set environment variables:

```text
OPENAI_API_KEY=...
OPENAI_BASE_URL=https://dashscope.aliyuncs.com/compatible-mode/v1
OPENAI_DEFAULT_MODEL=qwen3.6-flash
AGENTS_DIR=agents
BIND_ADDR=127.0.0.1:3000
```

Do not commit real API keys.

## Run

```bash
cargo run
```

## List Agents

```bash
curl http://127.0.0.1:3000/v1/agents
```

## Stream a Run

```bash
curl -N \
  -H 'content-type: application/json' \
  -H 'accept: text/event-stream' \
  -d '{"input":{"question":"用中文解释这个平台的架构"}}' \
  http://127.0.0.1:3000/v1/agents/researcher/runs/stream
```
```

- [ ] **Step 2: Run full test suite**

Run:

```bash
cargo test
```

Expected: PASS.

- [ ] **Step 3: Run formatter and check**

Run:

```bash
cargo fmt --check
cargo check
```

Expected: PASS.

- [ ] **Step 4: Start server with real environment variables**

Run:

```bash
OPENAI_API_KEY="$OPENAI_API_KEY" \
OPENAI_BASE_URL="${OPENAI_BASE_URL:-https://dashscope.aliyuncs.com/compatible-mode/v1}" \
OPENAI_DEFAULT_MODEL="${OPENAI_DEFAULT_MODEL:-qwen3.6-flash}" \
cargo run
```

Expected: process logs `server listening` on `127.0.0.1:3000`.

- [ ] **Step 5: Verify agent list**

In another shell:

```bash
curl -s http://127.0.0.1:3000/v1/agents
```

Expected: JSON array containing `researcher` and no prompt file contents.

- [ ] **Step 6: Verify streaming run**

Run:

```bash
curl -N \
  -H 'content-type: application/json' \
  -H 'accept: text/event-stream' \
  -d '{"input":{"question":"用中文简要说明 Rust agent 平台设计"}}' \
  http://127.0.0.1:3000/v1/agents/researcher/runs/stream
```

Expected: SSE events including `run_started`, `step_started`, at least one `token_delta`, `tool_call_started`, `tool_call_completed`, and `run_completed`.

- [ ] **Step 7: Commit**

```bash
git add README.md
git commit -m "docs: add local run instructions"
```
