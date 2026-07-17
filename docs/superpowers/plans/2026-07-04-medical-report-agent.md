# Medical Report Agent Implementation Plan

> **Historical / superseded:** this plan predates the structured v2 DSL; its config/node and multimodal syntax is not accepted. The current `llm`/messages/content/image contract is [DSL Authoring Surface Redesign](../specs/2026-07-17-dsl-authoring-surface-redesign.md).

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add an OpenAI-compatible multimodal medical report interpretation agent with text/image inputs, multi-turn context, non-report refusal prompts, and a three-step interpretation flow.

**Architecture:** Preserve existing config-driven execution. Extend the model message type to represent either text-only or multimodal content, then let individual LLM steps opt into attaching image URLs from `input.images` via `image_input`. The medical agent is implemented as `agent.yaml` plus Markdown prompts.

**Tech Stack:** Rust, Axum, Serde, serde_json, serde_yaml, Handlebars, OpenAI-compatible chat completions, cargo tests.

## Global Constraints

- No external agent creation API.
- Existing text-only agents must continue to send `"content": "..."`.
- Multimodal messages must serialize as OpenAI-compatible content parts.
- The platform must not download, store, transform, or OCR images.
- No server-side conversation memory; callers send `messages` each run.
- No real model integration test with private API keys.

---

### Task 1: Multimodal Chat Message Model

**Files:**
- Modify: `src/model/types.rs`
- Modify: `src/model/openai.rs`

**Interfaces:**
- Produces: `ChatMessage::text(role: impl Into<String>, content: impl Into<String>) -> ChatMessage`
- Produces: `ChatMessage::multimodal(role: impl Into<String>, parts: Vec<ChatContentPart>) -> ChatMessage`
- Produces: `ChatContentPart::text(text: impl Into<String>) -> ChatContentPart`
- Produces: `ChatContentPart::image_url(url: impl Into<String>) -> ChatContentPart`

- [ ] **Step 1: Write failing OpenAI serialization tests**

Add tests in `src/model/openai.rs` test module:

```rust
#[test]
fn text_message_serializes_as_plain_string_content() {
    let body = serde_json::to_value(super::OpenAiRequest {
        model: "model".to_string(),
        messages: vec![ChatMessage::text("user", "Hi")],
        temperature: None,
        max_tokens: None,
        stream: true,
    })
    .unwrap();

    assert_eq!(body["messages"][0]["role"], "user");
    assert_eq!(body["messages"][0]["content"], "Hi");
}

#[test]
fn multimodal_message_serializes_as_openai_content_parts() {
    let body = serde_json::to_value(super::OpenAiRequest {
        model: "model".to_string(),
        messages: vec![ChatMessage::multimodal(
            "user",
            vec![
                ChatContentPart::text("Interpret this report."),
                ChatContentPart::image_url("data:image/png;base64,abc123"),
            ],
        )],
        temperature: None,
        max_tokens: None,
        stream: true,
    })
    .unwrap();

    assert_eq!(body["messages"][0]["role"], "user");
    assert_eq!(body["messages"][0]["content"][0]["type"], "text");
    assert_eq!(
        body["messages"][0]["content"][0]["text"],
        "Interpret this report."
    );
    assert_eq!(body["messages"][0]["content"][1]["type"], "image_url");
    assert_eq!(
        body["messages"][0]["content"][1]["image_url"]["url"],
        "data:image/png;base64,abc123"
    );
}
```

- [ ] **Step 2: Run tests and verify failure**

Run: `cargo test model::openai::tests::text_message_serializes_as_plain_string_content model::openai::tests::multimodal_message_serializes_as_openai_content_parts`

Expected: fail because `ChatMessage::text`, `ChatMessage::multimodal`, and `ChatContentPart` do not exist.

- [ ] **Step 3: Implement minimal multimodal message types**

In `src/model/types.rs`, replace string-only content with:

```rust
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ChatMessage {
    pub role: String,
    pub content: ChatContent,
}

impl ChatMessage {
    pub fn text(role: impl Into<String>, content: impl Into<String>) -> Self {
        Self {
            role: role.into(),
            content: ChatContent::Text(content.into()),
        }
    }

    pub fn multimodal(role: impl Into<String>, parts: Vec<ChatContentPart>) -> Self {
        Self {
            role: role.into(),
            content: ChatContent::Parts(parts),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(untagged)]
pub enum ChatContent {
    Text(String),
    Parts(Vec<ChatContentPart>),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ChatContentPart {
    Text { text: String },
    ImageUrl { image_url: ImageUrl },
}

impl ChatContentPart {
    pub fn text(text: impl Into<String>) -> Self {
        Self::Text { text: text.into() }
    }

    pub fn image_url(url: impl Into<String>) -> Self {
        Self::ImageUrl {
            image_url: ImageUrl { url: url.into() },
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ImageUrl {
    pub url: String,
}
```

Update existing struct literals to `ChatMessage::text(...)`.

- [ ] **Step 4: Run tests and commit**

Run: `cargo test model::openai::tests::text_message_serializes_as_plain_string_content model::openai::tests::multimodal_message_serializes_as_openai_content_parts`

Expected: both tests pass.

Commit:

```bash
git add src/model/types.rs src/model/openai.rs src/engine/runner.rs tests/runner.rs
git commit -m "feat: support multimodal chat messages"
```

### Task 2: Runner Image Input Support

**Files:**
- Modify: `src/agent/config.rs`
- Modify: `src/engine/runner.rs`
- Modify: `tests/runner.rs`

**Interfaces:**
- Consumes: `ChatContentPart::text` and `ChatContentPart::image_url`
- Produces: `StepConfig.image_input: Option<String>`
- Produces: runner support for `image_input: input.images`

- [ ] **Step 1: Write failing runner test**

Add a test in `tests/runner.rs`:

```rust
#[tokio::test]
async fn llm_step_attaches_input_images_to_user_message_when_configured() {
    let model = RecordingModelClient::default();
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
            input: InputConfig {
                schema: json!({"type":"object"}),
            },
            prompts: Default::default(),
            steps: vec![StepConfig {
                id: "vision".to_string(),
                kind: StepKind::Llm,
                prompt_ref: None,
                prompt: Some("Read {{ input.report_text }}".to_string()),
                system_prompt_ref: None,
                system_prompt: None,
                image_input: Some("input.images".to_string()),
                stream: true,
                tool: None,
                args: serde_json::Value::Null,
            }],
        },
    };

    let engine = RunEngine::new(model.clone(), ToolRegistry::default());
    let events: Vec<_> = engine
        .run(
            agent,
            json!({
                "report_text": "血红蛋白偏低",
                "images": [
                    "https://example.com/report.png",
                    "data:image/png;base64,abc123"
                ]
            }),
        )
        .collect()
        .await;

    assert!(events
        .iter()
        .any(|event| event.kind == RunEventKind::RunCompleted));
    let requests = model.requests.lock().unwrap();
    let message = &requests[0].messages[0];
    let value = serde_json::to_value(message).unwrap();
    assert_eq!(value["content"][0]["text"], "Read 血红蛋白偏低");
    assert_eq!(
        value["content"][1]["image_url"]["url"],
        "https://example.com/report.png"
    );
    assert_eq!(
        value["content"][2]["image_url"]["url"],
        "data:image/png;base64,abc123"
    );
}
```

- [ ] **Step 2: Run test and verify failure**

Run: `cargo test --test runner llm_step_attaches_input_images_to_user_message_when_configured`

Expected: fail because `StepConfig` has no `image_input` field and runner never attaches images.

- [ ] **Step 3: Implement image input support**

Add `image_input` to `StepConfig`:

```rust
#[serde(default)]
pub image_input: Option<String>,
```

In `execute_llm_step`, build the user message with a helper:

```rust
let user_prompt = self.renderer.render(prompt_template, &ctx.template_data())?;
messages.push(build_user_message(user_prompt, step.image_input.as_deref(), ctx)?);
```

Add helpers in `src/engine/runner.rs`:

```rust
fn build_user_message(
    prompt: String,
    image_input: Option<&str>,
    ctx: &RunContext,
) -> Result<ChatMessage, AppError> {
    let Some(path) = image_input else {
        return Ok(ChatMessage::text("user", prompt));
    };
    let images = resolve_image_input(path, ctx)?;
    if images.is_empty() {
        return Ok(ChatMessage::text("user", prompt));
    }
    let mut parts = Vec::with_capacity(images.len() + 1);
    parts.push(ChatContentPart::text(prompt));
    parts.extend(images.into_iter().map(ChatContentPart::image_url));
    Ok(ChatMessage::multimodal("user", parts))
}

fn resolve_image_input(path: &str, ctx: &RunContext) -> Result<Vec<String>, AppError> {
    if path != "input.images" {
        return Err(AppError::Config(format!(
            "unsupported image_input path '{path}'"
        )));
    }
    let Some(images) = ctx.input.get("images") else {
        return Ok(Vec::new());
    };
    let Some(images) = images.as_array() else {
        return Err(AppError::Run("image_input 'input.images' must be an array".to_string()));
    };
    Ok(images
        .iter()
        .filter_map(|image| image.as_str().map(str::to_string))
        .collect())
}
```

Update all `StepConfig` literals in tests to include `image_input: None`.

- [ ] **Step 4: Run tests and commit**

Run: `cargo test --test runner`

Expected: all runner tests pass.

Commit:

```bash
git add src/agent/config.rs src/engine/runner.rs tests/runner.rs
git commit -m "feat: attach configured input images to llm steps"
```

### Task 3: Medical Report Agent Configuration

**Files:**
- Create: `agents/medical_report_interpreter/agent.yaml`
- Create: `agents/medical_report_interpreter/prompts/system.md`
- Create: `agents/medical_report_interpreter/prompts/abnormal_indicators.md`
- Create: `agents/medical_report_interpreter/prompts/comprehensive_interpretation.md`
- Create: `agents/medical_report_interpreter/prompts/health_advice.md`
- Modify: `tests/agent_loader.rs`
- Modify: `README.md`

**Interfaces:**
- Consumes: `image_input: input.images`
- Produces: agent id `medical_report_interpreter`

- [ ] **Step 1: Write failing loader test**

Add test in `tests/agent_loader.rs`:

```rust
#[test]
fn loads_medical_report_interpreter_agent() {
    let agents = load_agents(Path::new("agents")).unwrap();
    let agent = agents
        .iter()
        .find(|agent| agent.config.id == "medical_report_interpreter")
        .unwrap();

    assert_eq!(agent.config.steps.len(), 3);
    assert_eq!(agent.config.steps[0].id, "abnormal_indicators");
    assert_eq!(
        agent.config.steps[0].image_input.as_deref(),
        Some("input.images")
    );
    assert!(agent.prompts.contains_key("health_advice"));
}
```

- [ ] **Step 2: Run test and verify failure**

Run: `cargo test --test agent_loader loads_medical_report_interpreter_agent`

Expected: fail because the agent does not exist yet.

- [ ] **Step 3: Add agent YAML and prompts**

Create `agents/medical_report_interpreter/agent.yaml` with three LLM steps, shared system prompt, and `image_input: input.images` on each step.

Create Markdown prompts that include:

- Non-medical report refusal rule.
- `{{ input.report_text }}`.
- `{{#each input.messages}}...{{/each}}` for history.
- `{{ input.question }}`.
- Prior step output references in later steps.

- [ ] **Step 4: Update README example**

Add a curl example for `medical_report_interpreter` showing `report_text`, `images`, `messages`, and `question`.

- [ ] **Step 5: Run tests and commit**

Run: `cargo test --test agent_loader loads_medical_report_interpreter_agent`

Expected: pass.

Commit:

```bash
git add agents/medical_report_interpreter tests/agent_loader.rs README.md
git commit -m "feat: add medical report interpreter agent"
```

### Task 4: Full Verification

**Files:**
- Verify only.

- [ ] **Step 1: Run formatting**

Run: `cargo fmt --check`

Expected: success.

- [ ] **Step 2: Run compiler check**

Run: `cargo check`

Expected: success.

- [ ] **Step 3: Run full test suite**

Run: `cargo test`

Expected: all tests pass.

- [ ] **Step 4: Inspect git status**

Run: `git status --short`

Expected: clean.
