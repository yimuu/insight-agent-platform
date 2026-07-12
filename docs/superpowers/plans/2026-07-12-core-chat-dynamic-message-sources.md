# Core Chat Dynamic Message Sources Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Let `core.chat.messages` interleave static messages with bounded, strictly validated runtime message arrays from `input.*` or `nodes.<id>.output.*` paths.

**Architecture:** Keep static compilation unchanged and add `src/nodes/chat/dynamic.rs` for dynamic configuration, canonical paths, resolution, limits, strict parsing, content allowlists, and conversion. `chat.rs` owns ordered static/dynamic entry compilation and expansion; existing graph validation, model capabilities, provider serialization, and body-free observability remain authoritative.

**Tech Stack:** Rust, Serde/serde_json, YAML, AgentCompiler graph validation, Tokio, tracing test recorder, existing OpenAI-compatible message types.

## Global Constraints

- Existing static chat YAML and behavior remain compatible.
- Entries expand in configuration order; dynamic content is not Handlebars-rendered again.
- Paths are canonical dotted `input.<field>...` or `nodes.<id>.output.<field>...`; direct node output arrays are valid.
- Dynamic roles are only `user` and `assistant`.
- Defaults: `optional: false`, `max_messages: 50`, `max_bytes: 262144`, `allowed_content: [text]`.
- Missing optional sources and empty arrays expand to zero; null and non-arrays fail.
- Limits are positive and fail before parsing, logging, or provider invocation; no truncation.
- First-version content kinds are `text` and `image_url`.
- Images require explicit allowance, user role, nonblank URL, and Vision capability.
- Errors and logs never expose dynamic bodies, text, URLs, or source JSON.
- `input_audio` / `AudioInput` is a future extension and is not implemented.

---

## File Structure

- Create `src/nodes/chat/dynamic.rs`: dynamic source configuration, paths, resolution, validation, conversion, and errors.
- Modify `src/nodes/chat.rs`: parse and compile ordered entries, collect references/capabilities, flatten messages, reject final emptiness.
- Modify `tests/core_chat_action.rs`: direct compiler/executor contract.
- Modify `tests/formal_agent_compile.rs`: valid predecessor reference.
- Modify `tests/dsl_parallel.rs`: invalid cross-branch/future references.
- Modify `tests/observability.rs`: final counts, failure ordering, and body secrecy.
- Run existing `tests/repository_agents_v1.rs`: static repository compatibility without test churn.

### Task 1: Dynamic Message Sources End to End

**Files:**
- Create: `src/nodes/chat/dynamic.rs`
- Modify: `src/nodes/chat.rs`
- Modify: `tests/core_chat_action.rs`
- Modify: `tests/formal_agent_compile.rs`
- Modify: `tests/dsl_parallel.rs`
- Modify: `tests/observability.rs`

**Interfaces:**
- Consumes: `RunContext::input`, `RunContext::node_output`, model message types, Vision capability, node references, and graph dominance validation.
- Produces: ordered `{from: ...}` entries and the compile/runtime error codes fixed in the design specification.

- [ ] **Step 1: Write failing direct compiler/executor tests**

Import `handlebars::Handlebars` and add this helper in `tests/core_chat_action.rs`:

```rust
fn compile_chat_with_messages(
    messages: Value,
    capabilities: BTreeSet<ModelCapability>,
) -> (
    CompiledNode,
    Arc<Handlebars<'static>>,
    Arc<Mutex<Vec<ChatRequest>>>,
) {
    let model = RecordingModel {
        requests: Arc::new(Mutex::new(Vec::new())),
        capabilities,
    };
    let requests = Arc::clone(&model.requests);
    let mut models = ModelRegistry::default();
    models.register("primary", model).unwrap();
    let actions = ActionRegistry::default();
    let mut compile_context = CompileContext::new(&models, &actions);
    let compilation = ChatNode
        .compile(
            "answer",
            json!({"model":"primary", "messages":messages}),
            &mut compile_context,
        )
        .unwrap();
    (
        compiled_node("answer", "core.chat", EmitPolicy::None, compilation),
        Arc::new(compile_context.into_templates()),
        requests,
    )
}

#[tokio::test]
async fn dynamic_messages_expand_in_order_without_second_rendering() {
    let (node, templates, requests) = compile_chat_with_messages(
        json!([
            {"role":"system", "content":"system"},
            {"from":{"path":"input.messages"}},
            {"role":"user", "content":"current"}
        ]),
        BTreeSet::new(),
    );
    let context = context(json!({"messages":[
        {"role":"user", "content":"{{ input.literal }}"},
        {"role":"assistant", "content":"history"}
    ]}))
    .with_templates(templates);
    let (control, _) = capturing_control();

    ChatNode.execute(&node, &context, &control).await.unwrap();

    let request = requests.lock().unwrap().pop().unwrap();
    assert_eq!(
        request.messages.iter().map(|message| message.role).collect::<Vec<_>>(),
        vec![ChatRole::System, ChatRole::User, ChatRole::Assistant, ChatRole::User]
    );
    assert_eq!(request.messages[1].text(), Some("{{ input.literal }}"));
    assert_eq!(request.messages[2].text(), Some("history"));
    assert_eq!(request.messages[3].text(), Some("current"));
}
```

Add the remaining successful expansion tests:

```rust
#[tokio::test]
async fn dynamic_messages_resolve_direct_and_nested_node_outputs() {
    for (path, output) in [
        (
            "nodes.prepare.output",
            json!([{"role":"user", "content":"from node"}]),
        ),
        (
            "nodes.prepare.output.messages",
            json!({"messages":[{"role":"user", "content":"from node"}]}),
        ),
    ] {
        let (node, templates, requests) = compile_chat_with_messages(
            json!([{"from":{"path":path}}]),
            BTreeSet::new(),
        );
        let mut context = context(json!({})).with_templates(templates);
        context.set_node_output("prepare", output);
        let (control, _) = capturing_control();

        ChatNode.execute(&node, &context, &control).await.unwrap();

        let request = requests.lock().unwrap().pop().unwrap();
        assert_eq!(request.messages[0].text(), Some("from node"));
    }
}

#[tokio::test]
async fn dynamic_messages_handle_optional_missing_and_empty_sources() {
    for input in [json!({}), json!({"messages":[]})] {
        let (node, templates, requests) = compile_chat_with_messages(
            json!([
                {"role":"system", "content":"system"},
                {"from":{"path":"input.messages", "optional":true}}
            ]),
            BTreeSet::new(),
        );
        let context = context(input).with_templates(templates);
        let (control, _) = capturing_control();

        ChatNode.execute(&node, &context, &control).await.unwrap();

        let request = requests.lock().unwrap().pop().unwrap();
        assert_eq!(request.messages.len(), 1);
        assert_eq!(request.messages[0].role, ChatRole::System);
    }
}

#[tokio::test]
async fn dynamic_user_images_preserve_provider_shape() {
    let (node, templates, requests) = compile_chat_with_messages(
        json!([{"from":{
            "path":"input.messages",
            "allowed_content":["text", "image_url"]
        }}]),
        BTreeSet::from([ModelCapability::Vision]),
    );
    let context = context(json!({"messages":[{
        "role":"user",
        "content":[
            {"type":"text", "text":"look"},
            {"type":"image_url", "image_url":{"url":"http://example.test/a.png"}}
        ]
    }]}))
    .with_templates(templates);
    let (control, _) = capturing_control();

    ChatNode.execute(&node, &context, &control).await.unwrap();

    let request = requests.lock().unwrap().pop().unwrap();
    assert_eq!(request.messages[0].text(), Some("look"));
    assert_eq!(
        request.messages[0].image_urls(),
        vec!["http://example.test/a.png"]
    );
}

#[tokio::test]
async fn dynamic_messages_reject_an_empty_final_request() {
    let (node, templates, requests) = compile_chat_with_messages(
        json!([{"from":{"path":"input.messages", "optional":true}}]),
        BTreeSet::new(),
    );
    let context = context(json!({})).with_templates(templates);
    let (control, _) = capturing_control();

    let error = ChatNode.execute(&node, &context, &control).await.unwrap_err();

    assert_eq!(error.code(), "CHAT_MESSAGES_EMPTY");
    assert!(requests.lock().unwrap().is_empty());
}
```

Add one table-driven failure test. Every row uses a real JSON source rather than a parser mock:

```rust
#[tokio::test]
async fn dynamic_messages_reject_invalid_sources_without_leaking_bodies() {
    const SECRET: &str = "dynamic-message-secret";
let cases = [
        (json!({"path":"input.messages"}), json!({}), "CHAT_DYNAMIC_MESSAGES_SOURCE_MISSING"),
        (json!({"path":"input.messages"}), json!({"messages":null}), "CHAT_DYNAMIC_MESSAGES_INVALID"),
        (json!({"path":"input.messages"}), json!({"messages":{}}), "CHAT_DYNAMIC_MESSAGES_INVALID"),
        (
            json!({"path":"input.messages", "max_messages":1}),
            json!({"messages":[
                {"role":"user", "content":"one"},
                {"role":"assistant", "content":"two"}
            ]}),
            "CHAT_DYNAMIC_MESSAGES_LIMIT_EXCEEDED",
        ),
        (
            json!({"path":"input.messages", "max_bytes":2}),
            json!({"messages":[{"role":"user", "content":SECRET}]}),
            "CHAT_DYNAMIC_MESSAGES_TOO_LARGE",
        ),
        (
            json!({"path":"input.messages"}),
            json!({"messages":[{"role":"system", "content":SECRET}]}),
            "CHAT_DYNAMIC_MESSAGES_INVALID",
        ),
        (
            json!({"path":"input.messages"}),
            json!({"messages":[{"role":"tool", "content":SECRET}]}),
            "CHAT_DYNAMIC_MESSAGES_INVALID",
        ),
        (
            json!({"path":"input.messages"}),
            json!({"messages":[{"role":"user", "content":42}]}),
            "CHAT_DYNAMIC_MESSAGES_INVALID",
        ),
        (
            json!({"path":"input.messages"}),
            json!({"messages":[{"role":"user", "content":[]}]}),
            "CHAT_DYNAMIC_MESSAGES_INVALID",
        ),
        (
            json!({"path":"input.messages"}),
            json!({"messages":[{"role":"user", "content":[
                {"type":"image_url", "image_url":{"url":SECRET}}
            ]}]}),
            "CHAT_DYNAMIC_MESSAGES_INVALID",
        ),
        (
            json!({"path":"input.messages", "allowed_content":["image_url"]}),
            json!({"messages":[{"role":"assistant", "content":[
                {"type":"image_url", "image_url":{"url":SECRET}}
            ]}]}),
            "CHAT_DYNAMIC_MESSAGES_INVALID",
        ),
        (
            json!({"path":"input.messages", "allowed_content":["image_url"]}),
            json!({"messages":[{"role":"user", "content":[
                {"type":"image_url", "image_url":{"url":"   "}}
            ]}]}),
            "CHAT_DYNAMIC_MESSAGES_INVALID",
        ),
        (
            json!({"path":"input.messages"}),
            json!({"messages":[{"role":"user", "content":SECRET, "optional":true}]}),
            "CHAT_DYNAMIC_MESSAGES_INVALID",
        ),
        (
            json!({"path":"input.messages"}),
            json!({"messages":[{"role":"user", "content":[
                {"type":"text", "text":SECRET, "optional":true}
            ]}]}),
            "CHAT_DYNAMIC_MESSAGES_INVALID",
        ),
];

    for (from, input, expected_code) in cases {
        let (node, templates, requests) = compile_chat_with_messages(
            json!([{"from":from}]),
            BTreeSet::from([ModelCapability::Vision]),
        );
        let context = context(input).with_templates(templates);
        let (control, _) = capturing_control();

        let error = ChatNode.execute(&node, &context, &control).await.unwrap_err();

        assert_eq!(error.code(), expected_code);
        assert!(!format!("{error:?} {error}").contains(SECRET));
        assert!(requests.lock().unwrap().is_empty());
    }
}

#[tokio::test]
async fn dynamic_messages_keep_empty_text_for_compatibility() {
    let (node, templates, requests) = compile_chat_with_messages(
        json!([{"from":{"path":"input.messages"}}]),
        BTreeSet::new(),
    );
    let context = context(json!({
        "messages":[{"role":"user", "content":""}]
    }))
    .with_templates(templates);
    let (control, _) = capturing_control();

    ChatNode.execute(&node, &context, &control).await.unwrap();

    let request = requests.lock().unwrap().pop().unwrap();
    assert_eq!(request.messages[0].text(), Some(""));
}
```

- [ ] **Step 2: Write failing configuration, capability, and graph tests**

Add a compile-only helper and exact rejection coverage in `tests/core_chat_action.rs`:

```rust
fn compile_messages_result(
    messages: Value,
    capabilities: BTreeSet<ModelCapability>,
) -> Result<NodeCompilation, insight_agent_platform::dsl::CompileError> {
    let mut models = ModelRegistry::default();
    models
        .register(
            "primary",
            RecordingModel {
                requests: Arc::new(Mutex::new(Vec::new())),
                capabilities,
            },
        )
        .unwrap();
    let actions = ActionRegistry::default();
    let mut context = CompileContext::new(&models, &actions);
    ChatNode.compile(
        "answer",
        json!({"model":"primary", "messages":messages}),
        &mut context,
    )
}

#[test]
fn dynamic_message_sources_validate_paths_configuration_and_capabilities() {
for path in ["input", "nodes.answer", "nodes.answer.input.messages", "input.items[0]"] {
        assert_compile_error(
            compile_messages_result(
                json!([{"from":{"path":path}}]),
                BTreeSet::new(),
            ),
            "CHAT_DYNAMIC_MESSAGES_PATH_INVALID",
        );
}

for from in [
    json!({"path":"input.messages", "max_messages":0}),
    json!({"path":"input.messages", "max_bytes":0}),
    json!({"path":"input.messages", "allowed_content":[]}),
    json!({"path":"input.messages", "allowed_content":["input_audio"]}),
] {
        assert_compile_error(
            compile_messages_result(json!([{"from":from}]), BTreeSet::new()),
            "CHAT_DYNAMIC_MESSAGES_CONFIG_INVALID",
        );
}

    compile_messages_result(
        json!([{"from":{"path":"input.messages"}}]),
        BTreeSet::new(),
    )
    .unwrap();
    assert_compile_error(
        compile_messages_result(
            json!([{"from":{
                "path":"input.messages",
                "allowed_content":["image_url"]
            }}]),
            BTreeSet::new(),
        ),
        "MODEL_CAPABILITY_REQUIRED",
    );
    assert_compile_error(
        compile_messages_result(
            json!([{"from":{"path":"input.messages", "extra":true}}]),
            BTreeSet::new(),
        ),
        "CHAT_DYNAMIC_MESSAGES_CONFIG_INVALID",
    );
    assert_compile_error(
        compile_messages_result(
            json!([{
                "from":{"path":"input.messages"},
                "role":"user"
            }]),
            BTreeSet::new(),
        ),
        "CHAT_DYNAMIC_MESSAGES_CONFIG_INVALID",
    );
    assert_compile_error(
        compile_messages_result(
            json!([{"role":"user", "content":"hi", "extra":true}]),
            BTreeSet::new(),
        ),
        "NODE_CONFIG_INVALID",
    );
}
```

In `tests/formal_agent_compile.rs`, replace the `prepare` value and the static user entry in `complete_formal_agent_with_all_core_nodes_compiles` with:

```yaml
  prepare:
    type: core.template
    next: answer
    config:
      value:
        messages:
          - role: user
            content: "{{ input.question }}"
  answer:
    type: core.chat
    next: classify
    config:
      model: primary
      messages:
        - role: system
          content: "You are concise."
        - from:
            path: nodes.prepare.output.messages
      parameters: {}
```

After the existing region assertions, add:

```rust
assert!(agent.nodes["answer"].references.contains("prepare"));
```

In `tests/dsl_parallel.rs`, add these imports (merge the grouped imports with the existing ones):

```rust
use async_trait::async_trait;
use futures::stream;
use insight_agent_platform::{
    dsl::{
        compiled::{JoinPolicy, NodeRegion},
        compiler::{AgentCompiler, CompileLimits},
        CompileError,
    },
    nodes::default_node_registries,
    resources::{
        actions::ActionRegistry,
        models::{
            ChatChunk, ChatModel, ChatRequest, ChatStream, ModelCapability, ModelRegistry,
        },
    },
    runtime::RunError,
};
use serde_json::Value;
```

Add this text-only graph model:

```rust
#[derive(Debug)]
struct GraphModel;

#[async_trait]
impl ChatModel for GraphModel {
    fn capabilities(&self) -> BTreeSet<ModelCapability> {
        BTreeSet::new()
    }

    fn validate_parameters(&self, parameters: &Value) -> Result<(), CompileError> {
        if parameters.is_object() {
            Ok(())
        } else {
            Err(CompileError::new(
                "MODEL_PARAMETERS_INVALID",
                "parameters must be an object",
            ))
        }
    }

    async fn stream_chat(&self, _request: ChatRequest) -> Result<ChatStream, RunError> {
        Ok(Box::pin(stream::empty::<Result<ChatChunk, RunError>>()))
    }
}
```

Replace `compiler()` with the exact registration:

```rust
fn compiler() -> AgentCompiler {
    let (node_types, _) = default_node_registries().unwrap();
    let mut models = ModelRegistry::default();
    models.register("graph", GraphModel).unwrap();
    AgentCompiler::new(
        node_types,
        models,
        ActionRegistry::default(),
        Duration::from_secs(30),
        CompileLimits {
            max_fork_branches: 32,
        },
    )
}
```

Then add both graph cases:

```rust
#[test]
fn dynamic_chat_rejects_sibling_branch_source() {
    assert_compile_error(
        r#"
version: 1
id: sibling-dynamic-source
name: Sibling Dynamic Source
input:
  schema: {type: object}
entry: fanout
nodes:
  fanout:
    type: core.fork
    config:
      branches: {answer: answer, prepare: prepare}
      join: collect
  answer:
    type: core.chat
    next: collect
    config:
      model: graph
      messages:
        - from: {path: nodes.prepare.output}
  prepare:
    type: core.template
    next: collect
    config:
      value:
        - {role: user, content: sibling}
  collect:
    type: core.join
    next: result
    config: {mode: all_settled}
  result:
    type: core.output
    config: {data: {ok: true}}
"#,
        "INVALID_NODE_REFERENCE",
    );
}

#[test]
fn dynamic_chat_rejects_future_linear_source() {
    assert_compile_error(
        r#"
version: 1
id: future-dynamic-source
name: Future Dynamic Source
input:
  schema: {type: object}
entry: answer
nodes:
  answer:
    type: core.chat
    next: prepare
    config:
      model: graph
      messages:
        - from: {path: nodes.prepare.output.messages}
  prepare:
    type: core.template
    next: result
    config:
      value:
        messages:
          - {role: user, content: future}
  result:
    type: core.output
    config: {data: {ok: true}}
"#,
        "INVALID_NODE_REFERENCE",
    );
}
```

- [ ] **Step 3: Write failing observability/privacy tests**

Add the secrets beside the existing observability constants:

```rust
const DYNAMIC_TEXT_SECRET: &str = "observability-dynamic-text-secret";
const DYNAMIC_IMAGE_SECRET: &str = "observability-dynamic-image-secret";
const DYNAMIC_MESSAGE_SECRET: &str = "observability-invalid-dynamic-secret";
```

Inside `fixture`, add these agents before registries are built:

```rust
write_agent(
    root.path(),
    "chat_dynamic",
    r#"entry: answer
nodes:
  answer:
    type: core.chat
    next: result
    config:
      model: obs
      messages:
        - role: system
          content: system
        - from:
            path: input.messages
            allowed_content: [text, image_url]
  result:
    type: core.output
    config:
      data:
        text: "{{ nodes.answer.output.text }}"
"#,
);
write_agent(
    root.path(),
    "chat_dynamic_invalid",
    r#"entry: answer
nodes:
  answer:
    type: core.chat
    next: result
    config:
      model: obs
      messages:
        - from: {path: input.messages}
  result:
    type: core.output
    config: {data: {ok: true}}
"#,
);
```

Add both end-to-end logging tests:

```rust
#[tokio::test]
async fn dynamic_chat_logs_final_counts_without_bodies() {
    let _guard = reset_logs().await;
    let fixture = fixture(&["chat_dynamic"]).await;
    let created = fixture
        .service
        .create_detached(
            "chat_dynamic",
            json!({"messages":[
                {"role":"assistant", "content":DYNAMIC_TEXT_SECRET},
                {"role":"user", "content":[
                    {"type":"text", "text":"look"},
                    {"type":"image_url", "image_url":{
                        "url":format!("https://example.test/{DYNAMIC_IMAGE_SECRET}.png")
                    }}
                ]}
            ]}),
            RequestMetadata::default(),
        )
        .await
        .unwrap();
    wait_for_status(&fixture.service, &created.run_id, RunStatus::Completed).await;

    assert_eq!(fixture.model_calls.load(Ordering::Relaxed), 1);
    let requests = info_logs("chat.request");
    assert_eq!(requests.len(), 1);
    assert_eq!(requests[0].field("messages_count"), Some("3"));
    assert_eq!(requests[0].field("image_parts_count"), Some("1"));
    assert_logs_exclude(&[
        DYNAMIC_TEXT_SECRET,
        DYNAMIC_IMAGE_SECRET,
        CHAT_RESPONSE_SECRET,
    ]);
}

#[tokio::test]
async fn invalid_dynamic_chat_logs_no_request_or_body() {
    let _guard = reset_logs().await;
    let fixture = fixture(&["chat_dynamic_invalid"]).await;
    let created = fixture
        .service
        .create_detached(
            "chat_dynamic_invalid",
            json!({"messages":[{
                "role":"user",
                "content":DYNAMIC_MESSAGE_SECRET,
                "extra":true
            }]}),
            RequestMetadata::default(),
        )
        .await
        .unwrap();
    wait_for_status(&fixture.service, &created.run_id, RunStatus::Failed).await;

    let failed = fixture.service.get_run(&created.run_id).await.unwrap();
    assert_eq!(
        failed.error_code.as_deref(),
        Some("CHAT_DYNAMIC_MESSAGES_INVALID")
    );
    assert_eq!(fixture.model_calls.load(Ordering::Relaxed), 0);
    assert!(info_logs("chat.request").is_empty());
    assert!(info_logs("chat.response").is_empty());
    assert_logs_exclude(&[DYNAMIC_MESSAGE_SECRET]);
}
```

- [ ] **Step 4: Run RED and verify behavioral failures**

Run:

```bash
cargo test --test core_chat_action dynamic_messages -- --nocapture
cargo test --test formal_agent_compile -- --nocapture
cargo test --test dsl_parallel dynamic -- --nocapture
cargo test --test observability dynamic -- --nocapture
```

Expected: tests compile and fail because `{from: ...}` is currently parsed as an invalid static message. Confirm `NODE_CONFIG_INVALID` or missing expected behavior, not Rust compilation errors.

- [ ] **Step 5: Create the focused dynamic-source module**

Create `src/nodes/chat/dynamic.rs` with the complete implementation below:

```rust
use std::collections::BTreeSet;

use serde::Deserialize;
use serde_json::Value;

use crate::{
    dsl::{references::is_dsl_identifier, CompileError},
    resources::models::{
        ChatContent, ChatContentPart, ChatMessage, ChatRole, ImageUrl,
    },
    runtime::{RunContext, RunError},
};

pub(super) const DEFAULT_MAX_MESSAGES: usize = 50;
pub(super) const DEFAULT_MAX_BYTES: usize = 262_144;

fn default_max_messages() -> usize {
    DEFAULT_MAX_MESSAGES
}

fn default_max_bytes() -> usize {
    DEFAULT_MAX_BYTES
}

fn default_allowed_content() -> Vec<String> {
    vec!["text".to_string()]
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct DynamicMessageEntryConfig {
    pub(super) from: DynamicMessagesConfig,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct DynamicMessagesConfig {
    pub(super) path: String,
    #[serde(default)]
    pub(super) optional: bool,
    #[serde(default = "default_max_messages")]
    pub(super) max_messages: usize,
    #[serde(default = "default_max_bytes")]
    pub(super) max_bytes: usize,
    #[serde(default = "default_allowed_content")]
    pub(super) allowed_content: Vec<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
enum DynamicContentKind {
    Text,
    ImageUrl,
}

#[derive(Debug)]
pub(super) struct CompiledDynamicMessages {
    path: DynamicSourcePath,
    optional: bool,
    max_messages: usize,
    max_bytes: usize,
    allowed_content: BTreeSet<DynamicContentKind>,
}

impl CompiledDynamicMessages {
    pub(super) fn compile(
        config: DynamicMessagesConfig,
        node_id: &str,
        entry_index: usize,
    ) -> Result<Self, CompileError> {
        if config.max_messages == 0 || config.max_bytes == 0 {
            return Err(config_invalid(node_id, entry_index, "limits must be positive"));
        }
        if config.allowed_content.is_empty() {
            return Err(config_invalid(
                node_id,
                entry_index,
                "allowed_content must not be empty",
            ));
        }
        let mut allowed_content = BTreeSet::new();
        for kind in config.allowed_content {
            let kind = match kind.as_str() {
                "text" => DynamicContentKind::Text,
                "image_url" => DynamicContentKind::ImageUrl,
                _ => {
                    return Err(config_invalid(
                        node_id,
                        entry_index,
                        "allowed_content contains an unsupported kind",
                    ));
                }
            };
            allowed_content.insert(kind);
        }
        Ok(Self {
            path: DynamicSourcePath::parse(config.path, node_id, entry_index)?,
            optional: config.optional,
            max_messages: config.max_messages,
            max_bytes: config.max_bytes,
            allowed_content,
        })
    }

    pub(super) fn reference(&self) -> Option<&str> {
        match &self.path {
            DynamicSourcePath::Input { .. } => None,
            DynamicSourcePath::NodeOutput { node_id, .. } => Some(node_id),
        }
    }

    pub(super) fn requires_vision(&self) -> bool {
        self.allowed_content.contains(&DynamicContentKind::ImageUrl)
    }

    pub(super) fn expand(&self, context: &RunContext) -> Result<Vec<ChatMessage>, RunError> {
        let Some(source) = self.path.resolve(context) else {
            return if self.optional {
                Ok(Vec::new())
            } else {
                Err(self.source_missing())
            };
        };
        let array = source.as_array().ok_or_else(|| self.invalid_source())?;
        let bytes = serde_json::to_vec(source)
            .map_err(|_| self.invalid_source())?
            .len();
        if bytes > self.max_bytes {
            return Err(self.too_large());
        }
        if array.len() > self.max_messages {
            return Err(self.limit_exceeded());
        }

        array
            .iter()
            .enumerate()
            .map(|(message_index, value)| {
                let message: DynamicMessage = serde_json::from_value(value.clone())
                    .map_err(|_| self.invalid_message(message_index, "has invalid shape"))?;
                self.convert(message, message_index)
            })
            .collect()
    }

    fn convert(
        &self,
        message: DynamicMessage,
        message_index: usize,
    ) -> Result<ChatMessage, RunError> {
        if message.role == ChatRole::System {
            return Err(self.invalid_message(message_index, "uses the system role"));
        }
        let content = match message.content {
            DynamicContent::Text(text) => {
                self.require_kind(DynamicContentKind::Text, message_index, None)?;
                ChatContent::Text(text)
            }
            DynamicContent::Parts(parts) => {
                if parts.is_empty() {
                    return Err(self.invalid_message(message_index, "has no content parts"));
                }
                let mut converted = Vec::with_capacity(parts.len());
                for (part_index, part) in parts.into_iter().enumerate() {
                    match part {
                        DynamicPart::Text { text } => {
                            self.require_kind(
                                DynamicContentKind::Text,
                                message_index,
                                Some(part_index),
                            )?;
                            converted.push(ChatContentPart::Text { text });
                        }
                        DynamicPart::ImageUrl { image_url } => {
                            self.require_kind(
                                DynamicContentKind::ImageUrl,
                                message_index,
                                Some(part_index),
                            )?;
                            if message.role != ChatRole::User {
                                return Err(self.invalid_part(
                                    message_index,
                                    part_index,
                                    "image_url is allowed only for user messages",
                                ));
                            }
                            if image_url.url.trim().is_empty() {
                                return Err(self.invalid_part(
                                    message_index,
                                    part_index,
                                    "image_url must not be blank",
                                ));
                            }
                            converted.push(ChatContentPart::ImageUrl {
                                image_url: ImageUrl { url: image_url.url },
                            });
                        }
                    }
                }
                ChatContent::Parts(converted)
            }
        };
        Ok(ChatMessage {
            role: message.role,
            content,
        })
    }

    fn require_kind(
        &self,
        kind: DynamicContentKind,
        message_index: usize,
        part_index: Option<usize>,
    ) -> Result<(), RunError> {
        if self.allowed_content.contains(&kind) {
            return Ok(());
        }
        match part_index {
            Some(part_index) => Err(self.invalid_part(
                message_index,
                part_index,
                "content kind is not allowed",
            )),
            None => Err(self.invalid_message(message_index, "content kind is not allowed")),
        }
    }

    fn source_missing(&self) -> RunError {
        RunError::new(
            "CHAT_DYNAMIC_MESSAGES_SOURCE_MISSING",
            format!("dynamic message source '{}' is missing", self.path.canonical()),
        )
    }

    fn invalid_source(&self) -> RunError {
        RunError::new(
            "CHAT_DYNAMIC_MESSAGES_INVALID",
            format!("dynamic message source '{}' must be an array", self.path.canonical()),
        )
    }

    fn limit_exceeded(&self) -> RunError {
        RunError::new(
            "CHAT_DYNAMIC_MESSAGES_LIMIT_EXCEEDED",
            format!(
                "dynamic message source '{}' exceeds max_messages {}",
                self.path.canonical(),
                self.max_messages
            ),
        )
    }

    fn too_large(&self) -> RunError {
        RunError::new(
            "CHAT_DYNAMIC_MESSAGES_TOO_LARGE",
            format!(
                "dynamic message source '{}' exceeds max_bytes {}",
                self.path.canonical(),
                self.max_bytes
            ),
        )
    }

    fn invalid_message(&self, message_index: usize, rule: &str) -> RunError {
        RunError::new(
            "CHAT_DYNAMIC_MESSAGES_INVALID",
            format!(
                "dynamic message source '{}' message {} {rule}",
                self.path.canonical(),
                message_index
            ),
        )
    }

    fn invalid_part(&self, message_index: usize, part_index: usize, rule: &str) -> RunError {
        RunError::new(
            "CHAT_DYNAMIC_MESSAGES_INVALID",
            format!(
                "dynamic message source '{}' message {} part {} {rule}",
                self.path.canonical(),
                message_index,
                part_index
            ),
        )
    }
}

#[derive(Debug)]
enum DynamicSourcePath {
    Input { canonical: String, fields: Vec<String> },
    NodeOutput {
        canonical: String,
        node_id: String,
        fields: Vec<String>,
    },
}

impl DynamicSourcePath {
    fn parse(value: String, node_id: &str, entry_index: usize) -> Result<Self, CompileError> {
        let segments = value.split('.').collect::<Vec<_>>();
        if !segments.iter().all(|segment| is_dsl_identifier(segment)) {
            return Err(path_invalid(node_id, entry_index));
        }
        match segments.as_slice() {
            ["input", fields @ ..] if !fields.is_empty() => Ok(Self::Input {
                canonical: value.clone(),
                fields: fields.iter().map(|field| (*field).to_string()).collect(),
            }),
            ["nodes", source_node, "output", fields @ ..] => Ok(Self::NodeOutput {
                canonical: value.clone(),
                node_id: (*source_node).to_string(),
                fields: fields.iter().map(|field| (*field).to_string()).collect(),
            }),
            _ => Err(path_invalid(node_id, entry_index)),
        }
    }

    fn canonical(&self) -> &str {
        match self {
            Self::Input { canonical, .. } | Self::NodeOutput { canonical, .. } => canonical,
        }
    }

    fn resolve<'a>(&self, context: &'a RunContext) -> Option<&'a Value> {
        let (mut current, fields) = match self {
            Self::Input { fields, .. } => (context.input(), fields),
            Self::NodeOutput {
                node_id, fields, ..
            } => (context.node_output(node_id)?, fields),
        };
        for field in fields {
            current = current.as_object()?.get(field)?;
        }
        Some(current)
    }
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct DynamicMessage {
    role: ChatRole,
    content: DynamicContent,
}

#[derive(Debug, Deserialize)]
#[serde(untagged)]
enum DynamicContent {
    Text(String),
    Parts(Vec<DynamicPart>),
}

#[derive(Debug, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
enum DynamicPart {
    Text { text: String },
    ImageUrl { image_url: DynamicImageUrl },
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct DynamicImageUrl {
    url: String,
}

fn config_invalid(node_id: &str, entry_index: usize, rule: &str) -> CompileError {
    CompileError::new(
        "CHAT_DYNAMIC_MESSAGES_CONFIG_INVALID",
        format!("chat node '{node_id}' dynamic message entry {entry_index} {rule}"),
    )
}

fn path_invalid(node_id: &str, entry_index: usize) -> CompileError {
    CompileError::new(
        "CHAT_DYNAMIC_MESSAGES_PATH_INVALID",
        format!("chat node '{node_id}' dynamic message entry {entry_index} has an invalid path"),
    )
}
```

- [ ] **Step 6: Wire ordered static/dynamic entries into `chat.rs`**

Add:

```rust
mod dynamic;
use dynamic::{CompiledDynamicMessages, DynamicMessageEntryConfig};
```

Change only the `messages` field in `ChatConfig`, and introduce the ordered compiled enum:

```rust
struct ChatConfig {
    model: String,
    messages: Vec<Value>,
    #[serde(default = "empty_object")]
    parameters: Value,
}

enum CompiledMessageEntry {
    Static(CompiledMessage),
    Dynamic(CompiledDynamicMessages),
}

struct CompiledChat {
    model: Arc<dyn ChatModel>,
    messages: Vec<CompiledMessageEntry>,
    parameters: Value,
}
```

Replace the existing compilation loop with ordered entry dispatch. Dynamic configuration parse failures are intentionally generic so configuration values are not copied into error messages:

```rust
let mut references = BTreeSet::new();
let mut has_images = false;
let mut messages = Vec::with_capacity(config.messages.len());
for (entry_index, entry) in config.messages.into_iter().enumerate() {
    if entry
        .as_object()
        .is_some_and(|object| object.contains_key("from"))
    {
        let config: DynamicMessageEntryConfig = serde_json::from_value(entry).map_err(|_| {
            CompileError::new(
                "CHAT_DYNAMIC_MESSAGES_CONFIG_INVALID",
                format!(
                    "chat node '{node_id}' dynamic message entry {entry_index} has invalid configuration"
                ),
            )
        })?;
        let dynamic = CompiledDynamicMessages::compile(config.from, node_id, entry_index)?;
        if let Some(reference) = dynamic.reference() {
            references.insert(reference.to_string());
        }
        has_images |= dynamic.requires_vision();
        messages.push(CompiledMessageEntry::Dynamic(dynamic));
    } else {
        let message: MessageConfig = serde_json::from_value(entry).map_err(|error| {
            CompileError::new(
                "NODE_CONFIG_INVALID",
                format!(
                    "invalid core.chat message {entry_index} for node '{node_id}': {error}"
                ),
            )
        })?;
        let (message, message_references, message_has_images) =
            compile_static_message(message, context, node_id, entry_index)?;
        references.extend(message_references);
        has_images |= message_has_images;
        messages.push(CompiledMessageEntry::Static(message));
    }
}
```

Keep the existing Vision capability check after this loop. In `NodeCompilation`, store the new `messages` vector and unchanged `model`/`parameters`.

Add this helper immediately before `compile_text_source`; it is the existing static compilation logic moved without behavioral changes:

```rust
fn compile_static_message(
    message: MessageConfig,
    context: &mut CompileContext<'_>,
    node_id: &str,
    message_index: usize,
) -> Result<(CompiledMessage, BTreeSet<String>, bool), CompileError> {
    let mut references = BTreeSet::new();
    let mut has_images = false;
    let content = match message.content {
        MessageContentConfig::Text(source) => {
            let template = compile_text_source(
                TextSourceConfig::Text(source),
                context,
                node_id,
                &format!("messages[{message_index}].content"),
            )?;
            references.extend(template.references.iter().cloned());
            CompiledMessageContent::Text(template)
        }
        MessageContentConfig::TemplateRef(source) => {
            let template = compile_text_source(
                TextSourceConfig::TemplateRef(source),
                context,
                node_id,
                &format!("messages[{message_index}].content"),
            )?;
            references.extend(template.references.iter().cloned());
            CompiledMessageContent::Text(template)
        }
        MessageContentConfig::Parts(parts) => {
            if parts.is_empty() {
                return Err(CompileError::new(
                    "CHAT_CONTENT_PARTS_REQUIRED",
                    format!(
                        "chat node '{node_id}' message {message_index} must contain at least one part"
                    ),
                ));
            }
            let mut compiled_parts = Vec::with_capacity(parts.len());
            for (part_index, part) in parts.into_iter().enumerate() {
                match part {
                    MessagePartConfig::Text { text } => {
                        let template = compile_text_source(
                            text,
                            context,
                            node_id,
                            &format!("messages[{message_index}].parts[{part_index}].text"),
                        )?;
                        references.extend(template.references.iter().cloned());
                        compiled_parts.push(CompiledMessagePart::Text(template));
                    }
                    MessagePartConfig::ImageUrl {
                        image_url,
                        optional,
                    } => {
                        let template = compile_text_source(
                            TextSourceConfig::Text(image_url.url),
                            context,
                            node_id,
                            &format!(
                                "messages[{message_index}].parts[{part_index}].image_url.url"
                            ),
                        )?;
                        references.extend(template.references.iter().cloned());
                        has_images = true;
                        compiled_parts.push(CompiledMessagePart::ImageUrl { template, optional });
                    }
                }
            }
            CompiledMessageContent::Parts(compiled_parts)
        }
    };
    Ok((
        CompiledMessage {
            role: message.role,
            content,
        },
        references,
        has_images,
    ))
}
```

Replace runtime message collection with ordered flattening:

```rust
let mut messages = Vec::new();
for entry in &body.messages {
    match entry {
        CompiledMessageEntry::Static(message) => {
            messages.push(message.render(context, &data)?);
        }
        CompiledMessageEntry::Dynamic(dynamic) => {
            messages.extend(dynamic.expand(context)?);
        }
    }
}
if messages.is_empty() {
    return Err(RunError::new(
        "CHAT_MESSAGES_EMPTY",
        "chat messages are empty after dynamic sources were expanded",
    ));
}
```

This must execute before request metadata logging and `stream_chat`.

- [ ] **Step 7: Run focused GREEN and adjacent suites**

Run:

```bash
cargo test --test core_chat_action -- --nocapture
cargo test --test formal_agent_compile -- --nocapture
cargo test --test dsl_parallel -- --nocapture
cargo test --test observability -- --nocapture
cargo test --test repository_agents_v1 -- --nocapture
cargo test --test formal_resources openai_adapter_serializes_formal_messages_and_allowed_parameters -- --exact --nocapture
```

Expected: every target passes; static repository agents compile unchanged, graph validation rejects invalid node sources, final provider order is correct, and logs remain body-free.

- [ ] **Step 8: Run full verification**

Run:

```bash
cargo fmt --check
cargo test --all-targets
git diff --check
```

Expected: formatting is clean, all targets pass without new warnings, and there are no whitespace errors.

- [ ] **Step 9: Inspect scope and commit**

Run:

```bash
git diff --stat
git diff -- src/nodes/chat.rs src/nodes/chat/dynamic.rs \
  tests/core_chat_action.rs tests/formal_agent_compile.rs \
  tests/dsl_parallel.rs tests/observability.rs
git status --short
```

Expected: only the six planned files changed. `tests/repository_agents_v1.rs` passes unchanged. No agent YAML, provider production code, persistence code, or API code changed.

Commit:

```bash
git add src/nodes/chat.rs src/nodes/chat/dynamic.rs \
  tests/core_chat_action.rs tests/formal_agent_compile.rs \
  tests/dsl_parallel.rs tests/observability.rs
git commit -m "feat: support dynamic chat message sources"
```
