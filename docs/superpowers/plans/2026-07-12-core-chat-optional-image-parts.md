# Core Chat Optional Image Parts Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add opt-in omission for missing or blank `core.chat` image parts, then migrate the medical report agent to optional HTTP/HTTPS/data-image input.

**Architecture:** Extend only the `image_url` message-part variant with `optional: bool`, defaulting to false. Preserve strict rendering for required parts and non-missing errors; filter optional missing/blank images before model invocation, then adopt the capability in the repository-owned medical agent.

**Tech Stack:** Rust, Serde, Handlebars 6.4 structured render errors, JSON Schema Draft 7, YAML agent DSL, Tokio integration tests.

## Global Constraints

- `optional` is valid only on `type: image_url` parts and defaults to `false`.
- Existing image parts without `optional` retain their current behavior.
- Optional image parts omit `MissingVariable`, empty-string, and whitespace-only results.
- Optional image parts propagate every non-`MissingVariable` render error as `TEMPLATE_RENDER_FAILED`.
- A multipart message emptied by optional filtering fails with `CHAT_CONTENT_PARTS_EMPTY` before model invocation.
- Any configured image part, including an optional one, still requires a Vision-capable model.
- Medical `image_url` accepts an absent field, `""`, `http://`, `https://`, or `data:image/`; `null`, non-strings, and other schemes remain invalid.
- HTTP image URLs are passed through to the model provider; the platform does not fetch, resolve, probe, or inspect them.
- Initial-turn three-step output and follow-up one-step direct-answer output remain unchanged.

---

## File Structure

- Modify `src/nodes/chat.rs`: parse, compile, render, and filter optional image parts.
- Modify `tests/core_chat_action.rs`: cover compatibility, omission, URL preservation, errors, empty multipart messages, and capability validation.
- Modify `agents/medical_report_interpreter/agent.yaml`: make `image_url` optional, accept HTTP/blank values, and mark four image parts optional.
- Modify `agents/medical_report_interpreter/prompts/abnormal_indicators.md`: make image wording conditional.
- Modify `agents/medical_report_interpreter/prompts/follow_up.md`: make image wording conditional.
- Modify `tests/medical_report_follow_up.rs`: validate schema and execute missing, blank, and HTTP image cases.

### Task 1: Core Chat Optional Image Part Contract

**Files:**
- Modify: `tests/core_chat_action.rs:180-404`
- Modify: `src/nodes/chat.rs:68-103`
- Modify: `src/nodes/chat.rs:165-197`
- Modify: `src/nodes/chat.rs:364-407`

**Interfaces:**
- Consumes: `handlebars::RenderErrorReason::MissingVariable`, `TemplateProgram`, and existing image-part serialization.
- Produces: `MessagePartConfig::ImageUrl { image_url, optional }`, compiled optional metadata, and runtime error `CHAT_CONTENT_PARTS_EMPTY`.

- [ ] **Step 1: Write failing core chat tests**

Add a helper in `tests/core_chat_action.rs` that compiles one user message from supplied parts and returns the node, compiled templates, and recording-model requests:

```rust
fn compile_chat_with_parts(
    parts: Value,
) -> (CompiledNode, Arc<Handlebars<'static>>, Arc<Mutex<Vec<ChatRequest>>>) {
    let model = RecordingModel::vision();
    let requests = Arc::clone(&model.requests);
    let mut models = ModelRegistry::default();
    models.register("primary", model).unwrap();
    let actions = ActionRegistry::default();
    let mut compile_context = CompileContext::new(&models, &actions);
    let compilation = ChatNode
        .compile(
            "answer",
            json!({
                "model":"primary",
                "messages":[{"role":"user", "content":parts}]
            }),
            &mut compile_context,
        )
        .unwrap();
    (
        compiled_node("answer", "core.chat", EmitPolicy::None, compilation),
        Arc::new(compile_context.into_templates()),
        requests,
    )
}
```

Import `handlebars::Handlebars`. Add these behavioral cases using the existing `context` and `capturing_control` helpers:

```rust
#[tokio::test]
async fn optional_image_parts_omit_missing_empty_and_blank_values() {
    for input in [json!({}), json!({"image_url":""}), json!({"image_url":"   "})] {
        let (node, templates, requests) = compile_chat_with_parts(json!([
            {"type":"text", "text":"question"},
            {"type":"image_url", "optional":true,
             "image_url":{"url":"{{ input.image_url }}"}}
        ]));
        let context = context(input).with_templates(templates);
        let (control, _) = capturing_control();
        ChatNode.execute(&node, &context, &control).await.unwrap();
        let request = requests.lock().unwrap().pop().unwrap();
        assert_eq!(request.messages[0].text(), Some("question"));
        assert!(request.messages[0].image_urls().is_empty());
    }
}

#[tokio::test]
async fn optional_image_parts_preserve_non_blank_urls() {
    for url in [
        "http://example.test/report.png",
        "https://example.test/report.png",
        "data:image/png;base64,AA==",
    ] {
        let (node, templates, requests) = compile_chat_with_parts(json!([
            {"type":"text", "text":"question"},
            {"type":"image_url", "optional":true,
             "image_url":{"url":"{{ input.image_url }}"}}
        ]));
        let context = context(json!({"image_url":url})).with_templates(templates);
        let (control, _) = capturing_control();
        ChatNode.execute(&node, &context, &control).await.unwrap();
        let request = requests.lock().unwrap().pop().unwrap();
        assert_eq!(request.messages[0].image_urls(), vec![url]);
    }
}

#[tokio::test]
async fn required_image_parts_still_fail_for_missing_values() {
    let (node, templates, requests) = compile_chat_with_parts(json!([
        {"type":"text", "text":"question"},
        {"type":"image_url", "image_url":{"url":"{{ input.image_url }}"}}
    ]));
    let context = context(json!({})).with_templates(templates);
    let (control, _) = capturing_control();
    let error = ChatNode.execute(&node, &context, &control).await.unwrap_err();
    assert_eq!(error.code(), "TEMPLATE_RENDER_FAILED");
    assert!(requests.lock().unwrap().is_empty());
}

#[tokio::test]
async fn optional_image_parts_preserve_non_missing_render_errors() {
    let (node, templates, requests) = compile_chat_with_parts(json!([
        {"type":"text", "text":"question"},
        {"type":"image_url", "optional":true,
         "image_url":{"url":"{{#if}}{{/if}}"}}
    ]));
    let context = context(json!({})).with_templates(templates);
    let (control, _) = capturing_control();
    let error = ChatNode.execute(&node, &context, &control).await.unwrap_err();
    assert_eq!(error.code(), "TEMPLATE_RENDER_FAILED");
    assert!(requests.lock().unwrap().is_empty());
}

#[tokio::test]
async fn optional_image_parts_reject_messages_emptied_by_filtering() {
    let (node, templates, requests) = compile_chat_with_parts(json!([
        {"type":"image_url", "optional":true,
         "image_url":{"url":"{{ input.image_url }}"}}
    ]));
    let context = context(json!({})).with_templates(templates);
    let (control, _) = capturing_control();
    let error = ChatNode.execute(&node, &context, &control).await.unwrap_err();
    assert_eq!(error.code(), "CHAT_CONTENT_PARTS_EMPTY");
    assert!(requests.lock().unwrap().is_empty());
}
```

Extend the existing compile-rejection test with:

```rust
assert_compile_error(
    ChatNode.compile(
        "chat",
        json!({"model":"text", "messages":[{"role":"user", "content":[
            {"type":"image_url", "optional":true,
             "image_url":{"url":"{{ input.image_url }}"}}
        ]}]}),
        &mut context,
    ),
    "MODEL_CAPABILITY_REQUIRED",
);
assert_compile_error(
    ChatNode.compile(
        "chat",
        json!({"model":"text", "messages":[{"role":"user", "content":[
            {"type":"text", "optional":true, "text":"hello"}
        ]}]}),
        &mut context,
    ),
    "NODE_CONFIG_INVALID",
);
```

- [ ] **Step 2: Run core tests and verify RED**

Run: `cargo test --test core_chat_action optional_image -- --nocapture`

Expected: tests fail because `optional` is currently rejected as an unknown image-part field. Confirm the failure is behavioral rather than a test compilation error.

- [ ] **Step 3: Implement parsing and compilation**

In `src/nodes/chat.rs`, import `handlebars::{RenderError, RenderErrorReason}` and change the variants to:

```rust
enum MessagePartConfig {
    Text { text: TextSourceConfig },
    ImageUrl {
        image_url: ImageUrlConfig,
        #[serde(default)]
        optional: bool,
    },
}

enum CompiledMessagePart {
    Text(TemplateProgram),
    ImageUrl {
        template: TemplateProgram,
        optional: bool,
    },
}
```

Compile text and image variants explicitly. For the image variant, compile `image_url.url`, extend references, set `has_images = true`, and push `CompiledMessagePart::ImageUrl { template, optional }`. Do not change Vision capability validation.

- [ ] **Step 4: Implement structured optional rendering**

Add raw and mapped rendering helpers:

```rust
fn render_raw_template(
    context: &RunContext,
    template: &TemplateProgram,
    data: &Value,
) -> Result<String, RenderError> {
    context.templates().render(&template.name, data)
}

fn template_render_error(template: &TemplateProgram, error: RenderError) -> RunError {
    RunError::new(
        "TEMPLATE_RENDER_FAILED",
        format!("failed to render template '{}': {error}", template.name),
    )
}
```

Keep required rendering mapped through `template_render_error`. Render multipart content with a loop. Text always renders and is pushed. For images, apply:

```rust
match render_raw_template(context, template, data) {
    Ok(url) if *optional && url.trim().is_empty() => {}
    Ok(url) => rendered.push(ChatContentPart::ImageUrl {
        image_url: ImageUrl { url },
    }),
    Err(error)
        if *optional
            && matches!(error.reason(), RenderErrorReason::MissingVariable(_)) => {}
    Err(error) => return Err(template_render_error(template, error)),
}
```

After filtering, return:

```rust
if rendered.is_empty() {
    return Err(RunError::new(
        "CHAT_CONTENT_PARTS_EMPTY",
        "chat message has no content parts after optional parts were omitted",
    ));
}
```

- [ ] **Step 5: Run focused and adjacent tests**

Run:

```bash
cargo test --test core_chat_action -- --nocapture
cargo test --test observability -- --nocapture
```

Expected: all tests pass and request metadata still counts only rendered image parts.

- [ ] **Step 6: Format, inspect, and commit Task 1**

Run:

```bash
cargo fmt --check
git diff --check
git diff -- src/nodes/chat.rs tests/core_chat_action.rs
```

Commit:

```bash
git add src/nodes/chat.rs tests/core_chat_action.rs
git commit -m "feat: support optional chat image parts"
```

### Task 2: Medical Agent Optional HTTP Images

**Files:**
- Modify: `agents/medical_report_interpreter/agent.yaml:6-139`
- Modify: `agents/medical_report_interpreter/prompts/abnormal_indicators.md:9`
- Modify: `agents/medical_report_interpreter/prompts/follow_up.md:8`
- Modify: `tests/medical_report_follow_up.rs:135-288`

**Interfaces:**
- Consumes: Task 1's optional image contract and `JsonSchemaValidator::is_valid`.
- Produces: medical input acceptance for missing, blank, HTTP, HTTPS, and data-image values while retaining existing outputs.

- [ ] **Step 1: Write failing medical schema and execution tests**

Add:

```rust
fn medical_input(image_url: Option<Value>, messages: Value) -> Value {
    let mut input = json!({
        "report_text":"血红蛋白偏低",
        "messages":messages,
        "question":"请解读报告"
    });
    if let Some(image_url) = image_url {
        input["image_url"] = image_url;
    }
    input
}
```

Add schema acceptance and rejection:

```rust
#[test]
fn medical_image_schema_accepts_optional_http_images_and_rejects_invalid_values() {
    let (agent, _) = compile_agent();
    for image_url in [
        None,
        Some(json!("")),
        Some(json!("http://example.test/report.png")),
        Some(json!("https://example.test/report.png")),
        Some(json!("data:image/png;base64,AA==")),
    ] {
        assert!(agent.input_schema.is_valid(&medical_input(image_url, json!([]))));
    }
    for image_url in [
        json!(null),
        json!(7),
        json!("ftp://example.test/report.png"),
        json!("file:///tmp/report.png"),
    ] {
        assert!(!agent
            .input_schema
            .is_valid(&medical_input(Some(image_url), json!([]))));
    }
}
```

Add the text-only execution cases:

```rust
#[tokio::test]
async fn missing_image_runs_initial_flow_with_text_only_messages() {
    let (agent, requests) = compile_agent();
    let output = run_agent(
        agent,
        "run_initial_without_image",
        medical_input(None, json!([])),
    )
    .await;

    let requests = requests.lock().unwrap();
    assert_eq!(requests.len(), 3);
    assert!(requests
        .iter()
        .all(|request| request.messages[1].image_urls().is_empty()));
    assert_eq!(
        output.content.as_deref(),
        Some("异常指标响应\n\n综合解读响应\n\n健康建议响应")
    );
    assert_eq!(output.format.as_deref(), Some("markdown"));
    assert_eq!(
        output.data,
        json!({
            "abnormal_indicators": ABNORMAL_RESPONSE,
            "comprehensive_interpretation": COMPREHENSIVE_RESPONSE,
            "health_advice": ADVICE_RESPONSE,
        })
    );
}

#[tokio::test]
async fn missing_and_blank_images_run_follow_up_with_one_text_only_message() {
    for (run_id, image_url) in [
        ("run_follow_up_without_image", None),
        ("run_follow_up_with_blank_image", Some(json!(""))),
    ] {
        let (agent, requests) = compile_agent();
        let output = run_agent(
            agent,
            run_id,
            medical_input(
                image_url,
                json!([{"role":"user", "content":"请解读报告"}]),
            ),
        )
        .await;

        let requests = requests.lock().unwrap();
        assert_eq!(requests.len(), 1);
        assert!(requests[0].messages[1].image_urls().is_empty());
        assert_eq!(output.content.as_deref(), Some(FOLLOW_UP_RESPONSE));
        assert_eq!(output.format.as_deref(), Some("markdown"));
        assert!(output.data.is_null());
    }
}
```

Change the existing initial-turn image to HTTP and retain exact propagation assertions:

```rust
"image_url": "http://example.test/report.png",
```

```rust
assert_eq!(
    user_message.image_urls(),
    vec!["http://example.test/report.png"]
);
```

- [ ] **Step 2: Run medical tests and verify RED**

Run: `cargo test --test medical_report_follow_up -- --nocapture`

Expected: the schema test fails for missing, blank, and HTTP input under the current required HTTPS/data schema; execution cannot yet produce the text-only requests.

- [ ] **Step 3: Update schema and image parts**

Change the schema fragment in `agent.yaml` to:

```yaml
required: [report_text, messages, question]
additionalProperties: false
properties:
  report_text:
    type: string
  image_url:
    type: string
    pattern: "^(http://|https://|data:image/|$)"
```

For `follow_up`, `abnormal_indicators`, `comprehensive_interpretation`, and `health_advice`, use:

```yaml
- type: image_url
  optional: true
  image_url:
    url: "{{ input.image_url }}"
```

Do not change routing, prompt references, models, parameters, transitions, or result templates.

- [ ] **Step 4: Make prompt image wording conditional**

Set the abnormal prompt image line to:

```markdown
图片：如果本条消息提供了图片，请结合其中可见的报告名称、项目、结果、单位、参考范围和异常标记；未提供图片时只依据报告文本和对话信息。
```

Set the follow-up prompt image line to:

```markdown
图片：如果本条消息提供了图片，需要时可结合其中可见的报告信息回答；未提供图片时只依据报告文本和历史对话。
```

- [ ] **Step 5: Run focused repository-agent tests**

Run:

```bash
cargo test --test medical_report_follow_up --test repository_agents_v1 -- --nocapture
```

Expected: missing and blank values produce text-only requests, HTTP is preserved, both output contracts pass, and repository production-registry compilation succeeds.

- [ ] **Step 6: Run full verification**

Run:

```bash
cargo fmt --check
cargo test --all-targets
git diff --check
```

Expected: formatting is clean, every target passes, and no whitespace errors are present.

- [ ] **Step 7: Inspect and commit Task 2**

Run `git status --short` and inspect the diff for the four Task 2 files. Commit:

```bash
git add agents/medical_report_interpreter/agent.yaml \
  agents/medical_report_interpreter/prompts/abnormal_indicators.md \
  agents/medical_report_interpreter/prompts/follow_up.md \
  tests/medical_report_follow_up.rs
git commit -m "feat: allow optional medical report images"
```
