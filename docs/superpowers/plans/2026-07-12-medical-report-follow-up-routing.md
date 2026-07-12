# Medical Report Follow-up Routing Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Route medical-report follow-up turns through one dedicated prompt and model call while preserving the existing three-step initial-turn flow.

**Architecture:** Add a `core.condition` entry node to classify turns exclusively by whether `input.messages` is empty. The follow-up branch uses one new `core.chat` node and its own `core.output`; the default branch retains the existing three chat nodes and result composition.

**Tech Stack:** Rust integration tests, Tokio, async-trait, CEL-backed `core.condition`, Handlebars prompts, YAML agent DSL, existing `Scheduler` and `ChatModel` interfaces.

## Global Constraints

- An empty `input.messages` array is always an initial turn.
- A non-empty `input.messages` array is always a follow-up turn.
- Follow-up responses execute exactly one chat node and do not execute `abnormal_indicators`, `comprehensive_interpretation`, or `health_advice`.
- Follow-up responses answer `input.question` directly and contain no section titles.
- Initial-turn prompts, emitted content, Markdown composition, and structured result fields remain unchanged.
- Do not change the input schema, HTTP API, runtime node implementations, or caller behavior.

---

## File Structure

- Create `tests/medical_report_follow_up.rs`: compile and execute the repository-owned agent with a recording vision model; assert both routing paths, prompt rendering, model-call counts, image propagation, and final output shape.
- Create `agents/medical_report_interpreter/prompts/follow_up.md`: define the dedicated follow-up behavior and output restrictions.
- Modify `agents/medical_report_interpreter/agent.yaml`: add the prompt, condition entry, follow-up chat node, and follow-up output node while preserving the initial path.

### Task 1: Dedicated Follow-up Route

**Files:**
- Create: `tests/medical_report_follow_up.rs`
- Create: `agents/medical_report_interpreter/prompts/follow_up.md`
- Modify: `agents/medical_report_interpreter/agent.yaml:32-118`

**Interfaces:**
- Consumes: `AgentCompiler::compile_dir`, the production node registries, `Scheduler::run`, `ChatModel::stream_chat`, and the existing medical-report input schema.
- Produces: agent nodes named `route`, `follow_up`, and `follow_up_result`; a prompt reference named `follow_up`; follow-up `RunOutput { content: Some(_), format: Some("markdown"), data: Value::Null }`.

- [ ] **Step 1: Write the failing integration tests**

Create `tests/medical_report_follow_up.rs` with a recording vision model and a no-op event repository. Compile the real agent directory and execute it through the production scheduler:

```rust
use std::{
    collections::BTreeSet,
    fmt,
    path::Path,
    sync::{Arc, Mutex},
    time::Duration,
};

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use futures::stream;
use insight_agent_platform::{
    dsl::{
        compiled::RunOutput,
        compiler::{AgentCompiler, CompileLimits},
    },
    events::{
        hub::{EventHub, EventHubConfig},
        protocol::RunEvent,
    },
    history::{
        repository::{HistoryError, RunRepository},
        types::{NewRun, NodeOutputRecord, RunRecord, TerminalUpdate},
    },
    nodes::default_node_registries,
    resources::{
        actions::ActionRegistry,
        models::{
            ChatChunk, ChatModel, ChatRequest, ChatStream, ModelCapability, ModelRegistry,
        },
    },
    runtime::{
        stop_pair, CompiledAgentRegistry, ExecutionLimiter, RunContext, RunError, RunMetadata,
        Scheduler, SchedulerResult,
    },
};
use serde_json::{json, Value};
use tokio::sync::Semaphore;

#[derive(Clone)]
struct RecordingVisionModel {
    requests: Arc<Mutex<Vec<ChatRequest>>>,
}

impl fmt::Debug for RecordingVisionModel {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.debug_struct("RecordingVisionModel").finish()
    }
}

#[async_trait]
impl ChatModel for RecordingVisionModel {
    fn capabilities(&self) -> BTreeSet<ModelCapability> {
        BTreeSet::from([ModelCapability::Vision])
    }

    fn validate_parameters(
        &self,
        _parameters: &Value,
    ) -> Result<(), insight_agent_platform::dsl::CompileError> {
        Ok(())
    }

    async fn stream_chat(&self, request: ChatRequest) -> Result<ChatStream, RunError> {
        self.requests.lock().unwrap().push(request);
        Ok(Box::pin(stream::iter(vec![Ok(ChatChunk {
            text: "直接回答追问".to_string(),
            finish_reason: Some("stop".to_string()),
            usage: None,
        })])))
    }
}

#[derive(Debug, Default)]
struct NoopRepository;

#[async_trait]
impl RunRepository for NoopRepository {
    async fn create_run(&self, _run: NewRun) -> Result<(), HistoryError> { Ok(()) }
    async fn mark_running(&self, _run_id: &str, _at: DateTime<Utc>) -> Result<(), HistoryError> { Ok(()) }
    async fn append_events(&self, _events: &[RunEvent]) -> Result<(), HistoryError> { Ok(()) }
    async fn put_node_output(&self, _output: NodeOutputRecord) -> Result<(), HistoryError> { Ok(()) }
    async fn finish_run(&self, _update: TerminalUpdate, _event: RunEvent) -> Result<bool, HistoryError> { Ok(true) }
    async fn recover_run(&self, _update: TerminalUpdate, event: RunEvent) -> Result<RunEvent, HistoryError> { Ok(event) }
    async fn get_run(&self, _run_id: &str) -> Result<Option<RunRecord>, HistoryError> { Ok(None) }
    async fn list_events_after(&self, _run_id: &str, _after_seq: u64, _limit: usize) -> Result<Vec<RunEvent>, HistoryError> { Ok(Vec::new()) }
    async fn mark_incomplete_interrupted(&self, _at: DateTime<Utc>) -> Result<u64, HistoryError> { Ok(0) }
}

fn compile_agent() -> (Arc<insight_agent_platform::dsl::compiled::CompiledAgent>, Arc<Mutex<Vec<ChatRequest>>>) {
    let requests = Arc::new(Mutex::new(Vec::new()));
    let model = RecordingVisionModel { requests: Arc::clone(&requests) };
    let mut models = ModelRegistry::default();
    models.register("vision_chat", model).unwrap();
    let (node_types, _) = default_node_registries().unwrap();
    let agent = AgentCompiler::new(
        node_types,
        models,
        ActionRegistry::default(),
        Duration::from_secs(1),
        CompileLimits { max_fork_branches: 32 },
    )
    .compile_dir(Path::new("agents/medical_report_interpreter"))
    .unwrap();
    let registry = CompiledAgentRegistry::new(vec![Arc::new(agent)]).unwrap();
    (registry.get("medical_report_interpreter").unwrap(), requests)
}

async fn run_agent(
    agent: Arc<insight_agent_platform::dsl::compiled::CompiledAgent>,
    run_id: &str,
    input: Value,
) -> RunOutput {
    let (_, executors) = default_node_registries().unwrap();
    let repository: Arc<dyn RunRepository> = Arc::new(NoopRepository);
    let events = EventHub::new(
        repository,
        EventHubConfig {
            subscriber_capacity: 8,
            journal_capacity: 32,
            journal_batch_size: 8,
            operation_timeout: Duration::from_secs(1),
        },
    );
    let scheduler = Scheduler::new(
        Arc::clone(&agent),
        executors,
        events,
        ExecutionLimiter::new(Arc::new(Semaphore::new(4)), Arc::new(Semaphore::new(4))),
    );
    let context = RunContext::new(
        RunMetadata {
            run_id: run_id.to_string(),
            request_id: format!("req_{run_id}"),
            agent_id: agent.id.clone(),
            agent_version: agent.version_hash.clone(),
            started_at: Utc::now(),
        },
        input,
    )
    .with_templates(Arc::clone(&agent.templates));
    let (_, stop) = stop_pair();
    match scheduler.run(context, stop).await.unwrap() {
        SchedulerResult::Completed(output) => output,
        result => panic!("expected completed run, got {result:?}"),
    }
}

#[tokio::test]
async fn empty_history_keeps_the_existing_three_step_flow() {
    let (agent, requests) = compile_agent();
    assert_eq!(agent.entry, "route");
    assert_eq!(agent.nodes["route"].kind, "core.condition");
    assert_eq!(agent.nodes["follow_up"].kind, "core.chat");
    assert_eq!(agent.nodes["follow_up_result"].kind, "core.output");

    let output = run_agent(
        agent,
        "run_initial",
        json!({
            "report_text": "血红蛋白偏低",
            "image_url": "https://example.test/report.png",
            "messages": [],
            "question": "请解读报告"
        }),
    )
    .await;

    assert_eq!(requests.lock().unwrap().len(), 3);
    assert!(output.data.get("abnormal_indicators").is_some());
    assert!(output.data.get("comprehensive_interpretation").is_some());
    assert!(output.data.get("health_advice").is_some());
}

#[tokio::test]
async fn non_empty_history_uses_one_dedicated_follow_up_request() {
    let (agent, requests) = compile_agent();
    let output = run_agent(
        agent,
        "run_follow_up",
        json!({
            "report_text": "血红蛋白 98 g/L，参考范围 115-150 g/L",
            "image_url": "https://example.test/report.png",
            "messages": [
                {"role": "user", "content": "请解读报告"},
                {"role": "assistant", "content": "血红蛋白偏低。"}
            ],
            "question": "这和缺铁有关吗？"
        }),
    )
    .await;

    let requests = requests.lock().unwrap();
    assert_eq!(requests.len(), 1);
    let user_message = &requests[0].messages[1];
    let prompt = user_message.text().unwrap();
    assert!(prompt.contains("血红蛋白 98 g/L"));
    assert!(prompt.contains("assistant: 血红蛋白偏低。"));
    assert!(prompt.contains("这和缺铁有关吗？"));
    assert!(prompt.contains("不要输出任何标题"));
    assert!(prompt.contains("不要重新执行首轮的三个步骤"));
    assert_eq!(user_message.image_urls(), vec!["https://example.test/report.png"]);
    assert_eq!(output.content.as_deref(), Some("直接回答追问"));
    assert_eq!(output.format.as_deref(), Some("markdown"));
    assert!(output.data.is_null());
}
```

- [ ] **Step 2: Run the new test and verify the behavioral failure**

Run:

```bash
cargo test --test medical_report_follow_up -- --nocapture
```

Expected: the test binary compiles, then `empty_history_keeps_the_existing_three_step_flow` fails because the current entry is `abnormal_indicators`, not `route`. This confirms the test detects the missing branch before production configuration changes.

- [ ] **Step 3: Add the dedicated follow-up prompt**

Create `agents/medical_report_interpreter/prompts/follow_up.md`:

```markdown
这是多轮对话中的报告追问。请只回答用户当前的问题，不要重新执行首轮的三个步骤。

先判断报告文本和历史对话是否与医学检验、检查、体检或病理报告有关。如果不是，只输出：
“我只能解读医学报告单。当前内容不像医学检验、检查、体检或病理报告，因此不能进行医学解读。”

报告文本：
{{ input.report_text }}

图片：已随本条消息一起提供。需要时可结合图片中可见的报告信息回答。

历史对话：
{{#each input.messages}}
- {{ role }}: {{ content }}
{{/each}}

当前问题：
{{ input.question }}

输出要求：
- 结合报告和历史对话，直接回答当前问题。
- 不要重新生成完整报告解读，不要重复用户未追问的内容。
- 不要输出任何标题，包括“异常指标解读”“综合解读”和“健康建议”。
- 不要采用首轮的三步格式。
- 信息不足、图片不清、缺少单位或参考范围时，明确说明不确定性和需要补充的信息。
- 不做确定诊断，不提供处方药调整方案，不替代医生面诊。
- 出现急症信号时，优先提示及时就医或急诊评估。
```

- [ ] **Step 4: Add condition routing and the one-node follow-up branch**

Modify `agents/medical_report_interpreter/agent.yaml` so its prompt map, entry, and new nodes are exactly:

```yaml
prompts:
  system: prompts/system.md
  abnormal_indicators: prompts/abnormal_indicators.md
  comprehensive_interpretation: prompts/comprehensive_interpretation.md
  health_advice: prompts/health_advice.md
  follow_up: prompts/follow_up.md

entry: route

nodes:
  route:
    type: core.condition
    config:
      cases:
        - when: "size(input.messages) > 0"
          next: follow_up
      default: abnormal_indicators

  follow_up:
    type: core.chat
    next: follow_up_result
    emit: content
    config:
      model: vision_chat
      messages:
        - role: system
          content:
            template_ref: system
        - role: user
          content:
            - type: text
              text:
                template_ref: follow_up
            - type: image_url
              image_url:
                url: "{{ input.image_url }}"
      parameters:
        temperature: 0.2

  follow_up_result:
    type: core.output
    config:
      content:
        template: "{{ nodes.follow_up.output.text }}"
      format: markdown
```

Keep the existing `abnormal_indicators`, `comprehensive_interpretation`, `health_advice`, and `result` node definitions byte-for-byte unchanged below these nodes.

- [ ] **Step 5: Run focused tests and verify both paths pass**

Run:

```bash
cargo test --test medical_report_follow_up --test repository_agents_v1 -- --nocapture
```

Expected: both test binaries pass. The initial case records three requests and retains all three structured fields; the follow-up case records one request and returns `data: null`.

- [ ] **Step 6: Run formatting and the full test suite**

Run:

```bash
cargo fmt --check
cargo test --all-targets
```

Expected: formatting reports no differences and every target passes without warnings introduced by these files.

- [ ] **Step 7: Inspect the final diff and commit**

Run:

```bash
git diff --check
git diff -- agents/medical_report_interpreter/agent.yaml agents/medical_report_interpreter/prompts/follow_up.md tests/medical_report_follow_up.rs
git status --short
```

Expected: only the three planned implementation files are present in the uncommitted diff, with no whitespace errors or unrelated changes.

Commit:

```bash
git add agents/medical_report_interpreter/agent.yaml \
  agents/medical_report_interpreter/prompts/follow_up.md \
  tests/medical_report_follow_up.rs
git commit -m "feat: route medical report follow-up turns"
```
