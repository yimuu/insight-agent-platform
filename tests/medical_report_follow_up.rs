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
        sqlite::SqliteRunRepository,
        types::{NewRun, NodeOutputRecord, RunRecord, TerminalUpdate},
    },
    nodes::default_node_registries,
    resources::{
        actions::ActionRegistry,
        models::{ChatChunk, ChatModel, ChatRequest, ChatStream, ModelCapability, ModelRegistry},
    },
    runtime::{
        stop_pair, CompiledAgentRegistry, ExecutionLimiter, RequestMetadata, RunContext, RunError,
        RunMetadata, RunService, RunServiceConfig, Scheduler, SchedulerResult,
    },
};
use serde_json::{json, Value};
use tokio::sync::Semaphore;

const ABNORMAL_RESPONSE: &str = "异常指标响应";
const COMPREHENSIVE_RESPONSE: &str = "综合解读响应";
const ADVICE_RESPONSE: &str = "健康建议响应";
const FOLLOW_UP_RESPONSE: &str = "直接回答追问";

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
        let prompt = request.messages[1].text().unwrap();
        let response = if prompt.starts_with("请执行第 1 步：异常指标解读。") {
            ABNORMAL_RESPONSE
        } else if prompt.starts_with("请执行第 2 步：综合解读。") {
            COMPREHENSIVE_RESPONSE
        } else if prompt.starts_with("请执行第 3 步：健康建议，并输出最终回复。")
        {
            ADVICE_RESPONSE
        } else {
            FOLLOW_UP_RESPONSE
        };
        self.requests.lock().unwrap().push(request);
        Ok(Box::pin(stream::iter(vec![Ok(ChatChunk {
            text: response.to_string(),
            finish_reason: Some("stop".to_string()),
            usage: None,
        })])))
    }
}

#[derive(Debug, Default)]
struct NoopRepository;

#[async_trait]
impl RunRepository for NoopRepository {
    async fn create_run(&self, _run: NewRun) -> Result<(), HistoryError> {
        Ok(())
    }
    async fn mark_running(&self, _run_id: &str, _at: DateTime<Utc>) -> Result<(), HistoryError> {
        Ok(())
    }
    async fn append_events(&self, _events: &[RunEvent]) -> Result<(), HistoryError> {
        Ok(())
    }
    async fn put_node_output(&self, _output: NodeOutputRecord) -> Result<(), HistoryError> {
        Ok(())
    }
    async fn finish_run(
        &self,
        _update: TerminalUpdate,
        _event: RunEvent,
    ) -> Result<bool, HistoryError> {
        Ok(true)
    }
    async fn recover_run(
        &self,
        _update: TerminalUpdate,
        event: RunEvent,
    ) -> Result<RunEvent, HistoryError> {
        Ok(event)
    }
    async fn get_run(&self, _run_id: &str) -> Result<Option<RunRecord>, HistoryError> {
        Ok(None)
    }
    async fn list_events_after(
        &self,
        _run_id: &str,
        _after_seq: u64,
        _limit: usize,
    ) -> Result<Vec<RunEvent>, HistoryError> {
        Ok(Vec::new())
    }
    async fn mark_incomplete_interrupted(&self, _at: DateTime<Utc>) -> Result<u64, HistoryError> {
        Ok(0)
    }
}

fn compile_agent() -> (
    Arc<insight_agent_platform::dsl::compiled::CompiledAgent>,
    Arc<Mutex<Vec<ChatRequest>>>,
) {
    let requests = Arc::new(Mutex::new(Vec::new()));
    let model = RecordingVisionModel {
        requests: Arc::clone(&requests),
    };
    let mut models = ModelRegistry::default();
    models.register("vision_chat", model).unwrap();
    let (node_types, _) = default_node_registries().unwrap();
    let agent = AgentCompiler::new(
        node_types,
        models,
        ActionRegistry::default(),
        Duration::from_secs(1),
        CompileLimits {
            max_fork_branches: 32,
        },
    )
    .compile_dir(Path::new("agents/medical_report_interpreter"))
    .unwrap();
    let registry = CompiledAgentRegistry::new(vec![Arc::new(agent)]).unwrap();
    (
        registry.get("medical_report_interpreter").unwrap(),
        requests,
    )
}

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

async fn run_service(
    agent: Arc<insight_agent_platform::dsl::compiled::CompiledAgent>,
) -> RunService {
    let (_, executors) = default_node_registries().unwrap();
    let repository = Arc::new(SqliteRunRepository::in_memory().await.unwrap());
    let repository: Arc<dyn RunRepository> = repository;
    let events = EventHub::new(
        Arc::clone(&repository),
        EventHubConfig {
            subscriber_capacity: 8,
            journal_capacity: 32,
            journal_batch_size: 8,
            operation_timeout: Duration::from_secs(1),
        },
    );
    RunService::new(
        CompiledAgentRegistry::new(vec![agent]).unwrap(),
        executors,
        repository,
        events,
        RunServiceConfig {
            max_concurrent_runs: 4,
            max_parallel_node_executions: 4,
            max_parallel_branches_per_run: 4,
            run_timeout: Duration::from_secs(5),
        },
    )
    .unwrap()
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
        assert!(agent
            .input_schema
            .is_valid(&medical_input(image_url, json!([]))));
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

#[tokio::test]
async fn medical_service_rejects_invalid_image_values_before_provider_invocation() {
    let (agent, requests) = compile_agent();
    let service = run_service(agent).await;

    for image_url in [
        json!(null),
        json!(7),
        json!("ftp://example.test/report.png"),
        json!("file:///tmp/report.png"),
    ] {
        let error = service
            .create_detached(
                "medical_report_interpreter",
                medical_input(Some(image_url), json!([])),
                RequestMetadata::default(),
            )
            .await
            .unwrap_err();

        assert_eq!(error.code(), "INPUT_INVALID");
        assert!(requests.lock().unwrap().is_empty());
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
            "image_url": "http://example.test/report.png",
            "messages": [],
            "question": "请解读报告"
        }),
    )
    .await;

    let requests = requests.lock().unwrap();
    assert_eq!(requests.len(), 3);
    for (request, prompt_start) in requests.iter().zip([
        "请执行第 1 步：异常指标解读。",
        "请执行第 2 步：综合解读。",
        "请执行第 3 步：健康建议，并输出最终回复。",
    ]) {
        let user_message = &request.messages[1];
        assert!(user_message.text().unwrap().starts_with(prompt_start));
        assert_eq!(
            user_message.image_urls(),
            vec!["http://example.test/report.png"]
        );
    }
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
    assert!(prompt.contains("直接回答当前问题"));
    assert!(prompt.contains("不要输出任何标题"));
    assert!(prompt.contains("不要重新执行首轮的三个步骤"));
    assert_eq!(
        user_message.image_urls(),
        vec!["https://example.test/report.png"]
    );
    assert_eq!(output.content.as_deref(), Some(FOLLOW_UP_RESPONSE));
    assert_eq!(output.format.as_deref(), Some("markdown"));
    assert!(output.data.is_null());
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
            medical_input(image_url, json!([{"role":"user", "content":"请解读报告"}])),
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
