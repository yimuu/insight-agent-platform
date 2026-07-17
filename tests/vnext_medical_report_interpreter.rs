use std::{
    collections::BTreeSet,
    sync::{Arc, Mutex},
    time::Duration,
};

use async_trait::async_trait;
use futures::stream;
use insight_agent_platform::{
    catalog::AgentCatalog,
    dsl::{vnext::compiler::WorkflowCompiler, CompileError},
    events::hub::{EventHub, EventHubConfig},
    history::{
        repository::RunRepository,
        sqlite::SqliteRunRepository,
        types::{RunLifecycle, RunRecord},
    },
    resources::{
        actions::ActionRegistry,
        models::{
            ChatChunk, ChatContent, ChatContentPart, ChatModel, ChatRequest, ChatRole, ChatStream,
            ModelCapability, ModelRegistry,
        },
    },
    runtime::{RequestMetadata, RunError, RunService, RunServiceConfig},
};
use serde_json::{json, Value};

const ABNORMAL_RESULT: &str = "ABNORMAL_RESULT_SENTINEL";
const COMPREHENSIVE_RESULT: &str = "COMPREHENSIVE_RESULT_SENTINEL";
const HEALTH_RESULT: &str = "HEALTH_RESULT_SENTINEL";
const FOLLOW_UP_RESULT: &str = "FOLLOW_UP_RESULT_SENTINEL";

#[derive(Debug, Clone, PartialEq, Eq)]
struct CapturedRequest {
    text: String,
    image_urls: Vec<String>,
    messages: Vec<(ChatRole, String)>,
}

#[derive(Debug, Default)]
struct MedicalTracker {
    requests: Mutex<Vec<CapturedRequest>>,
}

impl MedicalTracker {
    fn record(&self, request: &ChatRequest) -> CapturedRequest {
        let mut text = Vec::new();
        let mut image_urls = Vec::new();
        let mut messages = Vec::new();
        for message in &request.messages {
            let mut message_text = Vec::new();
            match &message.content {
                ChatContent::Text(value) => message_text.push(value.clone()),
                ChatContent::Parts(parts) => {
                    for part in parts {
                        match part {
                            ChatContentPart::Text { text: value } => {
                                message_text.push(value.clone());
                            }
                            ChatContentPart::Image { image } => {
                                image_urls.push(image.clone());
                            }
                        }
                    }
                }
            }
            text.extend(message_text.iter().cloned());
            messages.push((message.role, message_text.join("\n")));
        }
        let captured = CapturedRequest {
            text: text.join("\n"),
            image_urls,
            messages,
        };
        self.requests.lock().unwrap().push(captured.clone());
        captured
    }

    fn requests(&self) -> Vec<CapturedRequest> {
        self.requests.lock().unwrap().clone()
    }
}

#[derive(Debug)]
struct MedicalModel {
    tracker: Arc<MedicalTracker>,
}

#[async_trait]
impl ChatModel for MedicalModel {
    fn capabilities(&self) -> BTreeSet<ModelCapability> {
        BTreeSet::from([ModelCapability::Vision])
    }

    fn validate_parameters(&self, _parameters: &Value) -> Result<(), CompileError> {
        Ok(())
    }

    async fn stream_chat(&self, request: ChatRequest) -> Result<ChatStream, RunError> {
        let request = self.tracker.record(&request);
        let response = if request.text.contains("这是多轮对话中的报告追问") {
            FOLLOW_UP_RESULT
        } else if request.text.contains("请执行第 3 步：健康建议") {
            HEALTH_RESULT
        } else if request.text.contains("请执行第 2 步：综合解读") {
            COMPREHENSIVE_RESULT
        } else if request.text.contains("请执行第 1 步：异常指标解读") {
            ABNORMAL_RESULT
        } else {
            return Err(RunError::operation(
                "TEST_MEDICAL_REQUEST_UNCLASSIFIED",
                "medical fake model could not classify the authored prompt",
            ));
        };
        Ok(Box::pin(stream::iter([Ok(ChatChunk {
            text: response.to_string(),
            finish_reason: Some("stop".to_string()),
            usage: Some(json!({"total_tokens": 1})),
        })])))
    }
}

async fn run_medical(input: Value) -> (RunRecord, Arc<MedicalTracker>) {
    let tracker = Arc::new(MedicalTracker::default());
    let mut models = ModelRegistry::default();
    models
        .register(
            "vision_chat",
            MedicalModel {
                tracker: Arc::clone(&tracker),
            },
        )
        .unwrap();
    let compiler = WorkflowCompiler::new(models, ActionRegistry::default());
    let agent_dir =
        std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("agents/medical_report_interpreter");
    let workflow = Arc::new(compiler.compile_dir(&agent_dir).unwrap());
    let agents = AgentCatalog::new(vec![workflow]).unwrap();

    let repository = Arc::new(SqliteRunRepository::in_memory().await.unwrap());
    let repository_trait: Arc<dyn RunRepository> = repository;
    let events = EventHub::new(
        Arc::clone(&repository_trait),
        EventHubConfig {
            subscriber_capacity: 32,
            journal_capacity: 64,
            journal_batch_size: 8,
            operation_timeout: Duration::from_secs(1),
        },
    );
    let service = RunService::new(
        agents,
        repository_trait,
        events,
        RunServiceConfig {
            max_concurrent_runs: 1,
            max_concurrent_operations: 4,
            max_concurrent_operations_per_run: 4,
            operation_timeout: Duration::from_secs(2),
            operation_cancel_grace_period: Duration::from_millis(100),
            max_template_output_bytes: 64 * 1024,
            run_timeout: Duration::from_secs(3),
        },
    )
    .unwrap();
    let created = service
        .create_detached(
            "medical_report_interpreter",
            input,
            RequestMetadata::default(),
        )
        .await
        .unwrap();
    let record = tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            let record = service.get_run(&created.run_id).await.unwrap();
            if record.status().is_terminal() {
                return record;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .expect("medical workflow did not reach a durable terminal");
    service.shutdown(Duration::from_secs(1)).await.unwrap();
    (record, tracker)
}

fn initial_input(image_url: Value) -> Value {
    json!({
        "report_text": "ALT 80 U/L, reference range 7-40 U/L",
        "image_url": image_url,
        "messages": [],
        "question": "请解读这份报告"
    })
}

fn completed_output(record: &RunRecord) -> &insight_agent_platform::outcome::RunOutput {
    let RunLifecycle::Completed { output } = &record.lifecycle else {
        panic!("expected completed medical workflow")
    };
    output
}

#[tokio::test]
async fn initial_route_runs_three_steps_and_transfers_each_prior_result() {
    for (image_url, expected_images) in [
        (Value::Null, Vec::<String>::new()),
        (
            json!("https://example.test/report.png"),
            vec!["https://example.test/report.png".to_string()],
        ),
    ] {
        let (record, tracker) = run_medical(initial_input(image_url)).await;
        let output = completed_output(&record);
        let combined = format!("{ABNORMAL_RESULT}\n\n{COMPREHENSIVE_RESULT}\n\n{HEALTH_RESULT}");
        assert_eq!(output.content.as_deref(), Some(combined.as_str()));
        assert_eq!(
            output.data,
            json!({
                "mode": "initial",
                "answer": combined,
                "abnormal_indicators": ABNORMAL_RESULT,
                "comprehensive_interpretation": COMPREHENSIVE_RESULT,
                "health_advice": HEALTH_RESULT,
            })
        );

        let requests = tracker.requests();
        assert_eq!(requests.len(), 3, "initial Switch arm must run three chats");
        assert!(requests[0].text.contains("ALT 80 U/L"));
        assert!(requests[1].text.contains(ABNORMAL_RESULT));
        assert!(requests[2].text.contains(ABNORMAL_RESULT));
        assert!(requests[2].text.contains(COMPREHENSIVE_RESULT));
        for request in requests {
            assert_eq!(request.image_urls, expected_images);
            assert_eq!(
                request
                    .messages
                    .iter()
                    .map(|(role, _)| *role)
                    .collect::<Vec<_>>(),
                vec![ChatRole::System, ChatRole::User]
            );
        }
    }
}

#[tokio::test]
async fn follow_up_route_runs_only_the_follow_up_chat() {
    let (record, tracker) = run_medical(json!({
        "report_text": "ALT 80 U/L, reference range 7-40 U/L",
        "image_url": null,
        "messages": [
            {"role": "user", "content": "请先解读肝功能"},
            {"role": "assistant", "content": "此前已经说明 ALT 升高"}
        ],
        "question": "需要多久复查？"
    }))
    .await;
    let output = completed_output(&record);
    assert_eq!(output.content.as_deref(), Some(FOLLOW_UP_RESULT));
    assert_eq!(
        output.data,
        json!({"mode": "follow_up", "answer": FOLLOW_UP_RESULT})
    );

    let requests = tracker.requests();
    assert_eq!(
        requests.len(),
        1,
        "follow-up Switch arm must skip initial steps"
    );
    assert!(requests[0].text.contains("需要多久复查"));
    assert!(requests[0].text.contains("此前已经说明 ALT 升高"));
    assert_eq!(
        requests[0]
            .messages
            .iter()
            .map(|(role, _)| *role)
            .collect::<Vec<_>>(),
        vec![
            ChatRole::System,
            ChatRole::User,
            ChatRole::Assistant,
            ChatRole::User,
        ]
    );
    assert_eq!(requests[0].messages[1].1, "请先解读肝功能");
    assert_eq!(requests[0].messages[2].1, "此前已经说明 ALT 升高");
    assert!(requests[0].messages[3].1.contains("需要多久复查"));
    assert!(requests[0].image_urls.is_empty());
    assert!(!requests[0].text.contains(ABNORMAL_RESULT));
    assert!(!requests[0].text.contains(COMPREHENSIVE_RESULT));
    assert!(!requests[0].text.contains(HEALTH_RESULT));
}
