use std::{
    collections::BTreeMap,
    env,
    io::{BufRead as _, BufReader},
    path::{Path, PathBuf},
    process::{Child, Command, Stdio},
    sync::Arc,
};

use async_trait::async_trait;
use insight_api::mcp::{
    build_mcp_server_router, McpHttpPrincipal, McpHttpService, StaticBearerMcpHttpAuthorizer,
};
use insight_mcp::{
    CacheScope, ClientCapabilities, ClientInfo, CompleteResult, Completion, CompletionArgument,
    CompletionReference, ContentBlock, CreateTaskResult, DiscoverResult, GetPromptResult,
    GetTaskResult, InputResponse, ListPromptsResult, ListResourceTemplatesResult,
    ListResourcesResult, ListToolsResult, McpCallOutcome, McpClient, McpServerBackend,
    McpServerDispatcher, McpServerError, McpServerRequestContext, McpTransport, MetaMap,
    NoopNotificationObserver, ReadResourceResult, ServerCapabilities, ServerInfo, StdioTransport,
    StdioTransportPolicy, StreamableHttpPolicy, StreamableHttpTransport, SubscriptionFilter, Task,
    TaskAcknowledgement, TaskStatus, Tool, ToolCallResult, UpdateTaskParams, MCP_PROTOCOL_VERSION,
    MCP_TASKS_EXTENSION_ID,
};
use serde_json::{json, Value};
use tokio_util::sync::CancellationToken;

const QUALIFY_ENV: &str = "INSIGHT_MCP_EXTERNAL_SDK_QUALIFY";
const NODE_ENV: &str = "INSIGHT_MCP_NODE";
const GO_FIXTURE_ENV: &str = "INSIGHT_MCP_GO_FIXTURE_BIN";
const TOKEN: &str = "qualification-secret";

fn qualification_enabled() -> bool {
    env::var_os(QUALIFY_ENV).as_deref() == Some(std::ffi::OsStr::new("1"))
}

fn workspace() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).to_path_buf()
}

fn node_fixture() -> (PathBuf, Vec<String>, PathBuf) {
    let node = PathBuf::from(env::var_os(NODE_ENV).expect("qualification runner must set Node"));
    let directory = workspace().join("tests/interop/typescript");
    (node, vec!["fixture.mjs".to_owned()], directory)
}

fn go_fixture() -> (PathBuf, Vec<String>, PathBuf) {
    (
        PathBuf::from(
            env::var_os(GO_FIXTURE_ENV).expect("qualification runner must set Go fixture binary"),
        ),
        Vec::new(),
        workspace().join("tests/interop/go"),
    )
}

struct ChildGuard(Child);

impl Drop for ChildGuard {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

fn start_http_fixture(
    executable: &Path,
    prefix_args: &[String],
    directory: &Path,
) -> (ChildGuard, String) {
    let mut command = Command::new(executable);
    command
        .args(prefix_args)
        .args(["server-http", "0"])
        .current_dir(directory)
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::inherit());
    let mut child = command.spawn().expect("external SDK HTTP fixture starts");
    let stdout = child.stdout.take().expect("fixture stdout");
    let mut line = String::new();
    BufReader::new(stdout)
        .read_line(&mut line)
        .expect("fixture readiness line");
    let ready: Value = serde_json::from_str(line.trim()).expect("fixture readiness JSON");
    let endpoint = ready["ready"]
        .as_str()
        .expect("fixture readiness endpoint")
        .to_owned();
    (ChildGuard(child), endpoint)
}

fn tasks_capabilities() -> ClientCapabilities {
    ClientCapabilities {
        extensions: Some(
            MetaMap::new(BTreeMap::from([(
                MCP_TASKS_EXTENSION_ID.to_owned(),
                json!({}),
            )]))
            .unwrap(),
        ),
        ..ClientCapabilities::default()
    }
}

fn client(transport: Arc<dyn McpTransport>) -> McpClient {
    McpClient::new(
        transport,
        ClientInfo {
            name: "platform-external-sdk-qualification".to_owned(),
            version: "1.0.0".to_owned(),
            title: None,
            description: None,
            website_url: None,
            icons: Vec::new(),
        },
        tasks_capabilities(),
    )
    .unwrap()
}

async fn qualify_platform_client(client: McpClient, expected_task_id: &str) {
    let cancellation = CancellationToken::new();
    let observer = NoopNotificationObserver;
    let discovered = client.discover(&cancellation, &observer).await.unwrap();
    assert_eq!(discovered.supported_versions, [MCP_PROTOCOL_VERSION]);
    assert!(discovered
        .capabilities
        .extensions
        .as_ref()
        .and_then(|extensions| extensions.get(MCP_TASKS_EXTENSION_ID))
        .is_some());
    let catalog = client.list_tools(&cancellation, &observer).await.unwrap();
    assert!(catalog.rejected.is_empty());
    let echo = catalog
        .tools
        .iter()
        .find(|tool| tool.name == "sdk_echo")
        .unwrap();
    let complete = client
        .call_tool(
            echo,
            json!({"value":"platform-client"}),
            None,
            None,
            &cancellation,
            &observer,
        )
        .await
        .unwrap();
    let McpCallOutcome::Complete(complete) = complete else {
        panic!("echo must complete synchronously");
    };
    assert_eq!(
        complete.structured_content,
        Some(json!({"value":"platform-client"}))
    );
    let task_tool = catalog
        .tools
        .iter()
        .find(|tool| tool.name == "sdk_task")
        .unwrap();
    let created = client
        .call_tool(task_tool, json!({}), None, None, &cancellation, &observer)
        .await
        .unwrap();
    let McpCallOutcome::Task(created) = created else {
        panic!("task tool must return a negotiated task");
    };
    assert_eq!(created.task.task_id, expected_task_id);
    let completed = client
        .get_task(expected_task_id, &cancellation, &observer)
        .await
        .unwrap();
    assert_eq!(completed.task.status, TaskStatus::Completed);
}

#[tokio::test]
async fn platform_client_interoperates_with_two_pinned_sdk_servers_over_both_transports_and_tasks()
{
    if !qualification_enabled() {
        return;
    }
    for (executable, prefix, directory, task_id) in [
        {
            let (executable, prefix, directory) = node_fixture();
            (executable, prefix, directory, "typescript-task-1")
        },
        {
            let (executable, prefix, directory) = go_fixture();
            (executable, prefix, directory, "go-task-1")
        },
    ] {
        let mut stdio_args = prefix.clone();
        stdio_args.push("server-stdio".to_owned());
        let stdio = Arc::new(
            StdioTransport::new(
                StdioTransportPolicy::new(
                    executable.clone(),
                    stdio_args,
                    directory.clone(),
                    BTreeMap::new(),
                )
                .unwrap(),
            )
            .unwrap(),
        );
        qualify_platform_client(client(stdio.clone()), task_id).await;
        stdio.shutdown().await;

        let (_server, endpoint) = start_http_fixture(&executable, &prefix, directory.as_path());
        let http = StreamableHttpTransport::new(
            StreamableHttpPolicy::for_loopback_test(&endpoint).unwrap(),
            None,
        )
        .unwrap();
        qualify_platform_client(client(Arc::new(http)), task_id).await;
    }
}

#[derive(Clone, Default)]
struct QualificationBackend;

fn task(status: TaskStatus) -> Task {
    Task {
        task_id: "platform-task-1".to_owned(),
        status,
        status_message: None,
        created_at: "2026-07-30T00:00:00Z".parse().unwrap(),
        last_updated_at: "2026-07-30T00:00:01Z".parse().unwrap(),
        ttl_ms: Some(60_000),
        poll_interval_ms: Some(25),
    }
}

fn exported_tool(name: &str) -> Tool {
    Tool {
        name: name.to_owned(),
        title: None,
        description: Some("External SDK qualification export.".to_owned()),
        input_schema: if name == "qualified_echo" {
            json!({
                "type":"object",
                "properties":{"value":{"type":"string"}},
                "required":["value"],
                "additionalProperties":false
            })
        } else {
            json!({"type":"object","additionalProperties":false})
        },
        output_schema: (name == "qualified_echo").then(|| {
            json!({
                "type":"object",
                "properties":{"value":{"type":"string"}},
                "required":["value"],
                "additionalProperties":false
            })
        }),
        annotations: None,
        metadata: None,
        icons: Vec::new(),
    }
}

#[async_trait]
impl McpServerBackend for QualificationBackend {
    async fn discover(
        &self,
        _context: &McpServerRequestContext,
    ) -> Result<DiscoverResult, McpServerError> {
        Ok(DiscoverResult {
            result_type: "complete".to_owned(),
            supported_versions: vec![MCP_PROTOCOL_VERSION.to_owned()],
            capabilities: ServerCapabilities {
                tools: Some(json!({})),
                extensions: Some(
                    MetaMap::new(BTreeMap::from([(
                        MCP_TASKS_EXTENSION_ID.to_owned(),
                        json!({}),
                    )]))
                    .unwrap(),
                ),
                ..ServerCapabilities::default()
            },
            server_info: ServerInfo {
                name: "insight-platform-qualification".to_owned(),
                version: "1.0.0".to_owned(),
                title: None,
                description: None,
                website_url: None,
                icons: Vec::new(),
            },
            ttl_ms: 1_000,
            cache_scope: CacheScope::Private,
            instructions: None,
            metadata: None,
        })
    }

    async fn list_tools(
        &self,
        _context: &McpServerRequestContext,
        _cursor: Option<String>,
    ) -> Result<ListToolsResult, McpServerError> {
        Ok(ListToolsResult {
            result_type: "complete".to_owned(),
            tools: vec![
                exported_tool("qualified_echo"),
                exported_tool("qualified_task"),
            ],
            next_cursor: None,
            ttl_ms: 1_000,
            cache_scope: CacheScope::Private,
            metadata: None,
        })
    }

    async fn call_tool(
        &self,
        _context: &McpServerRequestContext,
        name: &str,
        arguments: Value,
        _input_responses: Option<BTreeMap<String, InputResponse>>,
        _request_state: Option<String>,
    ) -> Result<Value, McpServerError> {
        match name {
            "qualified_echo" => serde_json::to_value(ToolCallResult {
                result_type: "complete".to_owned(),
                content: vec![ContentBlock::Text {
                    text: arguments["value"].as_str().unwrap_or_default().to_owned(),
                    annotations: None,
                    metadata: None,
                }],
                structured_content: Some(json!({"value":arguments["value"]})),
                is_error: Some(false),
                metadata: None,
            })
            .map_err(|_| McpServerError::internal()),
            "qualified_task" => serde_json::to_value(CreateTaskResult {
                result_type: "task".to_owned(),
                task: task(TaskStatus::Working),
                metadata: None,
            })
            .map_err(|_| McpServerError::internal()),
            _ => Err(McpServerError::method_not_found()),
        }
    }

    async fn list_resources(
        &self,
        _context: &McpServerRequestContext,
        _cursor: Option<String>,
    ) -> Result<ListResourcesResult, McpServerError> {
        Ok(ListResourcesResult {
            result_type: "complete".to_owned(),
            resources: Vec::new(),
            next_cursor: None,
            ttl_ms: 1_000,
            cache_scope: CacheScope::Private,
            metadata: None,
        })
    }

    async fn list_resource_templates(
        &self,
        _context: &McpServerRequestContext,
        _cursor: Option<String>,
    ) -> Result<ListResourceTemplatesResult, McpServerError> {
        Ok(ListResourceTemplatesResult {
            result_type: "complete".to_owned(),
            resource_templates: Vec::new(),
            next_cursor: None,
            ttl_ms: 1_000,
            cache_scope: CacheScope::Private,
            metadata: None,
        })
    }

    async fn read_resource(
        &self,
        _context: &McpServerRequestContext,
        _uri: &str,
        _input_responses: Option<BTreeMap<String, InputResponse>>,
        _request_state: Option<String>,
    ) -> Result<Value, McpServerError> {
        serde_json::to_value(ReadResourceResult {
            result_type: "complete".to_owned(),
            contents: Vec::new(),
            ttl_ms: 1_000,
            cache_scope: CacheScope::Private,
            metadata: None,
        })
        .map_err(|_| McpServerError::internal())
    }

    async fn list_prompts(
        &self,
        _context: &McpServerRequestContext,
        _cursor: Option<String>,
    ) -> Result<ListPromptsResult, McpServerError> {
        Ok(ListPromptsResult {
            result_type: "complete".to_owned(),
            prompts: Vec::new(),
            next_cursor: None,
            ttl_ms: 1_000,
            cache_scope: CacheScope::Private,
            metadata: None,
        })
    }

    async fn get_prompt(
        &self,
        _context: &McpServerRequestContext,
        _name: &str,
        _arguments: BTreeMap<String, String>,
        _input_responses: Option<BTreeMap<String, InputResponse>>,
        _request_state: Option<String>,
    ) -> Result<Value, McpServerError> {
        serde_json::to_value(GetPromptResult {
            result_type: "complete".to_owned(),
            description: None,
            messages: Vec::new(),
            metadata: None,
        })
        .map_err(|_| McpServerError::internal())
    }

    async fn complete(
        &self,
        _context: &McpServerRequestContext,
        _reference: CompletionReference,
        _argument: CompletionArgument,
        _arguments: BTreeMap<String, String>,
    ) -> Result<CompleteResult, McpServerError> {
        Ok(CompleteResult {
            result_type: "complete".to_owned(),
            completion: Completion {
                values: Vec::new(),
                total: Some(0),
                has_more: Some(false),
            },
            metadata: None,
        })
    }

    async fn listen_subscriptions(
        &self,
        _context: &McpServerRequestContext,
        _filter: &SubscriptionFilter,
    ) -> Result<(), McpServerError> {
        Ok(())
    }

    async fn get_task(
        &self,
        _context: &McpServerRequestContext,
        task_id: &str,
    ) -> Result<GetTaskResult, McpServerError> {
        if task_id != "platform-task-1" {
            return Err(McpServerError::invalid_params());
        }
        Ok(GetTaskResult {
            result_type: "complete".to_owned(),
            task: task(TaskStatus::Completed),
            input_requests: None,
            result: Some(json!({
                "content":[{"type":"text","text":"platform-task-complete"}],
                "isError":false
            })),
            error: None,
            metadata: None,
        })
    }

    async fn update_task(
        &self,
        _context: &McpServerRequestContext,
        _update: UpdateTaskParams,
    ) -> Result<TaskAcknowledgement, McpServerError> {
        Ok(TaskAcknowledgement::complete())
    }

    async fn cancel_task(
        &self,
        _context: &McpServerRequestContext,
        _task_id: &str,
    ) -> Result<TaskAcknowledgement, McpServerError> {
        Ok(TaskAcknowledgement::complete())
    }
}

#[tokio::test]
async fn two_pinned_sdk_clients_interoperate_with_platform_http_server_and_tasks() {
    if !qualification_enabled() {
        return;
    }
    let service =
        Arc::new(McpServerDispatcher::new(QualificationBackend)) as Arc<dyn McpHttpService>;
    let authorizer = Arc::new(
        StaticBearerMcpHttpAuthorizer::new(
            TOKEN,
            McpHttpPrincipal::new("qualification/client").unwrap(),
        )
        .unwrap(),
    );
    let app = build_mcp_server_router("/mcp", service, authorizer, None).unwrap();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let endpoint = format!("http://{}/mcp", listener.local_addr().unwrap());
    let shutdown = CancellationToken::new();
    let server_shutdown = shutdown.clone();
    let server = tokio::spawn(async move {
        axum::serve(listener, app)
            .with_graceful_shutdown(server_shutdown.cancelled_owned())
            .await
            .unwrap();
    });

    for (executable, mut args, directory) in [node_fixture(), go_fixture()] {
        args.push("client-http".to_owned());
        args.push(endpoint.clone());
        let output = tokio::process::Command::new(executable)
            .args(args)
            .current_dir(directory)
            .output()
            .await
            .unwrap();
        assert!(
            output.status.success(),
            "external client failed: {}",
            String::from_utf8_lossy(&output.stderr)
        );
        let report: Value = serde_json::from_slice(&output.stdout).unwrap();
        assert_eq!(report["qualified"], true);
        assert_eq!(report["tasks"], true);
    }

    shutdown.cancel();
    server.await.unwrap();
}
