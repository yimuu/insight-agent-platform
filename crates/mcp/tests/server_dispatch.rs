use std::collections::{BTreeMap, BTreeSet};

use async_trait::async_trait;
use insight_mcp::{
    CacheScope, CompleteResult, Completion, CompletionArgument, CompletionReference, ContentBlock,
    DiscoverResult, GetPromptResult, InputResponse, JsonRpcNotification, ListPromptsResult,
    ListResourceTemplatesResult, ListResourcesResult, ListToolsResult, McpRequestHeaders,
    McpServerBackend, McpServerDispatcher, McpServerError, McpServerReply, McpServerRequestContext,
    Prompt, ReadResourceResult, RequestId, Resource, ResourceContents, ResourceTemplate,
    ServerCapabilities, ServerInfo, SubscriptionFilter, Tool, ToolCallResult, MCP_PROTOCOL_VERSION,
};
use serde_json::{json, Value};

struct FixtureBackend;

#[async_trait]
impl McpServerBackend for FixtureBackend {
    async fn discover(
        &self,
        context: &McpServerRequestContext,
    ) -> Result<DiscoverResult, McpServerError> {
        Ok(DiscoverResult {
            result_type: "complete".to_owned(),
            supported_versions: vec![MCP_PROTOCOL_VERSION.to_owned()],
            capabilities: ServerCapabilities {
                tools: Some(json!({})),
                resources: Some(json!({})),
                prompts: Some(json!({})),
                completions: Some(json!({})),
                ..ServerCapabilities::default()
            },
            server_info: ServerInfo {
                name: "fixture".to_owned(),
                version: "1.0.0".to_owned(),
                title: None,
                description: context
                    .authorization_scopes
                    .as_ref()
                    .map(|scopes| scopes.iter().cloned().collect::<Vec<_>>().join(" ")),
                website_url: None,
                icons: Vec::new(),
            },
            ttl_ms: 1000,
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
            tools: vec![Tool {
                name: "echo".to_owned(),
                title: None,
                description: None,
                input_schema: json!({"type":"object"}),
                output_schema: None,
                annotations: None,
                metadata: None,
                icons: Vec::new(),
            }],
            next_cursor: None,
            ttl_ms: 1000,
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
        if name != "echo" {
            return Err(McpServerError::method_not_found());
        }
        serde_json::to_value(ToolCallResult {
            result_type: "complete".to_owned(),
            content: vec![ContentBlock::Text {
                text: arguments.to_string(),
                annotations: None,
                metadata: None,
            }],
            structured_content: Some(arguments),
            is_error: Some(false),
            metadata: None,
        })
        .map_err(|_| McpServerError::internal())
    }

    async fn list_resources(
        &self,
        _context: &McpServerRequestContext,
        _cursor: Option<String>,
    ) -> Result<ListResourcesResult, McpServerError> {
        Ok(ListResourcesResult {
            result_type: "complete".to_owned(),
            resources: vec![Resource {
                uri: "memory://one".to_owned(),
                name: "one".to_owned(),
                title: None,
                description: None,
                mime_type: Some("text/plain".to_owned()),
                size: Some(3),
                icons: Vec::new(),
                annotations: None,
                metadata: None,
            }],
            next_cursor: None,
            ttl_ms: 1000,
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
            resource_templates: vec![ResourceTemplate {
                uri_template: "memory://{id}".to_owned(),
                name: "memory".to_owned(),
                title: None,
                description: None,
                mime_type: None,
                icons: Vec::new(),
                annotations: None,
                metadata: None,
            }],
            next_cursor: None,
            ttl_ms: 1000,
            cache_scope: CacheScope::Private,
            metadata: None,
        })
    }

    async fn read_resource(
        &self,
        _context: &McpServerRequestContext,
        uri: &str,
        _input_responses: Option<BTreeMap<String, InputResponse>>,
        _request_state: Option<String>,
    ) -> Result<Value, McpServerError> {
        serde_json::to_value(ReadResourceResult {
            result_type: "complete".to_owned(),
            contents: vec![ResourceContents::Text {
                uri: uri.to_owned(),
                text: "one".to_owned(),
                mime_type: Some("text/plain".to_owned()),
                metadata: None,
            }],
            ttl_ms: 1000,
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
            prompts: vec![Prompt {
                name: "review".to_owned(),
                title: None,
                description: None,
                arguments: Vec::new(),
                icons: Vec::new(),
                metadata: None,
            }],
            next_cursor: None,
            ttl_ms: 1000,
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
                values: vec!["brief".to_owned()],
                total: Some(1),
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
}

fn metadata() -> Value {
    json!({
        "io.modelcontextprotocol/protocolVersion": MCP_PROTOCOL_VERSION,
        "io.modelcontextprotocol/clientCapabilities": {},
        "io.modelcontextprotocol/clientInfo": {"name":"test","version":"1"}
    })
}

fn request(id: i64, method: &str, mut params: Value) -> Vec<u8> {
    params
        .as_object_mut()
        .unwrap()
        .insert("_meta".to_owned(), metadata());
    serde_json::to_vec(&json!({
        "jsonrpc":"2.0",
        "id":id,
        "method":method,
        "params":params
    }))
    .unwrap()
}

fn headers(method: &str, name: Option<&str>) -> McpRequestHeaders {
    McpRequestHeaders {
        protocol_version: MCP_PROTOCOL_VERSION.to_owned(),
        method: method.to_owned(),
        name: name.map(str::to_owned),
    }
}

#[tokio::test]
async fn dispatcher_covers_every_modern_core_method() {
    let server = McpServerDispatcher::new(FixtureBackend);
    let cases = [
        ("server/discover", json!({}), None),
        ("tools/list", json!({}), None),
        (
            "tools/call",
            json!({"name":"echo","arguments":{"text":"hello"}}),
            Some("echo"),
        ),
        ("resources/list", json!({}), None),
        ("resources/templates/list", json!({}), None),
        (
            "resources/read",
            json!({"uri":"memory://one"}),
            Some("memory://one"),
        ),
        ("prompts/list", json!({}), None),
        (
            "prompts/get",
            json!({"name":"review","arguments":{}}),
            Some("review"),
        ),
        (
            "completion/complete",
            json!({
                "ref":{"type":"ref/prompt","name":"review"},
                "argument":{"name":"tone","value":"b"}
            }),
            None,
        ),
    ];
    for (index, (method, params, name)) in cases.into_iter().enumerate() {
        let McpServerReply::Json(response) = server
            .dispatch(
                &request(index as i64, method, params),
                &headers(method, name),
            )
            .await
        else {
            panic!("ordinary method returned subscription reply");
        };
        assert_eq!(response["jsonrpc"], "2.0");
        assert_eq!(response["id"], index as i64);
        assert!(response.get("result").is_some(), "{method}: {response}");
    }
}

#[tokio::test]
async fn dispatcher_enforces_header_body_and_closed_parameter_contracts() {
    let server = McpServerDispatcher::new(FixtureBackend);
    let body = request(
        1,
        "tools/call",
        json!({"name":"echo","arguments":{},"unknown":true}),
    );
    let McpServerReply::Json(response) = server
        .dispatch(&body, &headers("tools/call", Some("echo")))
        .await
    else {
        panic!();
    };
    assert_eq!(response["error"]["code"], -32602);

    let body = request(2, "tools/call", json!({"name":"echo","arguments":{}}));
    let McpServerReply::Json(response) = server
        .dispatch(&body, &headers("tools/list", Some("echo")))
        .await
    else {
        panic!();
    };
    assert_eq!(response["error"]["code"], -32600);
}

#[tokio::test]
async fn dispatcher_propagates_only_explicit_validated_authorization_scopes() {
    let server = McpServerDispatcher::new(FixtureBackend);
    let McpServerReply::Json(response) = server
        .dispatch_with_authorization(
            &request(3, "server/discover", json!({})),
            &headers("server/discover", None),
            Some("tenant/user".to_owned()),
            Some(BTreeSet::from([
                "mcp.read".to_owned(),
                "mcp.write".to_owned(),
            ])),
        )
        .await
    else {
        panic!();
    };
    assert_eq!(
        response["result"]["serverInfo"]["description"],
        "mcp.read mcp.write"
    );
}

#[tokio::test]
async fn subscription_reply_is_correlated_acknowledgement_notification() {
    let server = McpServerDispatcher::new(FixtureBackend);
    let reply = server
        .dispatch(
            &request(
                9,
                "subscriptions/listen",
                json!({"notifications":{"toolsListChanged":true}}),
            ),
            &headers("subscriptions/listen", None),
        )
        .await;
    let McpServerReply::SubscriptionAcknowledgement(JsonRpcNotification { method, params, .. }) =
        reply
    else {
        panic!();
    };
    assert_eq!(method, "notifications/subscriptions/acknowledged");
    assert_eq!(
        params.unwrap()["_meta"]["io.modelcontextprotocol/subscriptionId"],
        serde_json::to_value(RequestId::Integer(9)).unwrap()
    );
}
