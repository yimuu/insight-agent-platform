use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc, Mutex,
};

use axum::{
    body::Body,
    extract::State,
    http::{header, HeaderMap, Response, StatusCode},
    routing::post,
    Json, Router,
};
use futures::{stream, StreamExt};
use insight_agent_platform::mcp::{
    ClientCapabilities, ClientError, ClientInfo, JsonRpcNotification, LegacyCompatibilityTransport,
    McpCallOutcome, McpClient, McpNotificationObserver, NoopNotificationObserver,
    StreamableHttpPolicy, StreamableHttpTransport, SubscriptionFilter, ToolCallResult,
    TransportError, MCP_LEGACY_PROTOCOL_VERSION,
};
use serde_json::{json, Value};
use tokio_util::sync::CancellationToken;

type LegacyObservation = (String, Option<String>, Option<String>);

#[derive(Clone)]
struct ServerState {
    headers_valid: Arc<AtomicBool>,
}

#[derive(Clone, Default)]
struct LegacyHttpState {
    observations: Arc<Mutex<Vec<LegacyObservation>>>,
}

async fn legacy_http_handler(
    State(state): State<LegacyHttpState>,
    headers: HeaderMap,
    Json(body): Json<Value>,
) -> Response<Body> {
    let method = body["method"].as_str().unwrap_or_default().to_owned();
    state.observations.lock().unwrap().push((
        method.clone(),
        headers
            .get("mcp-session-id")
            .and_then(|value| value.to_str().ok())
            .map(str::to_owned),
        headers
            .get("mcp-protocol-version")
            .and_then(|value| value.to_str().ok())
            .map(str::to_owned),
    ));
    if body.get("id").is_none() {
        return Response::builder()
            .status(StatusCode::ACCEPTED)
            .body(Body::empty())
            .unwrap();
    }
    let id = body["id"].clone();
    let result = match method.as_str() {
        "server/discover" => {
            return Json(json!({
                "jsonrpc":"2.0",
                "id":id,
                "error":{"code":-32601,"message":"Method not found"}
            }))
            .into_response();
        }
        "initialize" => json!({
            "protocolVersion":MCP_LEGACY_PROTOCOL_VERSION,
            "capabilities":{"tools":{"listChanged":true}},
            "serverInfo":{"name":"legacy-http","version":"1.0.0"}
        }),
        "tools/list" => json!({
            "tools":[{
                "name":"legacy_http_echo",
                "inputSchema":{"type":"object","additionalProperties":false}
            }]
        }),
        _ => {
            return Json(json!({
                "jsonrpc":"2.0",
                "id":id,
                "error":{"code":-32601,"message":"Method not found"}
            }))
            .into_response();
        }
    };
    let mut response = Json(json!({"jsonrpc":"2.0","id":id,"result":result})).into_response();
    if method == "initialize" {
        response.headers_mut().insert(
            "mcp-session-id",
            axum::http::HeaderValue::from_static("legacy-session-1"),
        );
    }
    response
}

async fn mcp_handler(
    State(state): State<ServerState>,
    headers: HeaderMap,
    Json(body): Json<Value>,
) -> Response<Body> {
    let method = body["method"].as_str().unwrap_or_default();
    let protocol = body["params"]["_meta"]["io.modelcontextprotocol/protocolVersion"]
        .as_str()
        .unwrap_or_default();
    let accept_valid = headers
        .get(header::ACCEPT)
        .and_then(|value| value.to_str().ok())
        .is_some_and(|value| {
            value.contains("text/event-stream")
                && (method == "subscriptions/listen" || value.contains("application/json"))
        });
    let standard_valid = headers
        .get("mcp-protocol-version")
        .and_then(|value| value.to_str().ok())
        == Some(protocol)
        && headers
            .get("mcp-method")
            .and_then(|value| value.to_str().ok())
            == Some(method)
        && accept_valid
        && headers
            .get(header::AUTHORIZATION)
            .and_then(|value| value.to_str().ok())
            == Some("Bearer fixture-secret");
    let method_valid = match method {
        "tools/call" => {
            headers
                .get("mcp-name")
                .and_then(|value| value.to_str().ok())
                == Some("search")
                && headers
                    .get("mcp-param-route")
                    .and_then(|value| value.to_str().ok())
                    == Some("=?base64?SGVsbG8sIOS4lueVjA==?=")
        }
        _ => true,
    };
    state
        .headers_valid
        .fetch_and(standard_valid && method_valid, Ordering::SeqCst);

    let id = body["id"].clone();
    match method {
        "server/discover" => Json(json!({
            "jsonrpc":"2.0",
            "id":id,
            "result":{
                "resultType":"complete",
                "supportedVersions":["2026-07-28"],
                "capabilities":{"tools":{}},
                "serverInfo":{"name":"fixture","version":"1.0.0"},
                "ttlMs":1000,
                "cacheScope":"private"
            }
        }))
        .into_response(),
        "tools/list" => Json(json!({
            "jsonrpc":"2.0",
            "id":id,
            "result":{
                "resultType":"complete",
                "tools":[{
                    "name":"search",
                    "description":"Search the fixture.",
                    "inputSchema":{
                        "type":"object",
                        "properties":{
                            "route":{"type":"string","x-mcp-header":"Route"}
                        },
                        "required":["route"]
                    },
                    "outputSchema":{
                        "type":"object",
                        "properties":{"answer":{"type":"string"}},
                        "required":["answer"]
                    }
                }],
                "ttlMs":1000,
                "cacheScope":"private"
            }
        }))
        .into_response(),
        "tools/call" => {
            let progress_token = body["params"]["_meta"]["progressToken"].clone();
            let event = format!(
                "data: {}\n\n: keepalive\n\ndata: {}\n\n",
                json!({
                    "jsonrpc":"2.0",
                    "method":"notifications/progress",
                    "params":{"progressToken":progress_token,"progress":1}
                }),
                json!({
                    "jsonrpc":"2.0",
                    "id":id,
                    "result":{
                        "resultType":"complete",
                        "content":[{"type":"text","text":"done"}],
                        "structuredContent":{"answer":"done"},
                        "isError":false
                    }
                })
            );
            Response::builder()
                .status(StatusCode::OK)
                .header(header::CONTENT_TYPE, "text/event-stream")
                .body(Body::from(event))
                .unwrap()
        }
        "subscriptions/listen" => {
            let event = format!(
                "data: {}\n\ndata: {}\n\n",
                json!({
                    "jsonrpc":"2.0",
                    "method":"notifications/subscriptions/acknowledged",
                    "params":{
                        "notifications":{"toolsListChanged":true},
                        "_meta":{"io.modelcontextprotocol/subscriptionId":id.clone()}
                    }
                }),
                json!({
                    "jsonrpc":"2.0",
                    "method":"notifications/tools/list_changed",
                    "params":{
                        "_meta":{"io.modelcontextprotocol/subscriptionId":id}
                    }
                })
            );
            let body_stream = stream::once(async move {
                Ok::<_, std::convert::Infallible>(axum::body::Bytes::from(event))
            })
            .chain(stream::pending());
            Response::builder()
                .status(StatusCode::OK)
                .header(header::CONTENT_TYPE, "text/event-stream")
                .body(Body::from_stream(body_stream))
                .unwrap()
        }
        _ => Response::builder()
            .status(StatusCode::NOT_FOUND)
            .header(header::CONTENT_TYPE, "application/json")
            .body(Body::from(
                json!({
                    "jsonrpc":"2.0",
                    "id":id,
                    "error":{"code":-32601,"message":"Method not found"}
                })
                .to_string(),
            ))
            .unwrap(),
    }
}

trait IntoResponse {
    fn into_response(self) -> Response<Body>;
}

impl IntoResponse for Json<Value> {
    fn into_response(self) -> Response<Body> {
        Response::builder()
            .status(StatusCode::OK)
            .header(header::CONTENT_TYPE, "application/json")
            .body(Body::from(self.0.to_string()))
            .unwrap()
    }
}

#[derive(Default)]
struct Observer {
    methods: Mutex<Vec<String>>,
}

impl McpNotificationObserver for Observer {
    fn on_notification(
        &self,
        notification: &JsonRpcNotification<Value>,
    ) -> Result<(), TransportError> {
        self.methods
            .lock()
            .unwrap()
            .push(notification.method.clone());
        Ok(())
    }
}

#[tokio::test]
async fn modern_http_client_covers_discovery_headers_json_sse_progress_and_tools() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let state = ServerState {
        headers_valid: Arc::new(AtomicBool::new(true)),
    };
    let app = Router::new()
        .route("/mcp", post(mcp_handler))
        .with_state(state.clone());
    let shutdown = CancellationToken::new();
    let shutdown_server = shutdown.clone();
    let server = tokio::spawn(async move {
        axum::serve(listener, app)
            .with_graceful_shutdown(shutdown_server.cancelled_owned())
            .await
            .unwrap();
    });

    let endpoint = format!("http://{address}/mcp");
    let transport = StreamableHttpTransport::new(
        StreamableHttpPolicy::for_loopback_test(&endpoint).unwrap(),
        Some(insight_agent_platform::mcp::HttpCredential::bearer("fixture-secret").unwrap()),
    )
    .unwrap();
    let client = McpClient::new(
        Arc::new(transport),
        ClientInfo {
            name: "integration-test".to_owned(),
            version: "1.0.0".to_owned(),
            title: None,
            description: None,
            website_url: None,
            icons: Vec::new(),
        },
        ClientCapabilities::default(),
    )
    .unwrap();
    let cancellation = CancellationToken::new();
    let observer = Arc::new(Observer::default());

    client
        .discover(&cancellation, observer.as_ref())
        .await
        .unwrap();
    let catalog = client
        .list_tools(&cancellation, observer.as_ref())
        .await
        .unwrap();
    assert_eq!(catalog.tools.len(), 1);
    let outcome = client
        .call_tool(
            &catalog.tools[0],
            json!({"route":"Hello, 世界"}),
            None,
            None,
            &cancellation,
            observer.as_ref(),
        )
        .await
        .unwrap();
    assert!(matches!(
        outcome,
        McpCallOutcome::Complete(ToolCallResult {
            structured_content: Some(_),
            is_error: Some(false),
            ..
        })
    ));
    assert_eq!(
        observer.methods.lock().unwrap().as_slice(),
        ["notifications/progress"]
    );

    let subscription_cancellation = CancellationToken::new();
    let subscription_client = client.clone();
    let subscription_observer = Arc::clone(&observer);
    let subscription_stop = subscription_cancellation.clone();
    let subscription = tokio::spawn(async move {
        subscription_client
            .listen_subscriptions(
                SubscriptionFilter {
                    tools_list_changed: Some(true),
                    ..SubscriptionFilter::default()
                },
                &subscription_stop,
                subscription_observer.as_ref(),
            )
            .await
    });
    for _ in 0..100 {
        if observer.methods.lock().unwrap().len() >= 3 {
            break;
        }
        tokio::task::yield_now().await;
    }
    subscription_cancellation.cancel();
    subscription.await.unwrap().unwrap();
    assert_eq!(
        observer.methods.lock().unwrap().as_slice(),
        [
            "notifications/progress",
            "notifications/subscriptions/acknowledged",
            "notifications/tools/list_changed"
        ]
    );
    assert!(state.headers_valid.load(Ordering::SeqCst));

    shutdown.cancel();
    server.await.unwrap();
}

#[tokio::test]
async fn legacy_http_fallback_negotiates_and_reuses_an_isolated_session() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let state = LegacyHttpState::default();
    let app = Router::new()
        .route("/mcp", post(legacy_http_handler))
        .with_state(state.clone());
    let shutdown = CancellationToken::new();
    let shutdown_server = shutdown.clone();
    let server = tokio::spawn(async move {
        axum::serve(listener, app)
            .with_graceful_shutdown(shutdown_server.cancelled_owned())
            .await
            .unwrap();
    });

    let endpoint = format!("http://{address}/mcp");
    let transport = Arc::new(
        StreamableHttpTransport::new(
            StreamableHttpPolicy::for_loopback_test(&endpoint).unwrap(),
            None,
        )
        .unwrap(),
    );
    let info = ClientInfo {
        name: "legacy-http-conformance".to_owned(),
        version: "1.0.0".to_owned(),
        title: None,
        description: None,
        website_url: None,
        icons: Vec::new(),
    };
    let modern = McpClient::new(
        transport.clone(),
        info.clone(),
        ClientCapabilities::default(),
    )
    .unwrap();
    assert_eq!(
        modern
            .discover(&CancellationToken::new(), &NoopNotificationObserver)
            .await,
        Err(ClientError::Protocol(-32601))
    );
    let adapter = Arc::new(
        LegacyCompatibilityTransport::new(transport, info.clone(), ClientCapabilities::default())
            .unwrap(),
    );
    let legacy = McpClient::new_legacy(adapter, info, ClientCapabilities::default()).unwrap();
    let discovery = legacy
        .discover(&CancellationToken::new(), &NoopNotificationObserver)
        .await
        .unwrap();
    assert_eq!(
        discovery.supported_versions,
        vec![MCP_LEGACY_PROTOCOL_VERSION]
    );
    let catalog = legacy
        .list_tools(&CancellationToken::new(), &NoopNotificationObserver)
        .await
        .unwrap();
    assert_eq!(catalog.tools[0].name, "legacy_http_echo");

    let observations = state.observations.lock().unwrap().clone();
    assert_eq!(observations[1], ("initialize".to_owned(), None, None));
    assert_eq!(
        observations[2],
        (
            "notifications/initialized".to_owned(),
            Some("legacy-session-1".to_owned()),
            Some(MCP_LEGACY_PROTOCOL_VERSION.to_owned())
        )
    );
    assert_eq!(
        observations[3],
        (
            "tools/list".to_owned(),
            Some("legacy-session-1".to_owned()),
            Some(MCP_LEGACY_PROTOCOL_VERSION.to_owned())
        )
    );

    shutdown.cancel();
    server.await.unwrap();
}
