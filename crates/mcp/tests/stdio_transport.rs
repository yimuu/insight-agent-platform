use std::{
    collections::BTreeMap,
    path::PathBuf,
    sync::{Arc, Mutex},
    time::Duration,
};

use insight_mcp::{
    ClientCapabilities, ClientError, ClientInfo, JsonRpcNotification, LegacyCompatibilityTransport,
    McpClient, McpNotificationObserver, McpTransport, NoopNotificationObserver, StdioTransport,
    StdioTransportPolicy, TransportError, TransportKind, MCP_LEGACY_PROTOCOL_VERSION,
};
use serde_json::{json, Value};
use tokio::sync::Notify;
use tokio_util::sync::CancellationToken;

#[derive(Default)]
struct Observer {
    methods: Arc<Mutex<Vec<String>>>,
    changed: Notify,
}

impl Observer {
    async fn wait_for_methods(&self, expected: usize) {
        tokio::time::timeout(Duration::from_secs(2), async {
            loop {
                let changed = self.changed.notified();
                if self.methods.lock().unwrap().len() >= expected {
                    return;
                }
                changed.await;
            }
        })
        .await
        .expect("stdio subscription notifications were not observed");
    }
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
        self.changed.notify_one();
        Ok(())
    }
}

fn fixture(mode: &str, request_timeout: Duration) -> StdioTransport {
    let executable = PathBuf::from(env!("CARGO_BIN_EXE_mcp-stdio-fixture"));
    let working_directory = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let policy = StdioTransportPolicy::new(
        executable,
        vec![mode.to_owned()],
        working_directory,
        BTreeMap::from([("FIXTURE_SECRET".to_owned(), "not-logged".to_owned())]),
    )
    .unwrap()
    .with_timeouts(
        Duration::from_secs(1),
        request_timeout,
        Duration::from_secs(1),
    )
    .unwrap()
    .with_limits(64 * 1024, 64 * 1024, 1024)
    .unwrap();
    StdioTransport::new(policy).unwrap()
}

fn metadata() -> Value {
    json!({
        "io.modelcontextprotocol/protocolVersion":"2026-07-28",
        "io.modelcontextprotocol/clientCapabilities":{},
        "io.modelcontextprotocol/clientInfo":{
            "name":"stdio-conformance",
            "version":"1.0.0"
        }
    })
}

#[tokio::test]
async fn real_stdio_process_correlates_response_and_contains_stderr() {
    let transport = fixture("stderr", Duration::from_secs(2));
    assert_eq!(transport.kind(), TransportKind::Stdio);
    let response = transport
        .exchange(
            &json!({
                "jsonrpc":"2.0",
                "id":7,
                "method":"server/discover",
                "params":{"_meta":metadata()}
            }),
            &BTreeMap::new(),
            &CancellationToken::new(),
            &Observer::default(),
        )
        .await
        .unwrap();
    assert_eq!(response["id"], 7);
    assert_eq!(response["result"]["echoMethod"], "server/discover");
    assert!(!format!("{transport:?}").contains("not-logged"));
}

#[tokio::test]
async fn stdio_supervisor_reuses_a_process_and_restarts_after_crash() {
    let transport = fixture("normal", Duration::from_secs(2));
    let request = |id| {
        json!({
            "jsonrpc":"2.0",
            "id":id,
            "method":"server/discover",
            "params":{"_meta":metadata()}
        })
    };
    let first = transport
        .exchange(
            &request(1),
            &BTreeMap::new(),
            &CancellationToken::new(),
            &Observer::default(),
        )
        .await
        .unwrap();
    let second = transport
        .exchange(
            &request(2),
            &BTreeMap::new(),
            &CancellationToken::new(),
            &Observer::default(),
        )
        .await
        .unwrap();
    assert_eq!(first["result"]["processId"], second["result"]["processId"]);
    assert_eq!(first["result"]["requestCount"], 1);
    assert_eq!(second["result"]["requestCount"], 2);
    transport.shutdown().await;

    let crashing = fixture("crash_after_one", Duration::from_secs(2));
    crashing
        .exchange(
            &request(3),
            &BTreeMap::new(),
            &CancellationToken::new(),
            &Observer::default(),
        )
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_millis(20)).await;
    assert!(crashing
        .exchange(
            &request(4),
            &BTreeMap::new(),
            &CancellationToken::new(),
            &Observer::default(),
        )
        .await
        .is_err());
    crashing
        .exchange(
            &request(5),
            &BTreeMap::new(),
            &CancellationToken::new(),
            &Observer::default(),
        )
        .await
        .unwrap();
    assert!(insight_mcp::prometheus_metrics().contains(
        "insight_mcp_transport_events_total{transport=\"stdio\",event=\"process_restart\"}"
    ));
    crashing.shutdown().await;
}

#[tokio::test]
async fn malformed_stdout_timeout_and_cancellation_fail_closed() {
    let malformed = fixture("malformed", Duration::from_secs(1));
    let request = json!({
        "jsonrpc":"2.0",
        "id":1,
        "method":"server/discover",
        "params":{"_meta":metadata()}
    });
    assert_eq!(
        malformed
            .exchange(
                &request,
                &BTreeMap::new(),
                &CancellationToken::new(),
                &Observer::default(),
            )
            .await,
        Err(TransportError::Stdout)
    );

    let hanging = fixture("hang", Duration::from_millis(30));
    assert_eq!(
        hanging
            .exchange(
                &request,
                &BTreeMap::new(),
                &CancellationToken::new(),
                &Observer::default(),
            )
            .await,
        Err(TransportError::Timeout)
    );

    let cancellation = CancellationToken::new();
    cancellation.cancel();
    assert_eq!(
        hanging
            .exchange(
                &request,
                &BTreeMap::new(),
                &cancellation,
                &Observer::default(),
            )
            .await,
        Err(TransportError::Cancelled)
    );
}

#[tokio::test]
async fn stdio_subscription_requires_ack_and_stops_on_cancel() {
    let transport = fixture("normal", Duration::from_secs(2));
    let cancellation = CancellationToken::new();
    let observer = Arc::new(Observer::default());
    let task = {
        let cancellation = cancellation.clone();
        let observer = Arc::clone(&observer);
        tokio::spawn(async move {
            transport
                .listen(
                    &json!({
                        "jsonrpc":"2.0",
                        "id":9,
                        "method":"subscriptions/listen",
                        "params":{
                            "_meta":metadata(),
                            "notifications":{"toolsListChanged":true}
                        }
                    }),
                    &cancellation,
                    observer.as_ref(),
                )
                .await
        })
    };
    observer.wait_for_methods(2).await;
    cancellation.cancel();
    task.await.unwrap().unwrap();
    assert_eq!(
        observer.methods.lock().unwrap().as_slice(),
        [
            "notifications/subscriptions/acknowledged",
            "notifications/tools/list_changed"
        ]
    );
}

#[tokio::test]
async fn real_stdio_legacy_probe_fallback_freezes_the_selected_era() {
    let modern_transport = Arc::new(fixture("legacy", Duration::from_secs(2)));
    let info = ClientInfo {
        name: "legacy-conformance".to_owned(),
        version: "1.0.0".to_owned(),
        title: None,
        description: None,
        website_url: None,
        icons: Vec::new(),
    };
    let modern = McpClient::new(
        modern_transport.clone(),
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
    McpTransport::shutdown(modern_transport.as_ref())
        .await
        .unwrap();

    let adapter = Arc::new(
        LegacyCompatibilityTransport::new(
            modern_transport,
            info.clone(),
            ClientCapabilities::default(),
        )
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
    assert_eq!(catalog.tools[0].name, "legacy_echo");
}
