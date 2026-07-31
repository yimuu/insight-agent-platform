//! Isolated MCP `2025-11-25` compatibility profile.
//!
//! The adapter deliberately presents the modern internal client contract while
//! owning all legacy handshake/session mechanics. Modern transports never
//! decode a legacy request, Tasks are unavailable, and the selected era is
//! immutable for the lifetime of this adapter.

use std::{
    collections::BTreeMap,
    sync::{
        atomic::{AtomicI64, Ordering},
        Arc,
    },
};

use async_trait::async_trait;
use serde_json::{json, Map, Value};
use tokio::sync::Mutex;
use tokio_util::sync::CancellationToken;

use crate::{
    ClientCapabilities, ClientInfo, McpNotificationObserver, McpTransport, RequestId,
    SubscriptionFilter, TransportError, TransportKind, MCP_LEGACY_PROTOCOL_VERSION,
};

const LEGACY_CACHE_TTL_MS: u64 = 30_000;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LegacyError {
    Configuration,
}

impl std::fmt::Display for LegacyError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("invalid MCP legacy compatibility configuration")
    }
}

impl std::error::Error for LegacyError {}

#[derive(Clone)]
pub struct LegacyCompatibilityTransport {
    inner: Arc<dyn McpTransport>,
    client_info: ClientInfo,
    client_capabilities: ClientCapabilities,
    state: Arc<Mutex<Option<Value>>>,
    next_internal_id: Arc<AtomicI64>,
}

impl std::fmt::Debug for LegacyCompatibilityTransport {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("LegacyCompatibilityTransport")
            .field("kind", &self.inner.kind())
            .field("protocol_version", &MCP_LEGACY_PROTOCOL_VERSION)
            .finish_non_exhaustive()
    }
}

impl LegacyCompatibilityTransport {
    pub fn new(
        inner: Arc<dyn McpTransport>,
        client_info: ClientInfo,
        mut client_capabilities: ClientCapabilities,
    ) -> Result<Self, LegacyError> {
        if client_info.name.is_empty()
            || client_info.name.len() > 256
            || client_info.version.is_empty()
            || client_info.version.len() > 128
            || client_capabilities
                .additional
                .keys()
                .any(|key| matches!(key.as_str(), "roots" | "sampling"))
        {
            return Err(LegacyError::Configuration);
        }
        // Tasks did not become an accepted part of this compatibility
        // profile. Unknown extensions cannot silently enable behavior.
        client_capabilities.extensions = None;
        client_capabilities.additional.remove("roots");
        client_capabilities.additional.remove("sampling");
        Ok(Self {
            inner,
            client_info,
            client_capabilities,
            state: Arc::new(Mutex::new(None)),
            next_internal_id: Arc::new(AtomicI64::new(-1)),
        })
    }

    fn next_id(&self) -> Result<RequestId, TransportError> {
        let id = self.next_internal_id.fetch_sub(1, Ordering::Relaxed);
        if id >= 0 || id == i64::MIN {
            return Err(TransportError::Request);
        }
        Ok(RequestId::Integer(id))
    }

    fn initialize_request(&self) -> Result<Value, TransportError> {
        let capabilities = legacy_client_capabilities(&self.client_capabilities);
        Ok(json!({
            "jsonrpc": "2.0",
            "id": self.next_id()?,
            "method": "initialize",
            "params": {
                "protocolVersion": MCP_LEGACY_PROTOCOL_VERSION,
                "capabilities": capabilities,
                "clientInfo": self.client_info,
            }
        }))
    }

    fn initialized_notification() -> Value {
        json!({
            "jsonrpc": "2.0",
            "method": "notifications/initialized",
            "params": {}
        })
    }

    async fn ensure_initialized(
        &self,
        cancellation: &CancellationToken,
        observer: &dyn McpNotificationObserver,
    ) -> Result<Value, TransportError> {
        let mut state = self.state.lock().await;
        if let Some(discovery) = state.as_ref() {
            return Ok(discovery.clone());
        }
        let request = self.initialize_request()?;
        let response = self
            .inner
            .exchange_legacy(&request, cancellation, observer)
            .await?;
        let expected_id = request.get("id").cloned().ok_or(TransportError::Response)?;
        if response.get("jsonrpc").and_then(Value::as_str) != Some("2.0")
            || response.get("id") != Some(&expected_id)
        {
            return Err(TransportError::Correlation);
        }
        if response.get("error").is_some() {
            return Err(TransportError::Response);
        }
        let discovery = normalize_initialize_result(
            response
                .get("result")
                .cloned()
                .ok_or(TransportError::Response)?,
        )?;
        self.inner
            .notify_legacy(&Self::initialized_notification(), cancellation)
            .await?;
        *state = Some(discovery.clone());
        Ok(discovery)
    }
}

#[async_trait]
impl McpTransport for LegacyCompatibilityTransport {
    fn kind(&self) -> TransportKind {
        self.inner.kind()
    }

    async fn exchange(
        &self,
        request: &Value,
        parameter_headers: &BTreeMap<String, String>,
        cancellation: &CancellationToken,
        observer: &dyn McpNotificationObserver,
    ) -> Result<Value, TransportError> {
        if !parameter_headers.is_empty() {
            return Err(TransportError::Header);
        }
        let (request_id, method, params) = validate_facade_request(request)?;
        let discovery = self.ensure_initialized(cancellation, observer).await?;
        if method == "server/discover" {
            return Ok(json!({
                "jsonrpc": "2.0",
                "id": request_id,
                "result": discovery,
            }));
        }
        if method.starts_with("tasks/") || method == "subscriptions/listen" {
            return Err(TransportError::Unsupported);
        }
        let legacy_request = json!({
            "jsonrpc": "2.0",
            "id": request_id,
            "method": method,
            "params": strip_facade_metadata(params),
        });
        let response = self
            .inner
            .exchange_legacy(&legacy_request, cancellation, observer)
            .await?;
        normalize_legacy_response(response, &method)
    }

    async fn listen(
        &self,
        request: &Value,
        cancellation: &CancellationToken,
        observer: &dyn McpNotificationObserver,
    ) -> Result<(), TransportError> {
        let (_, method, params) = validate_facade_request(request)?;
        if method != "subscriptions/listen" {
            return Err(TransportError::Request);
        }
        self.ensure_initialized(cancellation, observer).await?;
        let filter: SubscriptionFilter = serde_json::from_value(
            params
                .get("notifications")
                .cloned()
                .ok_or(TransportError::Request)?,
        )
        .map_err(|_| TransportError::Request)?;
        if !filter.task_ids.is_empty() {
            return Err(TransportError::Unsupported);
        }
        let initialize_request = self.initialize_request()?;
        let subscription_requests = filter
            .resource_subscriptions
            .into_iter()
            .map(|uri| {
                Ok(json!({
                    "jsonrpc": "2.0",
                    "id": self.next_id()?,
                    "method": "resources/subscribe",
                    "params": {"uri": uri},
                }))
            })
            .collect::<Result<Vec<_>, TransportError>>()?;
        self.inner
            .listen_legacy(
                &initialize_request,
                &Self::initialized_notification(),
                &subscription_requests,
                cancellation,
                observer,
            )
            .await
    }

    async fn shutdown(&self) -> Result<(), TransportError> {
        self.inner.shutdown().await
    }
}

fn legacy_client_capabilities(capabilities: &ClientCapabilities) -> Value {
    let mut mapped = Map::new();
    if let Some(elicitation) = &capabilities.elicitation {
        mapped.insert("elicitation".to_owned(), elicitation.clone());
    }
    if let Some(experimental) = &capabilities.experimental {
        mapped.insert(
            "experimental".to_owned(),
            serde_json::to_value(experimental).unwrap_or_else(|_| json!({})),
        );
    }
    Value::Object(mapped)
}

fn normalize_initialize_result(result: Value) -> Result<Value, TransportError> {
    let mut object = result
        .as_object()
        .cloned()
        .ok_or(TransportError::Response)?;
    if object
        .remove("protocolVersion")
        .and_then(|value| value.as_str().map(str::to_owned))
        .as_deref()
        != Some(MCP_LEGACY_PROTOCOL_VERSION)
    {
        return Err(TransportError::Response);
    }
    let capabilities = object
        .remove("capabilities")
        .and_then(|value| value.as_object().cloned())
        .ok_or(TransportError::Response)?;
    let mut projected_capabilities = Map::new();
    for name in ["tools", "resources", "prompts"] {
        if let Some(value) = capabilities.get(name) {
            projected_capabilities.insert(name.to_owned(), value.clone());
        }
    }
    if let Some(value) = capabilities
        .get("completions")
        .or_else(|| capabilities.get("completion"))
    {
        projected_capabilities.insert("completions".to_owned(), value.clone());
    }
    if let Some(value) = capabilities.get("experimental") {
        projected_capabilities.insert("experimental".to_owned(), value.clone());
    }
    let server_info = object
        .remove("serverInfo")
        .and_then(|value| value.as_object().cloned())
        .ok_or(TransportError::Response)?;
    let name = server_info
        .get("name")
        .and_then(Value::as_str)
        .filter(|value| !value.is_empty() && value.len() <= 256)
        .ok_or(TransportError::Response)?;
    let version = server_info
        .get("version")
        .and_then(Value::as_str)
        .filter(|value| !value.is_empty() && value.len() <= 128)
        .ok_or(TransportError::Response)?;
    let instructions = object.remove("instructions");
    if instructions
        .as_ref()
        .is_some_and(|value| value.as_str().is_none_or(|value| value.len() > 64 * 1024))
    {
        return Err(TransportError::Response);
    }
    Ok(json!({
        "resultType": "complete",
        "supportedVersions": [MCP_LEGACY_PROTOCOL_VERSION],
        "capabilities": projected_capabilities,
        "serverInfo": {
            "name": name,
            "version": version,
        },
        "ttlMs": LEGACY_CACHE_TTL_MS,
        "cacheScope": "private",
        "instructions": instructions,
    }))
}

fn validate_facade_request(
    request: &Value,
) -> Result<(Value, String, Map<String, Value>), TransportError> {
    let object = request.as_object().ok_or(TransportError::Request)?;
    if object.get("jsonrpc").and_then(Value::as_str) != Some("2.0") {
        return Err(TransportError::Request);
    }
    let request_id = object.get("id").cloned().ok_or(TransportError::Request)?;
    serde_json::from_value::<RequestId>(request_id.clone()).map_err(|_| TransportError::Request)?;
    let method = object
        .get("method")
        .and_then(Value::as_str)
        .filter(|method| {
            matches!(
                *method,
                "server/discover"
                    | "tools/list"
                    | "tools/call"
                    | "resources/list"
                    | "resources/templates/list"
                    | "resources/read"
                    | "prompts/list"
                    | "prompts/get"
                    | "completion/complete"
                    | "subscriptions/listen"
                    | "tasks/get"
                    | "tasks/update"
                    | "tasks/cancel"
            )
        })
        .ok_or(TransportError::Request)?
        .to_owned();
    let params = object
        .get("params")
        .and_then(Value::as_object)
        .cloned()
        .ok_or(TransportError::Request)?;
    let metadata = params
        .get("_meta")
        .and_then(Value::as_object)
        .ok_or(TransportError::Request)?;
    if metadata
        .get("io.modelcontextprotocol/protocolVersion")
        .and_then(Value::as_str)
        != Some(MCP_LEGACY_PROTOCOL_VERSION)
    {
        return Err(TransportError::Request);
    }
    Ok((request_id, method, params))
}

fn strip_facade_metadata(mut params: Map<String, Value>) -> Value {
    if let Some(Value::Object(metadata)) = params.remove("_meta") {
        if let Some(progress_token) = metadata.get("progressToken") {
            params.insert("_meta".to_owned(), json!({"progressToken": progress_token}));
        }
    }
    Value::Object(params)
}

fn normalize_legacy_response(mut response: Value, method: &str) -> Result<Value, TransportError> {
    if response.get("error").is_some() {
        return Ok(response);
    }
    let result = response
        .get_mut("result")
        .and_then(Value::as_object_mut)
        .ok_or(TransportError::Response)?;
    match method {
        "tools/list" | "resources/list" | "resources/templates/list" | "prompts/list" => {
            result.insert("resultType".to_owned(), json!("complete"));
            result.insert("ttlMs".to_owned(), json!(LEGACY_CACHE_TTL_MS));
            result.insert("cacheScope".to_owned(), json!("private"));
        }
        "tools/call" | "resources/read" | "prompts/get" | "completion/complete" => {
            result.insert("resultType".to_owned(), json!("complete"));
            if method == "resources/read" {
                result.insert("ttlMs".to_owned(), json!(LEGACY_CACHE_TTL_MS));
                result.insert("cacheScope".to_owned(), json!("private"));
            }
        }
        _ => return Err(TransportError::Unsupported),
    }
    Ok(response)
}

#[cfg(test)]
mod tests {
    use std::sync::Mutex as StdMutex;

    use super::*;
    use crate::{MetaMap, NoopNotificationObserver};

    #[derive(Default)]
    struct FixtureLegacyWire {
        requests: StdMutex<Vec<Value>>,
        notifications: StdMutex<Vec<Value>>,
    }

    #[async_trait]
    impl McpTransport for FixtureLegacyWire {
        fn kind(&self) -> TransportKind {
            TransportKind::Stdio
        }

        async fn exchange(
            &self,
            _request: &Value,
            _parameter_headers: &BTreeMap<String, String>,
            _cancellation: &CancellationToken,
            _observer: &dyn McpNotificationObserver,
        ) -> Result<Value, TransportError> {
            panic!("modern exchange reached legacy wire")
        }

        async fn exchange_legacy(
            &self,
            request: &Value,
            _cancellation: &CancellationToken,
            _observer: &dyn McpNotificationObserver,
        ) -> Result<Value, TransportError> {
            self.requests.lock().unwrap().push(request.clone());
            let result = match request["method"].as_str().unwrap() {
                "initialize" => json!({
                    "protocolVersion": MCP_LEGACY_PROTOCOL_VERSION,
                    "capabilities": {
                        "tools": {"listChanged": true},
                        "sampling": {},
                        "logging": {},
                    },
                    "serverInfo": {"name": "legacy-fixture", "version": "1.0.0"},
                }),
                "tools/list" => json!({
                    "tools": [{
                        "name": "echo",
                        "description": "legacy echo",
                        "inputSchema": {"type": "object"},
                    }]
                }),
                _ => return Err(TransportError::Unsupported),
            };
            Ok(json!({"jsonrpc":"2.0","id":request["id"],"result":result}))
        }

        async fn notify_legacy(
            &self,
            notification: &Value,
            _cancellation: &CancellationToken,
        ) -> Result<(), TransportError> {
            self.notifications
                .lock()
                .unwrap()
                .push(notification.clone());
            Ok(())
        }
    }

    fn client_info() -> ClientInfo {
        ClientInfo {
            name: "insight-test".to_owned(),
            version: "1.0.0".to_owned(),
            title: None,
            description: None,
            website_url: None,
            icons: Vec::new(),
        }
    }

    #[tokio::test]
    async fn legacy_adapter_initializes_once_and_never_emits_modern_or_deprecated_fields() {
        let wire = Arc::new(FixtureLegacyWire::default());
        let adapter = Arc::new(
            LegacyCompatibilityTransport::new(
                wire.clone(),
                client_info(),
                ClientCapabilities {
                    elicitation: Some(json!({})),
                    experimental: Some(MetaMap::empty()),
                    ..ClientCapabilities::default()
                },
            )
            .unwrap(),
        );
        let client =
            crate::McpClient::new_legacy(adapter, client_info(), ClientCapabilities::default())
                .unwrap();
        let cancellation = CancellationToken::new();
        let discovery = client
            .discover(&cancellation, &NoopNotificationObserver)
            .await
            .unwrap();
        assert_eq!(
            discovery.supported_versions,
            vec![MCP_LEGACY_PROTOCOL_VERSION]
        );
        assert!(discovery.capabilities.tools.is_some());
        assert!(!discovery.capabilities.additional.contains_key("sampling"));
        assert!(!discovery.capabilities.additional.contains_key("logging"));
        let catalog = client
            .list_tools(&cancellation, &NoopNotificationObserver)
            .await
            .unwrap();
        assert_eq!(catalog.tools[0].name, "echo");

        let requests = wire.requests.lock().unwrap();
        assert_eq!(
            requests
                .iter()
                .filter(|request| request["method"] == "initialize")
                .count(),
            1
        );
        assert_eq!(
            requests[0]["params"]["protocolVersion"],
            MCP_LEGACY_PROTOCOL_VERSION
        );
        assert!(requests[0]["params"]["capabilities"].get("roots").is_none());
        assert!(requests[0]["params"]["capabilities"]
            .get("sampling")
            .is_none());
        assert!(requests[1]["params"]
            .get("_meta")
            .is_none_or(|metadata| metadata.get("progressToken").is_some()));
        assert_eq!(wire.notifications.lock().unwrap().len(), 1);
    }

    #[test]
    fn legacy_adapter_rejects_tasks_and_modern_metadata() {
        let request = json!({
            "jsonrpc":"2.0",
            "id":1,
            "method":"tools/list",
            "params":{"_meta":{"io.modelcontextprotocol/protocolVersion":"2026-07-28"}}
        });
        assert_eq!(
            validate_facade_request(&request),
            Err(TransportError::Request)
        );
    }
}
