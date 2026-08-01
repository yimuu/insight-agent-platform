use std::{
    collections::BTreeMap,
    net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr, ToSocketAddrs},
    path::PathBuf,
    process::Stdio,
    sync::Arc,
    time::Duration,
};

use async_trait::async_trait;
use futures::StreamExt;
use reqwest::{
    header::{HeaderName, HeaderValue, ACCEPT, AUTHORIZATION, CONTENT_TYPE},
    redirect::Policy,
    Client, Url,
};
use serde_json::Value;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    process::{Child, ChildStdin, ChildStdout, Command},
    sync::Mutex as AsyncMutex,
    task::JoinHandle,
    time::Instant,
};
use tokio_util::sync::CancellationToken;

use crate::{
    encode_header_value,
    observability::record_transport_event,
    wire::{JsonRpcNotification, RequestId, MCP_PROTOCOL_VERSION},
    McpCodec, ProtocolLimits,
};

const ACCEPT_VALUE: &str = "application/json, text/event-stream";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TransportKind {
    StreamableHttp,
    Stdio,
}

pub trait McpNotificationObserver: Send + Sync {
    fn on_notification(
        &self,
        notification: &JsonRpcNotification<Value>,
    ) -> Result<(), TransportError>;
}

#[derive(Debug, Clone, Copy, Default)]
pub struct NoopNotificationObserver;

impl McpNotificationObserver for NoopNotificationObserver {
    fn on_notification(
        &self,
        _notification: &JsonRpcNotification<Value>,
    ) -> Result<(), TransportError> {
        Ok(())
    }
}

#[async_trait]
pub trait McpTransport: Send + Sync {
    fn kind(&self) -> TransportKind;

    async fn exchange(
        &self,
        request: &Value,
        parameter_headers: &BTreeMap<String, String>,
        cancellation: &CancellationToken,
        observer: &dyn McpNotificationObserver,
    ) -> Result<Value, TransportError>;

    async fn listen(
        &self,
        _request: &Value,
        _cancellation: &CancellationToken,
        _observer: &dyn McpNotificationObserver,
    ) -> Result<(), TransportError> {
        Err(TransportError::Unsupported)
    }

    /// Raw `2025-11-25` request path. It is intentionally separate from
    /// `exchange`: modern codecs never accept a legacy envelope and a
    /// connection cannot change eras after selection.
    async fn exchange_legacy(
        &self,
        _request: &Value,
        _cancellation: &CancellationToken,
        _observer: &dyn McpNotificationObserver,
    ) -> Result<Value, TransportError> {
        Err(TransportError::Unsupported)
    }

    /// Sends one legacy notification on the selected legacy session.
    async fn notify_legacy(
        &self,
        _notification: &Value,
        _cancellation: &CancellationToken,
    ) -> Result<(), TransportError> {
        Err(TransportError::Unsupported)
    }

    /// Opens the legacy notification channel. A stdio implementation uses a
    /// dedicated initialized process; HTTP uses the negotiated session stream.
    async fn listen_legacy(
        &self,
        _initialize_request: &Value,
        _initialized_notification: &Value,
        _subscription_requests: &[Value],
        _cancellation: &CancellationToken,
        _observer: &dyn McpNotificationObserver,
    ) -> Result<(), TransportError> {
        Err(TransportError::Unsupported)
    }

    async fn shutdown(&self) -> Result<(), TransportError> {
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StreamableHttpPolicy {
    endpoint: Url,
    request_timeout: Duration,
    connect_timeout: Duration,
    max_request_bytes: usize,
    max_response_bytes: usize,
    max_sse_line_bytes: usize,
    max_sse_event_bytes: usize,
    allow_plaintext_loopback: bool,
    server_id: Option<String>,
}

impl StreamableHttpPolicy {
    pub fn new(endpoint: &str) -> Result<Self, TransportError> {
        let policy = Self {
            endpoint: Url::parse(endpoint).map_err(|_| TransportError::Endpoint)?,
            request_timeout: Duration::from_secs(60),
            connect_timeout: Duration::from_secs(10),
            max_request_bytes: 1024 * 1024,
            max_response_bytes: 4 * 1024 * 1024,
            max_sse_line_bytes: 64 * 1024,
            max_sse_event_bytes: 1024 * 1024,
            allow_plaintext_loopback: false,
            server_id: None,
        };
        policy.validate()?;
        Ok(policy)
    }

    pub fn for_loopback_test(endpoint: &str) -> Result<Self, TransportError> {
        let mut policy = Self::new_unvalidated(endpoint)?;
        policy.allow_plaintext_loopback = true;
        policy.validate()?;
        Ok(policy)
    }

    fn new_unvalidated(endpoint: &str) -> Result<Self, TransportError> {
        Ok(Self {
            endpoint: Url::parse(endpoint).map_err(|_| TransportError::Endpoint)?,
            request_timeout: Duration::from_secs(60),
            connect_timeout: Duration::from_secs(10),
            max_request_bytes: 1024 * 1024,
            max_response_bytes: 4 * 1024 * 1024,
            max_sse_line_bytes: 64 * 1024,
            max_sse_event_bytes: 1024 * 1024,
            allow_plaintext_loopback: false,
            server_id: None,
        })
    }

    pub fn with_limits(
        mut self,
        max_response_bytes: usize,
        max_sse_line_bytes: usize,
        max_sse_event_bytes: usize,
    ) -> Result<Self, TransportError> {
        self.max_response_bytes = max_response_bytes;
        self.max_sse_line_bytes = max_sse_line_bytes;
        self.max_sse_event_bytes = max_sse_event_bytes;
        self.validate()?;
        Ok(self)
    }

    pub fn with_request_limit(mut self, max_request_bytes: usize) -> Result<Self, TransportError> {
        self.max_request_bytes = max_request_bytes;
        self.validate()?;
        Ok(self)
    }

    pub fn with_timeouts(
        mut self,
        connect_timeout: Duration,
        request_timeout: Duration,
    ) -> Result<Self, TransportError> {
        self.connect_timeout = connect_timeout;
        self.request_timeout = request_timeout;
        self.validate()?;
        Ok(self)
    }

    pub fn with_server_id(mut self, server_id: &str) -> Result<Self, TransportError> {
        if !valid_server_id_label(server_id) {
            return Err(TransportError::Endpoint);
        }
        self.server_id = Some(server_id.to_owned());
        Ok(self)
    }

    pub fn endpoint(&self) -> &Url {
        &self.endpoint
    }

    pub fn request_timeout(&self) -> Duration {
        self.request_timeout
    }

    fn validate(&self) -> Result<(), TransportError> {
        if !self.endpoint.username().is_empty()
            || self.endpoint.password().is_some()
            || self.endpoint.fragment().is_some()
            || self.endpoint.query().is_some()
            || self.request_timeout.is_zero()
            || self.connect_timeout.is_zero()
            || self.max_request_bytes == 0
            || self.max_response_bytes == 0
            || self.max_sse_line_bytes == 0
            || self.max_sse_event_bytes == 0
            || self.max_sse_line_bytes > self.max_sse_event_bytes
            || self.max_sse_event_bytes > self.max_response_bytes
        {
            return Err(TransportError::Endpoint);
        }
        match self.endpoint.scheme() {
            "https" => Ok(()),
            "http" if self.allow_plaintext_loopback && is_exact_loopback(&self.endpoint) => Ok(()),
            _ => Err(TransportError::Endpoint),
        }
    }
}

fn is_exact_loopback(url: &Url) -> bool {
    match url.host_str() {
        Some("localhost") => true,
        Some(host) => host
            .parse::<IpAddr>()
            .map(|address| address.is_loopback())
            .unwrap_or(false),
        None => false,
    }
}

/// Builds a redirect-free HTTP client whose DNS answers are validated once
/// and pinned for the lifetime of the client. This is shared by MCP transport
/// and OAuth/JWKS requests so neither path can re-resolve to a private address.
pub fn build_pinned_http_client(
    endpoint: &Url,
    connect_timeout: Duration,
    request_timeout: Duration,
    allow_loopback: bool,
) -> Result<Client, TransportError> {
    if connect_timeout.is_zero()
        || request_timeout.is_zero()
        || !endpoint.username().is_empty()
        || endpoint.password().is_some()
        || endpoint.fragment().is_some()
        || endpoint.query().is_some()
    {
        return Err(TransportError::Endpoint);
    }
    let loopback_endpoint = allow_loopback && is_exact_loopback(endpoint);
    if endpoint.scheme() != "https" && !(endpoint.scheme() == "http" && loopback_endpoint) {
        return Err(TransportError::Endpoint);
    }
    let host = endpoint
        .host_str()
        .ok_or(TransportError::Endpoint)?
        .to_owned();
    let port = endpoint
        .port_or_known_default()
        .ok_or(TransportError::Endpoint)?;
    let mut addresses = (host.as_str(), port)
        .to_socket_addrs()
        .map_err(|_| TransportError::Endpoint)?
        .collect::<Vec<_>>();
    addresses.sort_unstable();
    addresses.dedup();
    if !pinned_addresses_allowed(&addresses, loopback_endpoint) {
        return Err(TransportError::Endpoint);
    }
    Client::builder()
        // Environment/system proxies would make the proxy, rather than the
        // policy-checked and DNS-pinned endpoint, the actual connection
        // authority. MCP endpoint traffic is therefore always direct.
        .no_proxy()
        .redirect(Policy::none())
        .connect_timeout(connect_timeout)
        .timeout(request_timeout)
        .resolve_to_addrs(&host, &addresses)
        .build()
        .map_err(|_| TransportError::Client)
}

fn pinned_addresses_allowed(addresses: &[SocketAddr], loopback_endpoint: bool) -> bool {
    !addresses.is_empty()
        && if loopback_endpoint {
            addresses.iter().all(|address| address.ip().is_loopback())
        } else {
            addresses
                .iter()
                .all(|address| is_public_network_address(address.ip()))
        }
}

fn is_public_network_address(address: IpAddr) -> bool {
    match address {
        IpAddr::V4(address) => is_public_ipv4(address),
        IpAddr::V6(address) => is_public_ipv6(address),
    }
}

fn is_public_ipv4(address: Ipv4Addr) -> bool {
    let octets = address.octets();
    !address.is_private()
        && !address.is_loopback()
        && !address.is_link_local()
        && !address.is_broadcast()
        && !address.is_documentation()
        && !address.is_unspecified()
        && !address.is_multicast()
        && octets[0] != 0
        && !(octets[0] == 100 && (64..=127).contains(&octets[1]))
        && !(octets[0] == 198 && matches!(octets[1], 18 | 19))
        && octets[0] < 240
}

fn is_public_ipv6(address: Ipv6Addr) -> bool {
    let segments = address.segments();
    if let Some(mapped) = address.to_ipv4_mapped() {
        return is_public_ipv4(mapped);
    }
    !address.is_unspecified()
        && !address.is_loopback()
        && !address.is_unique_local()
        && !address.is_unicast_link_local()
        && !address.is_multicast()
        && segments[0] != 0
        && !(segments[0] == 0x2001 && segments[1] < 0x0200)
        && !(segments[0] == 0x2001 && segments[1] == 0x0db8)
        && !(segments[0] == 0x0100 && segments[1] == 0 && segments[2] == 0 && segments[3] == 0)
        && (segments[0] & 0xffc0) != 0xfec0
}

/// Process-local service credential. Secret values are intentionally omitted
/// from `Debug`, errors, and response evidence.
#[derive(Clone)]
pub struct HttpCredential {
    name: HeaderName,
    value: HeaderValue,
}

impl HttpCredential {
    pub fn bearer(value: &str) -> Result<Self, TransportError> {
        if value.is_empty() || value.chars().any(char::is_control) {
            return Err(TransportError::Credential);
        }
        Self::header(AUTHORIZATION.as_str(), &format!("Bearer {value}"))
    }

    pub fn header(name: &str, value: &str) -> Result<Self, TransportError> {
        let name =
            HeaderName::from_bytes(name.as_bytes()).map_err(|_| TransportError::Credential)?;
        let lowered = name.as_str();
        if lowered.starts_with("mcp-")
            || matches!(
                lowered,
                "accept"
                    | "content-type"
                    | "content-length"
                    | "host"
                    | "connection"
                    | "transfer-encoding"
            )
        {
            return Err(TransportError::Credential);
        }
        let value = HeaderValue::from_str(value).map_err(|_| TransportError::Credential)?;
        Ok(Self { name, value })
    }
}

impl std::fmt::Debug for HttpCredential {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("HttpCredential")
            .field("name", &self.name)
            .field("value", &"[REDACTED]")
            .finish()
    }
}

#[derive(Clone)]
pub struct StreamableHttpTransport {
    policy: StreamableHttpPolicy,
    credential: Option<HttpCredential>,
    client: Client,
    codec: McpCodec,
    legacy_session_id: Arc<AsyncMutex<Option<String>>>,
}

#[derive(Clone)]
pub struct StdioTransportPolicy {
    executable: PathBuf,
    args: Vec<String>,
    working_directory: PathBuf,
    environment: BTreeMap<String, String>,
    startup_timeout: Duration,
    request_timeout: Duration,
    shutdown_timeout: Duration,
    max_request_bytes: usize,
    max_response_bytes: usize,
    max_stderr_bytes: usize,
    server_id: Option<String>,
}

impl std::fmt::Debug for StdioTransportPolicy {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("StdioTransportPolicy")
            .field("executable", &self.executable)
            .field("args", &self.args)
            .field("working_directory", &self.working_directory)
            .field(
                "environment_names",
                &self.environment.keys().collect::<Vec<_>>(),
            )
            .field("environment_values", &"[REDACTED]")
            .field("startup_timeout", &self.startup_timeout)
            .field("request_timeout", &self.request_timeout)
            .field("shutdown_timeout", &self.shutdown_timeout)
            .finish_non_exhaustive()
    }
}

impl StdioTransportPolicy {
    pub fn new(
        executable: impl Into<PathBuf>,
        args: Vec<String>,
        working_directory: impl Into<PathBuf>,
        environment: BTreeMap<String, String>,
    ) -> Result<Self, TransportError> {
        let policy = Self {
            executable: executable.into(),
            args,
            working_directory: working_directory.into(),
            environment,
            startup_timeout: Duration::from_secs(10),
            request_timeout: Duration::from_secs(60),
            shutdown_timeout: Duration::from_secs(10),
            max_request_bytes: 1024 * 1024,
            max_response_bytes: 4 * 1024 * 1024,
            max_stderr_bytes: 64 * 1024,
            server_id: None,
        };
        policy.validate()?;
        Ok(policy)
    }

    pub fn with_timeouts(
        mut self,
        startup_timeout: Duration,
        request_timeout: Duration,
        shutdown_timeout: Duration,
    ) -> Result<Self, TransportError> {
        self.startup_timeout = startup_timeout;
        self.request_timeout = request_timeout;
        self.shutdown_timeout = shutdown_timeout;
        self.validate()?;
        Ok(self)
    }

    pub fn with_limits(
        mut self,
        max_request_bytes: usize,
        max_response_bytes: usize,
        max_stderr_bytes: usize,
    ) -> Result<Self, TransportError> {
        self.max_request_bytes = max_request_bytes;
        self.max_response_bytes = max_response_bytes;
        self.max_stderr_bytes = max_stderr_bytes;
        self.validate()?;
        Ok(self)
    }

    pub fn with_server_id(mut self, server_id: &str) -> Result<Self, TransportError> {
        if !valid_server_id_label(server_id) {
            return Err(TransportError::Process);
        }
        self.server_id = Some(server_id.to_owned());
        Ok(self)
    }

    fn validate(&self) -> Result<(), TransportError> {
        if !self.executable.is_absolute()
            || !self.working_directory.is_absolute()
            || self.args.len() > 256
            || self.args.iter().any(|argument| {
                argument.is_empty()
                    || argument.len() > 16 * 1024
                    || argument.chars().any(char::is_control)
            })
            || self.environment.len() > 256
            || self.environment.iter().any(|(name, value)| {
                !valid_environment_name(name)
                    || value.is_empty()
                    || value.len() > 64 * 1024
                    || value.contains('\0')
            })
            || self.startup_timeout.is_zero()
            || self.request_timeout.is_zero()
            || self.shutdown_timeout.is_zero()
            || self.max_request_bytes == 0
            || self.max_response_bytes == 0
            || self.max_stderr_bytes == 0
        {
            return Err(TransportError::Process);
        }
        Ok(())
    }
}

#[derive(Clone)]
pub struct StdioTransport {
    policy: StdioTransportPolicy,
    codec: McpCodec,
    supervisor: Arc<AsyncMutex<StdioSupervisor>>,
}

struct StdioSession {
    child: Child,
    stdin: ChildStdin,
    stdout: ChildStdout,
    pending: Vec<u8>,
    stderr_task: JoinHandle<()>,
}

#[derive(Default)]
struct StdioSupervisor {
    session: Option<StdioSession>,
    ever_started: bool,
    consecutive_failures: u32,
    restart_not_before: Option<Instant>,
}

impl std::fmt::Debug for StdioTransport {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("StdioTransport")
            .field("policy", &self.policy)
            .finish_non_exhaustive()
    }
}

impl StdioTransport {
    pub fn new(policy: StdioTransportPolicy) -> Result<Self, TransportError> {
        policy.validate()?;
        let codec = McpCodec::new(ProtocolLimits {
            max_message_bytes: policy.max_request_bytes.max(policy.max_response_bytes),
            ..ProtocolLimits::default()
        })
        .map_err(|_| TransportError::Client)?;
        Ok(Self {
            policy,
            codec,
            supervisor: Arc::new(AsyncMutex::new(StdioSupervisor::default())),
        })
    }

    fn spawn(&self) -> Result<Child, TransportError> {
        let mut command = Command::new(&self.policy.executable);
        command
            .args(&self.policy.args)
            .current_dir(&self.policy.working_directory)
            .env_clear()
            .envs(&self.policy.environment)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .kill_on_drop(true);
        command.spawn().map_err(|_| TransportError::Process)
    }

    async fn stop_child(&self, child: &mut Child) {
        let _ = child.start_kill();
        let _ = tokio::time::timeout(self.policy.shutdown_timeout, child.wait()).await;
    }

    async fn spawn_session(&self) -> Result<StdioSession, TransportError> {
        let operation = async {
            let mut child = self.spawn()?;
            tokio::task::yield_now().await;
            if child
                .try_wait()
                .map_err(|_| TransportError::Process)?
                .is_some()
            {
                return Err(TransportError::Process);
            }
            let stdin = child.stdin.take().ok_or(TransportError::Process)?;
            let stdout = child.stdout.take().ok_or(TransportError::Process)?;
            let stderr = child.stderr.take().ok_or(TransportError::Process)?;
            let stderr_task = tokio::spawn(drain_stderr(stderr, self.policy.max_stderr_bytes));
            Ok(StdioSession {
                child,
                stdin,
                stdout,
                pending: Vec::new(),
                stderr_task,
            })
        };
        tokio::time::timeout(self.policy.startup_timeout, operation)
            .await
            .map_err(|_| TransportError::Timeout)?
    }

    async fn ensure_session(&self, supervisor: &mut StdioSupervisor) -> Result<(), TransportError> {
        if supervisor.session.is_some() {
            return Ok(());
        }
        if let Some(not_before) = supervisor.restart_not_before {
            tokio::time::sleep_until(not_before).await;
        }
        let is_restart = supervisor.ever_started;
        let session = self.spawn_session().await?;
        supervisor.session = Some(session);
        supervisor.ever_started = true;
        supervisor.restart_not_before = None;
        if is_restart {
            record_transport_event("stdio", "process_restart");
            if let Some(server_id) = &self.policy.server_id {
                crate::record_operational_event(
                    server_id,
                    crate::McpOperationalEvent::StdioProcessRestarted,
                );
            }
        }
        Ok(())
    }

    async fn fail_session(&self, supervisor: &mut StdioSupervisor) {
        if let Some(mut session) = supervisor.session.take() {
            self.stop_child(&mut session.child).await;
            session.stderr_task.abort();
        }
        supervisor.consecutive_failures = supervisor.consecutive_failures.saturating_add(1);
        let exponent = supervisor.consecutive_failures.saturating_sub(1).min(6);
        let delay_ms = 100_u64.saturating_mul(1_u64 << exponent).min(5_000);
        supervisor.restart_not_before = Some(Instant::now() + Duration::from_millis(delay_ms));
    }

    fn reset_backoff(supervisor: &mut StdioSupervisor) {
        supervisor.consecutive_failures = 0;
        supervisor.restart_not_before = None;
    }

    async fn send_cancel_notification(&self, session: &mut StdioSession, request_id: &RequestId) {
        let notification = serde_json::json!({
            "jsonrpc": "2.0",
            "method": "notifications/cancelled",
            "params": {
                "requestId": request_id,
                "reason": "local cancellation"
            }
        });
        let Ok(bytes) = self.codec.encode(&notification) else {
            return;
        };
        let _ = session.stdin.write_all(&bytes).await;
        let _ = session.stdin.write_all(b"\n").await;
        let _ = session.stdin.flush().await;
    }

    pub async fn shutdown(&self) {
        let mut supervisor = self.supervisor.lock().await;
        if let Some(mut session) = supervisor.session.take() {
            self.stop_child(&mut session.child).await;
            session.stderr_task.abort();
        }
    }
}

#[async_trait]
impl McpTransport for StdioTransport {
    fn kind(&self) -> TransportKind {
        TransportKind::Stdio
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
        let bytes = self
            .codec
            .encode(request)
            .map_err(|_| TransportError::Request)?;
        if bytes.len() > self.policy.max_request_bytes {
            record_limit_rejection(
                self.policy.server_id.as_deref(),
                crate::McpOperationalEvent::BodyLimitRejected,
            );
            return Err(TransportError::RequestTooLarge);
        }
        let decoded = self
            .codec
            .decode_request(&bytes)
            .map_err(|_| TransportError::Request)?;
        let request_id = decoded.id;
        let progress_token = decoded.metadata.progress_token;
        let mut supervisor = self.supervisor.lock().await;
        if let Err(error) = self.ensure_session(&mut supervisor).await {
            self.fail_session(&mut supervisor).await;
            return Err(error);
        }
        let write_result = {
            let session = supervisor.session.as_mut().ok_or(TransportError::Process)?;
            async {
                session
                    .stdin
                    .write_all(&bytes)
                    .await
                    .map_err(|_| TransportError::Process)?;
                session
                    .stdin
                    .write_all(b"\n")
                    .await
                    .map_err(|_| TransportError::Process)?;
                session
                    .stdin
                    .flush()
                    .await
                    .map_err(|_| TransportError::Process)
            }
        };
        if let Err(error) = write_result.await {
            self.fail_session(&mut supervisor).await;
            return Err(error);
        }

        let result = {
            let session = supervisor.session.as_mut().ok_or(TransportError::Process)?;
            let operation = async {
                loop {
                    let line = read_stdio_line(
                        &mut session.stdout,
                        &mut session.pending,
                        self.policy.max_response_bytes,
                    )
                    .await?;
                    let value: Value =
                        serde_json::from_slice(&line).map_err(|_| TransportError::Stdout)?;
                    if value.get("id").is_some() {
                        verify_final_response(&value, &request_id)?;
                        if session
                            .pending
                            .iter()
                            .any(|byte| !byte.is_ascii_whitespace())
                        {
                            return Err(TransportError::MessageAfterResponse);
                        }
                        session.pending.clear();
                        return Ok(value);
                    }
                    let notification: JsonRpcNotification<Value> =
                        serde_json::from_value(value).map_err(|_| TransportError::ServerRequest)?;
                    if notification.jsonrpc != "2.0"
                        || !matches!(
                            notification.method.as_str(),
                            "notifications/progress" | "notifications/message"
                        )
                        || (notification.method == "notifications/progress"
                            && notification
                                .params
                                .as_ref()
                                .and_then(|params| params.get("progressToken"))
                                != progress_token.as_ref())
                    {
                        return Err(TransportError::Notification);
                    }
                    observer.on_notification(&notification)?;
                }
            };
            tokio::select! {
                biased;
                _ = cancellation.cancelled() => Err(TransportError::Cancelled),
                result = tokio::time::timeout(self.policy.request_timeout, operation) => {
                    result.map_err(|_| TransportError::Timeout)?
                }
            }
        };
        match result {
            Ok(value) => {
                Self::reset_backoff(&mut supervisor);
                Ok(value)
            }
            Err(error) => {
                if matches!(error, TransportError::Cancelled | TransportError::Timeout) {
                    if let Some(session) = supervisor.session.as_mut() {
                        self.send_cancel_notification(session, &request_id).await;
                    }
                }
                self.fail_session(&mut supervisor).await;
                Err(error)
            }
        }
    }

    async fn listen(
        &self,
        request: &Value,
        cancellation: &CancellationToken,
        observer: &dyn McpNotificationObserver,
    ) -> Result<(), TransportError> {
        let bytes = self
            .codec
            .encode(request)
            .map_err(|_| TransportError::Request)?;
        let decoded = self
            .codec
            .decode_request(&bytes)
            .map_err(|_| TransportError::Request)?;
        if decoded.method.as_str() != "subscriptions/listen" {
            return Err(TransportError::Request);
        }
        let request_id = decoded.id;
        let mut child = self.spawn()?;
        let stderr = child.stderr.take().ok_or(TransportError::Process)?;
        let stderr_task = tokio::spawn(drain_stderr(stderr, self.policy.max_stderr_bytes));
        let mut stdin = child.stdin.take().ok_or(TransportError::Process)?;
        let mut stdout = child.stdout.take().ok_or(TransportError::Process)?;
        stdin
            .write_all(&bytes)
            .await
            .map_err(|_| TransportError::Process)?;
        stdin
            .write_all(b"\n")
            .await
            .map_err(|_| TransportError::Process)?;
        stdin.flush().await.map_err(|_| TransportError::Process)?;
        let operation = async {
            let mut pending = Vec::new();
            let mut acknowledged = false;
            loop {
                let mut line =
                    read_stdio_line(&mut stdout, &mut pending, self.policy.max_response_bytes)
                        .await?;
                dispatch_subscription_event(&mut line, &mut acknowledged, &request_id, observer)?;
            }
        };
        let result = tokio::select! {
            biased;
            _ = cancellation.cancelled() => Ok(()),
            result = operation => result,
        };
        if cancellation.is_cancelled() {
            let notification = serde_json::json!({
                "jsonrpc": "2.0",
                "method": "notifications/cancelled",
                "params": {"requestId": request_id, "reason": "subscription closed"}
            });
            if let Ok(bytes) = self.codec.encode(&notification) {
                let _ = stdin.write_all(&bytes).await;
                let _ = stdin.write_all(b"\n").await;
                let _ = stdin.flush().await;
            }
        }
        self.stop_child(&mut child).await;
        stderr_task.abort();
        result
    }

    async fn exchange_legacy(
        &self,
        request: &Value,
        cancellation: &CancellationToken,
        observer: &dyn McpNotificationObserver,
    ) -> Result<Value, TransportError> {
        let (request_id, _method, progress_token) = legacy_request_facts(request)?;
        let bytes = self
            .codec
            .encode(request)
            .map_err(|_| TransportError::Request)?;
        if bytes.len() > self.policy.max_request_bytes {
            record_limit_rejection(
                self.policy.server_id.as_deref(),
                crate::McpOperationalEvent::BodyLimitRejected,
            );
            return Err(TransportError::RequestTooLarge);
        }
        let mut supervisor = self.supervisor.lock().await;
        if let Err(error) = self.ensure_session(&mut supervisor).await {
            self.fail_session(&mut supervisor).await;
            return Err(error);
        }
        let session = supervisor.session.as_mut().ok_or(TransportError::Process)?;
        write_stdio_message(&mut session.stdin, &bytes).await?;
        let operation = read_legacy_stdio_response(
            &mut session.stdout,
            &mut session.pending,
            &request_id,
            progress_token.as_ref(),
            self.policy.max_response_bytes,
            observer,
        );
        let result = tokio::select! {
            biased;
            _ = cancellation.cancelled() => Err(TransportError::Cancelled),
            result = tokio::time::timeout(self.policy.request_timeout, operation) => {
                result.map_err(|_| TransportError::Timeout)?
            }
        };
        match result {
            Ok(value) => {
                Self::reset_backoff(&mut supervisor);
                Ok(value)
            }
            Err(error) => {
                if matches!(error, TransportError::Cancelled | TransportError::Timeout) {
                    if let Some(session) = supervisor.session.as_mut() {
                        self.send_cancel_notification(session, &request_id).await;
                    }
                }
                self.fail_session(&mut supervisor).await;
                Err(error)
            }
        }
    }

    async fn notify_legacy(
        &self,
        notification: &Value,
        cancellation: &CancellationToken,
    ) -> Result<(), TransportError> {
        validate_legacy_client_notification(notification)?;
        let bytes = self
            .codec
            .encode(notification)
            .map_err(|_| TransportError::Request)?;
        if bytes.len() > self.policy.max_request_bytes {
            record_limit_rejection(
                self.policy.server_id.as_deref(),
                crate::McpOperationalEvent::BodyLimitRejected,
            );
            return Err(TransportError::RequestTooLarge);
        }
        let mut supervisor = self.supervisor.lock().await;
        self.ensure_session(&mut supervisor).await?;
        let session = supervisor.session.as_mut().ok_or(TransportError::Process)?;
        tokio::select! {
            biased;
            _ = cancellation.cancelled() => Err(TransportError::Cancelled),
            result = write_stdio_message(&mut session.stdin, &bytes) => result,
        }
    }

    async fn listen_legacy(
        &self,
        initialize_request: &Value,
        initialized_notification: &Value,
        subscription_requests: &[Value],
        cancellation: &CancellationToken,
        observer: &dyn McpNotificationObserver,
    ) -> Result<(), TransportError> {
        let (initialize_id, method, _) = legacy_request_facts(initialize_request)?;
        if method != "initialize" {
            return Err(TransportError::Request);
        }
        validate_legacy_client_notification(initialized_notification)?;
        let mut child = self.spawn()?;
        let stderr = child.stderr.take().ok_or(TransportError::Process)?;
        let stderr_task = tokio::spawn(drain_stderr(stderr, self.policy.max_stderr_bytes));
        let mut stdin = child.stdin.take().ok_or(TransportError::Process)?;
        let mut stdout = child.stdout.take().ok_or(TransportError::Process)?;
        let mut pending = Vec::new();

        let initialize_bytes = self
            .codec
            .encode(initialize_request)
            .map_err(|_| TransportError::Request)?;
        write_stdio_message(&mut stdin, &initialize_bytes).await?;
        read_legacy_stdio_response(
            &mut stdout,
            &mut pending,
            &initialize_id,
            None,
            self.policy.max_response_bytes,
            observer,
        )
        .await?;
        let initialized_bytes = self
            .codec
            .encode(initialized_notification)
            .map_err(|_| TransportError::Request)?;
        write_stdio_message(&mut stdin, &initialized_bytes).await?;
        for request in subscription_requests {
            let (request_id, method, _) = legacy_request_facts(request)?;
            if method != "resources/subscribe" {
                return Err(TransportError::Request);
            }
            let bytes = self
                .codec
                .encode(request)
                .map_err(|_| TransportError::Request)?;
            write_stdio_message(&mut stdin, &bytes).await?;
            read_legacy_stdio_response(
                &mut stdout,
                &mut pending,
                &request_id,
                None,
                self.policy.max_response_bytes,
                observer,
            )
            .await?;
        }

        let operation = async {
            loop {
                let line =
                    read_stdio_line(&mut stdout, &mut pending, self.policy.max_response_bytes)
                        .await?;
                dispatch_legacy_notification_bytes(&line, observer)?;
            }
        };
        let result = tokio::select! {
            biased;
            _ = cancellation.cancelled() => Ok(()),
            result = operation => result,
        };
        self.stop_child(&mut child).await;
        stderr_task.abort();
        result
    }

    async fn shutdown(&self) -> Result<(), TransportError> {
        StdioTransport::shutdown(self).await;
        Ok(())
    }
}

async fn write_stdio_message(stdin: &mut ChildStdin, bytes: &[u8]) -> Result<(), TransportError> {
    stdin
        .write_all(bytes)
        .await
        .map_err(|_| TransportError::Process)?;
    stdin
        .write_all(b"\n")
        .await
        .map_err(|_| TransportError::Process)?;
    stdin.flush().await.map_err(|_| TransportError::Process)
}

async fn read_legacy_stdio_response(
    stdout: &mut ChildStdout,
    pending: &mut Vec<u8>,
    request_id: &RequestId,
    progress_token: Option<&Value>,
    max_response_bytes: usize,
    observer: &dyn McpNotificationObserver,
) -> Result<Value, TransportError> {
    loop {
        let line = read_stdio_line(stdout, pending, max_response_bytes).await?;
        let value: Value = serde_json::from_slice(&line).map_err(|_| TransportError::Stdout)?;
        if value.get("id").is_some() {
            verify_final_response(&value, request_id)?;
            return Ok(value);
        }
        dispatch_legacy_notification(value, progress_token, observer)?;
    }
}

async fn read_stdio_line(
    stdout: &mut ChildStdout,
    pending: &mut Vec<u8>,
    max_bytes: usize,
) -> Result<Vec<u8>, TransportError> {
    loop {
        if let Some(position) = pending.iter().position(|byte| *byte == b'\n') {
            let mut line = pending.drain(..=position).collect::<Vec<_>>();
            line.pop();
            if line.last() == Some(&b'\r') {
                line.pop();
            }
            if line.is_empty() {
                continue;
            }
            return Ok(line);
        }
        if pending.len() > max_bytes {
            return Err(TransportError::ResponseTooLarge);
        }
        let mut chunk = [0_u8; 8192];
        let count = stdout
            .read(&mut chunk)
            .await
            .map_err(|_| TransportError::Process)?;
        if count == 0 {
            return Err(TransportError::Incomplete);
        }
        if pending.len().saturating_add(count) > max_bytes {
            return Err(TransportError::ResponseTooLarge);
        }
        pending.extend_from_slice(&chunk[..count]);
    }
}

async fn drain_stderr(mut stderr: tokio::process::ChildStderr, max_bytes: usize) {
    let mut observed = 0usize;
    let mut buffer = [0_u8; 4096];
    loop {
        let Ok(count) = stderr.read(&mut buffer).await else {
            return;
        };
        if count == 0 {
            return;
        }
        observed = observed.saturating_add(count).min(max_bytes);
    }
}

fn valid_environment_name(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 256
        && value.bytes().enumerate().all(|(index, byte)| {
            byte == b'_' || byte.is_ascii_uppercase() || (index > 0 && byte.is_ascii_digit())
        })
}

fn valid_server_id_label(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 64
        && value
            .bytes()
            .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'-')
}

fn record_limit_rejection(server_id: Option<&str>, event: crate::McpOperationalEvent) {
    if let Some(server_id) = server_id {
        crate::record_operational_event(server_id, event);
    }
}

fn record_stream_limit_result<T>(
    policy: &StreamableHttpPolicy,
    result: &Result<T, TransportError>,
) {
    let event = match result {
        Err(TransportError::SseLineTooLarge | TransportError::SseEventTooLarge) => {
            Some(crate::McpOperationalEvent::FrameLimitRejected)
        }
        Err(TransportError::RequestTooLarge | TransportError::ResponseTooLarge) => {
            Some(crate::McpOperationalEvent::BodyLimitRejected)
        }
        _ => None,
    };
    if let Some(event) = event {
        record_limit_rejection(policy.server_id.as_deref(), event);
    }
}

impl StreamableHttpTransport {
    pub fn new(
        policy: StreamableHttpPolicy,
        credential: Option<HttpCredential>,
    ) -> Result<Self, TransportError> {
        policy.validate()?;
        let client = build_pinned_http_client(
            &policy.endpoint,
            policy.connect_timeout,
            policy.request_timeout,
            policy.allow_plaintext_loopback,
        )?;
        let codec = McpCodec::new(ProtocolLimits {
            max_message_bytes: policy.max_request_bytes.max(policy.max_response_bytes),
            ..ProtocolLimits::default()
        })
        .map_err(|_| TransportError::Client)?;
        Ok(Self {
            policy,
            credential,
            client,
            codec,
            legacy_session_id: Arc::new(AsyncMutex::new(None)),
        })
    }

    pub fn policy(&self) -> &StreamableHttpPolicy {
        &self.policy
    }

    fn request_facts(
        &self,
        request: &Value,
    ) -> Result<(RequestId, String, Option<String>, Option<Value>), TransportError> {
        let bytes = self
            .codec
            .encode(request)
            .map_err(|_| TransportError::Request)?;
        if bytes.len() > self.policy.max_request_bytes {
            record_limit_rejection(
                self.policy.server_id.as_deref(),
                crate::McpOperationalEvent::BodyLimitRejected,
            );
            return Err(TransportError::RequestTooLarge);
        }
        let decoded = self
            .codec
            .decode_request(&bytes)
            .map_err(|_| TransportError::Request)?;
        let name = match decoded.method.as_str() {
            "tools/call" | "prompts/get" => decoded
                .params
                .get("name")
                .and_then(Value::as_str)
                .map(str::to_owned)
                .ok_or(TransportError::Request)
                .map(Some)?,
            "resources/read" => decoded
                .params
                .get("uri")
                .and_then(Value::as_str)
                .map(str::to_owned)
                .ok_or(TransportError::Request)
                .map(Some)?,
            "tasks/get" | "tasks/update" | "tasks/cancel" => decoded
                .params
                .get("taskId")
                .and_then(Value::as_str)
                .map(str::to_owned)
                .ok_or(TransportError::Request)
                .map(Some)?,
            _ => None,
        };
        let progress_token = decoded.metadata.progress_token;
        Ok((
            decoded.id,
            decoded.method.as_str().to_owned(),
            name,
            progress_token,
        ))
    }
}

#[async_trait]
impl McpTransport for StreamableHttpTransport {
    fn kind(&self) -> TransportKind {
        TransportKind::StreamableHttp
    }

    async fn exchange(
        &self,
        request: &Value,
        parameter_headers: &BTreeMap<String, String>,
        cancellation: &CancellationToken,
        observer: &dyn McpNotificationObserver,
    ) -> Result<Value, TransportError> {
        let (request_id, method, name, progress_token) = self.request_facts(request)?;
        let mut builder = self
            .client
            .post(self.policy.endpoint.clone())
            .header(CONTENT_TYPE, "application/json")
            .header(ACCEPT, ACCEPT_VALUE)
            .header("MCP-Protocol-Version", MCP_PROTOCOL_VERSION)
            .header("Mcp-Method", &method)
            .json(request);
        if let Some(name) = name {
            builder = builder.header("Mcp-Name", encode_header_value(&name));
        }
        for (name, value) in parameter_headers {
            let parsed_name =
                HeaderName::from_bytes(name.as_bytes()).map_err(|_| TransportError::Header)?;
            if !parsed_name.as_str().starts_with("mcp-param-") {
                return Err(TransportError::Header);
            }
            let parsed_value = HeaderValue::from_str(value).map_err(|_| TransportError::Header)?;
            builder = builder.header(parsed_name, parsed_value);
        }
        if let Some(credential) = &self.credential {
            builder = builder.header(credential.name.clone(), credential.value.clone());
        }

        let response = tokio::select! {
            biased;
            _ = cancellation.cancelled() => return Err(TransportError::Cancelled),
            response = builder.send() => response.map_err(|_| TransportError::Network)?,
        };
        let status = response.status();
        let content_type = response
            .headers()
            .get(CONTENT_TYPE)
            .and_then(|value| value.to_str().ok())
            .and_then(|value| value.split(';').next())
            .map(str::trim)
            .map(str::to_ascii_lowercase);

        if !status.is_success() {
            let bytes =
                read_limited(response, self.policy.max_response_bytes, cancellation).await?;
            if let Ok(value) = serde_json::from_slice::<Value>(&bytes) {
                if let Some(code) = value
                    .get("error")
                    .and_then(|error| error.get("code"))
                    .and_then(Value::as_i64)
                {
                    return Err(TransportError::Remote {
                        status: status.as_u16(),
                        code,
                    });
                }
            }
            return Err(TransportError::HttpStatus(status.as_u16()));
        }

        match content_type.as_deref() {
            Some("application/json") => {
                let bytes =
                    read_limited(response, self.policy.max_response_bytes, cancellation).await?;
                let value = serde_json::from_slice::<Value>(&bytes)
                    .map_err(|_| TransportError::Response)?;
                verify_final_response(&value, &request_id)?;
                Ok(value)
            }
            Some("text/event-stream") => {
                let result = read_sse(
                    response,
                    &request_id,
                    progress_token.as_ref(),
                    &self.policy,
                    cancellation,
                    observer,
                )
                .await;
                record_stream_limit_result(&self.policy, &result);
                result
            }
            _ => Err(TransportError::ContentType),
        }
    }

    async fn listen(
        &self,
        request: &Value,
        cancellation: &CancellationToken,
        observer: &dyn McpNotificationObserver,
    ) -> Result<(), TransportError> {
        let (request_id, method, name, _) = self.request_facts(request)?;
        if method != "subscriptions/listen" || name.is_some() {
            return Err(TransportError::Request);
        }
        let mut builder = self
            .client
            .post(self.policy.endpoint.clone())
            .header(CONTENT_TYPE, "application/json")
            .header(ACCEPT, "text/event-stream")
            .header("MCP-Protocol-Version", MCP_PROTOCOL_VERSION)
            .header("Mcp-Method", &method)
            .json(request);
        if let Some(credential) = &self.credential {
            builder = builder.header(credential.name.clone(), credential.value.clone());
        }
        let response = tokio::select! {
            biased;
            _ = cancellation.cancelled() => return Ok(()),
            response = builder.send() => response.map_err(|_| TransportError::Network)?,
        };
        if !response.status().is_success() {
            return Err(TransportError::HttpStatus(response.status().as_u16()));
        }
        let content_type = response
            .headers()
            .get(CONTENT_TYPE)
            .and_then(|value| value.to_str().ok())
            .and_then(|value| value.split(';').next())
            .map(str::trim);
        if content_type != Some("text/event-stream") {
            return Err(TransportError::ContentType);
        }
        let result =
            read_subscription_sse(response, &request_id, &self.policy, cancellation, observer)
                .await;
        record_stream_limit_result(&self.policy, &result);
        result
    }

    async fn exchange_legacy(
        &self,
        request: &Value,
        cancellation: &CancellationToken,
        observer: &dyn McpNotificationObserver,
    ) -> Result<Value, TransportError> {
        let (request_id, method, progress_token) = legacy_request_facts(request)?;
        let session_id = self.legacy_session_id.lock().await.clone();
        let mut builder = self
            .client
            .post(self.policy.endpoint.clone())
            .header(CONTENT_TYPE, "application/json")
            .header(ACCEPT, ACCEPT_VALUE)
            .json(request);
        if method != "initialize" {
            builder = builder.header(
                "MCP-Protocol-Version",
                crate::wire::MCP_LEGACY_PROTOCOL_VERSION,
            );
        }
        if let Some(session_id) = session_id {
            builder = builder.header("Mcp-Session-Id", session_id);
        }
        if let Some(credential) = &self.credential {
            builder = builder.header(credential.name.clone(), credential.value.clone());
        }
        let response = tokio::select! {
            biased;
            _ = cancellation.cancelled() => return Err(TransportError::Cancelled),
            response = builder.send() => response.map_err(|_| TransportError::Network)?,
        };
        let status = response.status();
        let response_session_id = response
            .headers()
            .get("Mcp-Session-Id")
            .and_then(|value| value.to_str().ok())
            .map(str::to_owned);
        if let Some(response_session_id) = response_session_id {
            if response_session_id.is_empty()
                || response_session_id.len() > 1024
                || response_session_id.chars().any(char::is_control)
            {
                return Err(TransportError::Response);
            }
            let mut selected = self.legacy_session_id.lock().await;
            if selected
                .as_ref()
                .is_some_and(|selected| selected != &response_session_id)
            {
                return Err(TransportError::Correlation);
            }
            *selected = Some(response_session_id);
        }
        let content_type = response
            .headers()
            .get(CONTENT_TYPE)
            .and_then(|value| value.to_str().ok())
            .and_then(|value| value.split(';').next())
            .map(str::trim)
            .map(str::to_ascii_lowercase);
        if !status.is_success() {
            let bytes =
                read_limited(response, self.policy.max_response_bytes, cancellation).await?;
            if let Ok(value) = serde_json::from_slice::<Value>(&bytes) {
                if let Some(code) = value
                    .get("error")
                    .and_then(|error| error.get("code"))
                    .and_then(Value::as_i64)
                {
                    return Err(TransportError::Remote {
                        status: status.as_u16(),
                        code,
                    });
                }
            }
            return Err(TransportError::HttpStatus(status.as_u16()));
        }
        match content_type.as_deref() {
            Some("application/json") => {
                let bytes =
                    read_limited(response, self.policy.max_response_bytes, cancellation).await?;
                let value = serde_json::from_slice(&bytes).map_err(|_| TransportError::Response)?;
                verify_final_response(&value, &request_id)?;
                Ok(value)
            }
            Some("text/event-stream") => {
                let result = read_sse(
                    response,
                    &request_id,
                    progress_token.as_ref(),
                    &self.policy,
                    cancellation,
                    observer,
                )
                .await;
                record_stream_limit_result(&self.policy, &result);
                result
            }
            _ => Err(TransportError::ContentType),
        }
    }

    async fn notify_legacy(
        &self,
        notification: &Value,
        cancellation: &CancellationToken,
    ) -> Result<(), TransportError> {
        validate_legacy_client_notification(notification)?;
        let session_id = self.legacy_session_id.lock().await.clone();
        let mut builder = self
            .client
            .post(self.policy.endpoint.clone())
            .header(CONTENT_TYPE, "application/json")
            .header(ACCEPT, ACCEPT_VALUE)
            .header(
                "MCP-Protocol-Version",
                crate::wire::MCP_LEGACY_PROTOCOL_VERSION,
            )
            .json(notification);
        if let Some(session_id) = session_id {
            builder = builder.header("Mcp-Session-Id", session_id);
        }
        if let Some(credential) = &self.credential {
            builder = builder.header(credential.name.clone(), credential.value.clone());
        }
        let response = tokio::select! {
            biased;
            _ = cancellation.cancelled() => return Err(TransportError::Cancelled),
            response = builder.send() => response.map_err(|_| TransportError::Network)?,
        };
        if !response.status().is_success() {
            return Err(TransportError::HttpStatus(response.status().as_u16()));
        }
        Ok(())
    }

    async fn listen_legacy(
        &self,
        _initialize_request: &Value,
        _initialized_notification: &Value,
        subscription_requests: &[Value],
        cancellation: &CancellationToken,
        observer: &dyn McpNotificationObserver,
    ) -> Result<(), TransportError> {
        for request in subscription_requests {
            let (_, method, _) = legacy_request_facts(request)?;
            if method != "resources/subscribe" {
                return Err(TransportError::Request);
            }
            self.exchange_legacy(request, cancellation, observer)
                .await?;
        }
        let session_id = self.legacy_session_id.lock().await.clone();
        let mut builder = self
            .client
            .get(self.policy.endpoint.clone())
            .header(ACCEPT, "text/event-stream")
            .header(
                "MCP-Protocol-Version",
                crate::wire::MCP_LEGACY_PROTOCOL_VERSION,
            );
        if let Some(session_id) = session_id {
            builder = builder.header("Mcp-Session-Id", session_id);
        }
        if let Some(credential) = &self.credential {
            builder = builder.header(credential.name.clone(), credential.value.clone());
        }
        let response = tokio::select! {
            biased;
            _ = cancellation.cancelled() => return Ok(()),
            response = builder.send() => response.map_err(|_| TransportError::Network)?,
        };
        if !response.status().is_success() {
            return Err(TransportError::HttpStatus(response.status().as_u16()));
        }
        let content_type = response
            .headers()
            .get(CONTENT_TYPE)
            .and_then(|value| value.to_str().ok())
            .and_then(|value| value.split(';').next())
            .map(str::trim);
        if content_type != Some("text/event-stream") {
            return Err(TransportError::ContentType);
        }
        let result = read_legacy_sse(response, &self.policy, cancellation, observer).await;
        record_stream_limit_result(&self.policy, &result);
        result
    }

    async fn shutdown(&self) -> Result<(), TransportError> {
        let session_id = self.legacy_session_id.lock().await.take();
        let Some(session_id) = session_id else {
            return Ok(());
        };
        let mut builder = self
            .client
            .delete(self.policy.endpoint.clone())
            .header(
                "MCP-Protocol-Version",
                crate::wire::MCP_LEGACY_PROTOCOL_VERSION,
            )
            .header("Mcp-Session-Id", session_id);
        if let Some(credential) = &self.credential {
            builder = builder.header(credential.name.clone(), credential.value.clone());
        }
        let _ = builder.send().await;
        Ok(())
    }
}

async fn read_limited(
    response: reqwest::Response,
    limit: usize,
    cancellation: &CancellationToken,
) -> Result<Vec<u8>, TransportError> {
    if response
        .content_length()
        .is_some_and(|length| length > limit as u64)
    {
        return Err(TransportError::ResponseTooLarge);
    }
    let mut stream = response.bytes_stream();
    let mut output = Vec::new();
    loop {
        let next = tokio::select! {
            biased;
            _ = cancellation.cancelled() => return Err(TransportError::Cancelled),
            next = stream.next() => next,
        };
        let Some(chunk) = next else {
            break;
        };
        let chunk = chunk.map_err(|_| TransportError::Network)?;
        if output.len().saturating_add(chunk.len()) > limit {
            return Err(TransportError::ResponseTooLarge);
        }
        output.extend_from_slice(&chunk);
    }
    Ok(output)
}

async fn read_sse(
    response: reqwest::Response,
    request_id: &RequestId,
    progress_token: Option<&Value>,
    policy: &StreamableHttpPolicy,
    cancellation: &CancellationToken,
    observer: &dyn McpNotificationObserver,
) -> Result<Value, TransportError> {
    let mut stream = response.bytes_stream();
    let mut buffer = Vec::new();
    let mut data = Vec::<u8>::new();
    let mut total = 0usize;
    let mut final_response = None;
    loop {
        let next = tokio::select! {
            biased;
            _ = cancellation.cancelled() => return Err(TransportError::Cancelled),
            next = stream.next() => next,
        };
        let Some(chunk) = next else {
            break;
        };
        let chunk = chunk.map_err(|_| TransportError::Network)?;
        total = total.saturating_add(chunk.len());
        if total > policy.max_response_bytes {
            return Err(TransportError::ResponseTooLarge);
        }
        buffer.extend_from_slice(&chunk);
        while let Some(position) = buffer.iter().position(|byte| *byte == b'\n') {
            if position > policy.max_sse_line_bytes {
                return Err(TransportError::SseLineTooLarge);
            }
            let mut line = buffer.drain(..=position).collect::<Vec<_>>();
            line.pop();
            if line.last() == Some(&b'\r') {
                line.pop();
            }
            process_sse_line(
                &line,
                &mut data,
                &mut final_response,
                request_id,
                progress_token,
                policy,
                observer,
            )?;
        }
        if buffer.len() > policy.max_sse_line_bytes {
            return Err(TransportError::SseLineTooLarge);
        }
    }
    if !buffer.is_empty() {
        process_sse_line(
            &buffer,
            &mut data,
            &mut final_response,
            request_id,
            progress_token,
            policy,
            observer,
        )?;
    }
    if !data.is_empty() {
        dispatch_sse_event(
            &mut data,
            &mut final_response,
            request_id,
            progress_token,
            observer,
        )?;
    }
    final_response.ok_or(TransportError::Incomplete)
}

async fn read_subscription_sse(
    response: reqwest::Response,
    request_id: &RequestId,
    policy: &StreamableHttpPolicy,
    cancellation: &CancellationToken,
    observer: &dyn McpNotificationObserver,
) -> Result<(), TransportError> {
    let mut stream = response.bytes_stream();
    let mut buffer = Vec::new();
    let mut data = Vec::<u8>::new();
    let mut acknowledged = false;
    loop {
        let next = tokio::select! {
            biased;
            _ = cancellation.cancelled() => return Ok(()),
            next = stream.next() => next,
        };
        let Some(chunk) = next else {
            return Err(TransportError::Incomplete);
        };
        let chunk = chunk.map_err(|_| TransportError::Network)?;
        buffer.extend_from_slice(&chunk);
        while let Some(position) = buffer.iter().position(|byte| *byte == b'\n') {
            if position > policy.max_sse_line_bytes {
                return Err(TransportError::SseLineTooLarge);
            }
            let mut line = buffer.drain(..=position).collect::<Vec<_>>();
            line.pop();
            if line.last() == Some(&b'\r') {
                line.pop();
            }
            process_subscription_line(
                &line,
                &mut data,
                &mut acknowledged,
                request_id,
                policy,
                observer,
            )?;
        }
        if buffer.len() > policy.max_sse_line_bytes {
            return Err(TransportError::SseLineTooLarge);
        }
    }
}

async fn read_legacy_sse(
    response: reqwest::Response,
    policy: &StreamableHttpPolicy,
    cancellation: &CancellationToken,
    observer: &dyn McpNotificationObserver,
) -> Result<(), TransportError> {
    let mut stream = response.bytes_stream();
    let mut buffer = Vec::new();
    let mut data = Vec::<u8>::new();
    let mut total = 0usize;
    loop {
        let next = tokio::select! {
            biased;
            _ = cancellation.cancelled() => return Ok(()),
            next = stream.next() => next,
        };
        let Some(chunk) = next else {
            return Err(TransportError::Incomplete);
        };
        let chunk = chunk.map_err(|_| TransportError::Network)?;
        total = total.saturating_add(chunk.len());
        if total > policy.max_response_bytes {
            return Err(TransportError::ResponseTooLarge);
        }
        buffer.extend_from_slice(&chunk);
        while let Some(position) = buffer.iter().position(|byte| *byte == b'\n') {
            if position > policy.max_sse_line_bytes {
                return Err(TransportError::SseLineTooLarge);
            }
            let mut line = buffer.drain(..=position).collect::<Vec<_>>();
            line.pop();
            if line.last() == Some(&b'\r') {
                line.pop();
            }
            if line.is_empty() {
                if !data.is_empty() {
                    dispatch_legacy_notification_bytes(&data, observer)?;
                    data.clear();
                }
            } else if !line.starts_with(b":") {
                if let Some(value) = line.strip_prefix(b"data:") {
                    let value = value.strip_prefix(b" ").unwrap_or(value);
                    if !data.is_empty() {
                        data.push(b'\n');
                    }
                    if data.len().saturating_add(value.len()) > policy.max_sse_event_bytes {
                        return Err(TransportError::SseEventTooLarge);
                    }
                    data.extend_from_slice(value);
                }
            }
        }
        if buffer.len() > policy.max_sse_line_bytes {
            return Err(TransportError::SseLineTooLarge);
        }
    }
}

fn process_subscription_line(
    line: &[u8],
    data: &mut Vec<u8>,
    acknowledged: &mut bool,
    request_id: &RequestId,
    policy: &StreamableHttpPolicy,
    observer: &dyn McpNotificationObserver,
) -> Result<(), TransportError> {
    if line.is_empty() {
        return dispatch_subscription_event(data, acknowledged, request_id, observer);
    }
    if line.starts_with(b":") {
        return Ok(());
    }
    let Some(value) = line.strip_prefix(b"data:") else {
        return Ok(());
    };
    let value = value.strip_prefix(b" ").unwrap_or(value);
    if !data.is_empty() {
        data.push(b'\n');
    }
    if data.len().saturating_add(value.len()) > policy.max_sse_event_bytes {
        return Err(TransportError::SseEventTooLarge);
    }
    data.extend_from_slice(value);
    Ok(())
}

fn dispatch_subscription_event(
    data: &mut Vec<u8>,
    acknowledged: &mut bool,
    request_id: &RequestId,
    observer: &dyn McpNotificationObserver,
) -> Result<(), TransportError> {
    if data.is_empty() {
        return Ok(());
    }
    let notification: JsonRpcNotification<Value> =
        serde_json::from_slice(data).map_err(|_| TransportError::SseEvent)?;
    data.clear();
    if notification.jsonrpc != "2.0"
        || !matches!(
            notification.method.as_str(),
            "notifications/subscriptions/acknowledged"
                | "notifications/tools/list_changed"
                | "notifications/resources/list_changed"
                | "notifications/prompts/list_changed"
                | "notifications/resources/updated"
        )
    {
        return Err(TransportError::Notification);
    }
    let subscription_id = notification
        .params
        .as_ref()
        .and_then(|params| params.get("_meta"))
        .and_then(|metadata| metadata.get("io.modelcontextprotocol/subscriptionId"))
        .cloned()
        .ok_or(TransportError::Correlation)
        .and_then(|value| {
            serde_json::from_value::<RequestId>(value).map_err(|_| TransportError::Correlation)
        })?;
    if subscription_id != *request_id {
        return Err(TransportError::Correlation);
    }
    if !*acknowledged {
        if notification.method != "notifications/subscriptions/acknowledged" {
            return Err(TransportError::SubscriptionAcknowledgement);
        }
        *acknowledged = true;
    } else if notification.method == "notifications/subscriptions/acknowledged" {
        return Err(TransportError::SubscriptionAcknowledgement);
    }
    observer.on_notification(&notification)
}

fn process_sse_line(
    line: &[u8],
    data: &mut Vec<u8>,
    final_response: &mut Option<Value>,
    request_id: &RequestId,
    progress_token: Option<&Value>,
    policy: &StreamableHttpPolicy,
    observer: &dyn McpNotificationObserver,
) -> Result<(), TransportError> {
    if line.is_empty() {
        return dispatch_sse_event(data, final_response, request_id, progress_token, observer);
    }
    if line.starts_with(b":") {
        return Ok(());
    }
    let Some(value) = line.strip_prefix(b"data:") else {
        return Ok(());
    };
    let value = value.strip_prefix(b" ").unwrap_or(value);
    if !data.is_empty() {
        data.push(b'\n');
    }
    if data.len().saturating_add(value.len()) > policy.max_sse_event_bytes {
        return Err(TransportError::SseEventTooLarge);
    }
    data.extend_from_slice(value);
    Ok(())
}

fn dispatch_sse_event(
    data: &mut Vec<u8>,
    final_response: &mut Option<Value>,
    request_id: &RequestId,
    progress_token: Option<&Value>,
    observer: &dyn McpNotificationObserver,
) -> Result<(), TransportError> {
    if data.is_empty() {
        return Ok(());
    }
    let value = serde_json::from_slice::<Value>(data).map_err(|_| TransportError::SseEvent)?;
    data.clear();
    if value.get("id").is_some() {
        if final_response.is_some() {
            return Err(TransportError::MultipleResponses);
        }
        verify_final_response(&value, request_id)?;
        *final_response = Some(value);
        return Ok(());
    }
    if final_response.is_some() {
        return Err(TransportError::MessageAfterResponse);
    }
    let notification: JsonRpcNotification<Value> =
        serde_json::from_value(value).map_err(|_| TransportError::ServerRequest)?;
    if notification.jsonrpc != "2.0"
        || !matches!(
            notification.method.as_str(),
            "notifications/progress" | "notifications/message"
        )
    {
        return Err(TransportError::Notification);
    }
    if notification.method == "notifications/progress"
        && notification
            .params
            .as_ref()
            .and_then(|params| params.get("progressToken"))
            != progress_token
    {
        return Err(TransportError::Correlation);
    }
    observer.on_notification(&notification)
}

fn verify_final_response(value: &Value, request_id: &RequestId) -> Result<(), TransportError> {
    if value.get("jsonrpc").and_then(Value::as_str) != Some("2.0")
        || serde_json::from_value::<RequestId>(
            value.get("id").cloned().ok_or(TransportError::Response)?,
        )
        .map_err(|_| TransportError::Response)?
            != *request_id
        || (value.get("result").is_some() == value.get("error").is_some())
    {
        return Err(TransportError::Response);
    }
    Ok(())
}

fn legacy_request_facts(
    request: &Value,
) -> Result<(RequestId, String, Option<Value>), TransportError> {
    let object = request.as_object().ok_or(TransportError::Request)?;
    if object.len() != 4
        || object.get("jsonrpc").and_then(Value::as_str) != Some("2.0")
        || object.get("result").is_some()
        || object.get("error").is_some()
    {
        return Err(TransportError::Request);
    }
    let request_id =
        serde_json::from_value(object.get("id").cloned().ok_or(TransportError::Request)?)
            .map_err(|_| TransportError::Request)?;
    let method = object
        .get("method")
        .and_then(Value::as_str)
        .filter(|method| {
            matches!(
                *method,
                "initialize"
                    | "tools/list"
                    | "tools/call"
                    | "resources/list"
                    | "resources/templates/list"
                    | "resources/read"
                    | "resources/subscribe"
                    | "resources/unsubscribe"
                    | "prompts/list"
                    | "prompts/get"
                    | "completion/complete"
            )
        })
        .ok_or(TransportError::Request)?
        .to_owned();
    let params = object
        .get("params")
        .and_then(Value::as_object)
        .ok_or(TransportError::Request)?;
    if params.get("_meta").is_some_and(|metadata| {
        metadata
            .get("io.modelcontextprotocol/protocolVersion")
            .is_some()
            || metadata
                .get("io.modelcontextprotocol/clientCapabilities")
                .is_some()
            || metadata.get("io.modelcontextprotocol/clientInfo").is_some()
    }) {
        return Err(TransportError::Request);
    }
    let progress_token = params
        .get("_meta")
        .and_then(|metadata| metadata.get("progressToken"))
        .cloned();
    Ok((request_id, method, progress_token))
}

fn validate_legacy_client_notification(notification: &Value) -> Result<(), TransportError> {
    let object = notification.as_object().ok_or(TransportError::Request)?;
    if object.get("jsonrpc").and_then(Value::as_str) != Some("2.0")
        || object.get("id").is_some()
        || !matches!(
            object.get("method").and_then(Value::as_str),
            Some("notifications/initialized" | "notifications/cancelled")
        )
        || object
            .get("params")
            .is_some_and(|params| !params.is_object())
    {
        return Err(TransportError::Request);
    }
    Ok(())
}

fn dispatch_legacy_notification_bytes(
    bytes: &[u8],
    observer: &dyn McpNotificationObserver,
) -> Result<(), TransportError> {
    let value = serde_json::from_slice(bytes).map_err(|_| TransportError::SseEvent)?;
    dispatch_legacy_notification(value, None, observer)
}

fn dispatch_legacy_notification(
    value: Value,
    progress_token: Option<&Value>,
    observer: &dyn McpNotificationObserver,
) -> Result<(), TransportError> {
    if value.get("id").is_some() {
        return Err(TransportError::ServerRequest);
    }
    let notification: JsonRpcNotification<Value> =
        serde_json::from_value(value).map_err(|_| TransportError::Notification)?;
    if notification.jsonrpc != "2.0"
        || !matches!(
            notification.method.as_str(),
            "notifications/progress"
                | "notifications/message"
                | "notifications/tools/list_changed"
                | "notifications/resources/list_changed"
                | "notifications/prompts/list_changed"
                | "notifications/resources/updated"
        )
        || (notification.method == "notifications/progress"
            && progress_token.is_some()
            && notification
                .params
                .as_ref()
                .and_then(|params| params.get("progressToken"))
                != progress_token)
    {
        return Err(TransportError::Notification);
    }
    observer.on_notification(&notification)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TransportError {
    Endpoint,
    Credential,
    Client,
    Request,
    Header,
    Network,
    Cancelled,
    HttpStatus(u16),
    Remote { status: u16, code: i64 },
    ContentType,
    Response,
    ResponseTooLarge,
    RequestTooLarge,
    SseLineTooLarge,
    SseEventTooLarge,
    SseEvent,
    ServerRequest,
    Notification,
    MultipleResponses,
    MessageAfterResponse,
    Incomplete,
    Unsupported,
    Correlation,
    SubscriptionAcknowledgement,
    Process,
    Stdout,
    Timeout,
}

impl std::fmt::Display for TransportError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(match self {
            Self::Endpoint => "invalid MCP HTTP endpoint policy",
            Self::Credential => "invalid MCP HTTP credential",
            Self::Client => "MCP HTTP client construction failed",
            Self::Request => "invalid MCP HTTP request",
            Self::Header => "invalid MCP HTTP mirrored header",
            Self::Network => "MCP HTTP transport failed",
            Self::Cancelled => "MCP HTTP request was cancelled",
            Self::HttpStatus(_) => "MCP HTTP server returned an error status",
            Self::Remote { .. } => "MCP HTTP server returned a protocol error",
            Self::ContentType => "MCP HTTP server returned an unsupported content type",
            Self::Response => "invalid MCP HTTP JSON-RPC response",
            Self::ResponseTooLarge => "MCP HTTP response exceeds the configured limit",
            Self::RequestTooLarge => "MCP HTTP request exceeds the configured limit",
            Self::SseLineTooLarge => "MCP SSE line exceeds the configured limit",
            Self::SseEventTooLarge => "MCP SSE event exceeds the configured limit",
            Self::SseEvent => "invalid MCP SSE event",
            Self::ServerRequest => "MCP server sent a forbidden independent request",
            Self::Notification => "invalid MCP request-scoped notification",
            Self::MultipleResponses => "MCP SSE stream returned multiple final responses",
            Self::MessageAfterResponse => "MCP SSE stream continued after its final response",
            Self::Incomplete => "MCP SSE stream ended without a final response",
            Self::Unsupported => "MCP transport does not support this operation",
            Self::Correlation => "MCP notification correlation failed",
            Self::SubscriptionAcknowledgement => {
                "MCP subscription acknowledgement sequence is invalid"
            }
            Self::Process => "MCP stdio process failed",
            Self::Stdout => "MCP stdio stdout contained invalid protocol data",
            Self::Timeout => "MCP transport request timed out",
        })
    }
}

impl std::error::Error for TransportError {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn endpoint_and_credentials_fail_closed() {
        assert!(StreamableHttpPolicy::new("http://example.com/mcp").is_err());
        assert!(StreamableHttpPolicy::new("https://user:secret@example.com/mcp").is_err());
        assert!(StreamableHttpPolicy::new("https://example.com/mcp?token=secret").is_err());
        assert!(StreamableHttpPolicy::for_loopback_test("http://127.0.0.1:3000/mcp").is_ok());
        assert!(StreamableHttpTransport::new(
            StreamableHttpPolicy::new("https://127.0.0.1/mcp").unwrap(),
            None,
        )
        .is_err());
        assert!(StreamableHttpTransport::new(
            StreamableHttpPolicy::new("https://[::1]/mcp").unwrap(),
            None,
        )
        .is_err());
        assert!(HttpCredential::header("Mcp-Method", "forged").is_err());
        let credential = HttpCredential::bearer("secret").unwrap();
        assert!(!format!("{credential:?}").contains("secret"));
    }

    #[test]
    fn endpoint_network_policy_rejects_non_public_address_classes() {
        for address in [
            "0.0.0.0",
            "10.0.0.1",
            "100.64.0.1",
            "127.0.0.1",
            "169.254.1.1",
            "172.16.0.1",
            "192.168.0.1",
            "198.18.0.1",
            "224.0.0.1",
            "240.0.0.1",
            "::",
            "::1",
            "fc00::1",
            "fe80::1",
            "fec0::1",
            "ff02::1",
            "2001:db8::1",
            "::ffff:10.0.0.1",
            "::ffff:127.0.0.1",
            "::ffff:169.254.169.254",
        ] {
            assert!(
                !is_public_network_address(address.parse().unwrap()),
                "{address} must not pass the MCP endpoint network policy"
            );
        }
        assert!(is_public_network_address("8.8.8.8".parse().unwrap()));
        assert!(is_public_network_address(
            "2606:4700:4700::1111".parse().unwrap()
        ));
    }

    #[test]
    fn dns_pin_requires_every_answer_to_share_the_allowed_network_class() {
        let public = "8.8.8.8:443".parse().unwrap();
        let second_public = "1.1.1.1:443".parse().unwrap();
        let private = "169.254.169.254:443".parse().unwrap();
        assert!(pinned_addresses_allowed(&[public, second_public], false));
        assert!(!pinned_addresses_allowed(&[public, private], false));
        assert!(pinned_addresses_allowed(
            &[
                "127.0.0.1:3000".parse().unwrap(),
                "[::1]:3000".parse().unwrap()
            ],
            true
        ));
        assert!(!pinned_addresses_allowed(&[public], true));
        assert!(!pinned_addresses_allowed(&[], false));
    }

    #[test]
    fn endpoint_url_canonicalization_is_idna_safe_and_rejects_credential_components() {
        let international = Url::parse("https://b\u{fc}cher.example/mcp").unwrap();
        assert_eq!(international.host_str(), Some("xn--bcher-kva.example"));
        assert!(StreamableHttpPolicy::new(international.as_str()).is_ok());
        for endpoint in [
            "https://user@example.com/mcp",
            "https://example.com/mcp?authorization=secret",
            "https://example.com/mcp#private",
        ] {
            assert!(StreamableHttpPolicy::new(endpoint).is_err());
        }
    }

    #[test]
    fn final_response_correlation_is_exact() {
        assert!(verify_final_response(
            &serde_json::json!({"jsonrpc":"2.0","id":1,"result":{}}),
            &RequestId::Integer(1)
        )
        .is_ok());
        assert!(verify_final_response(
            &serde_json::json!({"jsonrpc":"2.0","id":2,"result":{}}),
            &RequestId::Integer(1)
        )
        .is_err());
    }

    #[test]
    fn subscription_requires_one_correlated_acknowledgement_first() {
        let request_id = RequestId::Integer(7);
        let mut acknowledged = false;
        let mut update = br#"{"jsonrpc":"2.0","method":"notifications/tools/list_changed","params":{"_meta":{"io.modelcontextprotocol/subscriptionId":7}}}"#.to_vec();
        assert_eq!(
            dispatch_subscription_event(
                &mut update,
                &mut acknowledged,
                &request_id,
                &NoopNotificationObserver,
            ),
            Err(TransportError::SubscriptionAcknowledgement)
        );

        let mut acknowledgement = br#"{"jsonrpc":"2.0","method":"notifications/subscriptions/acknowledged","params":{"notifications":{"toolsListChanged":true},"_meta":{"io.modelcontextprotocol/subscriptionId":7}}}"#.to_vec();
        dispatch_subscription_event(
            &mut acknowledgement,
            &mut acknowledged,
            &request_id,
            &NoopNotificationObserver,
        )
        .unwrap();
        assert!(acknowledged);

        let mut update = br#"{"jsonrpc":"2.0","method":"notifications/tools/list_changed","params":{"_meta":{"io.modelcontextprotocol/subscriptionId":7}}}"#.to_vec();
        dispatch_subscription_event(
            &mut update,
            &mut acknowledged,
            &request_id,
            &NoopNotificationObserver,
        )
        .unwrap();

        let mut wrong_stream = br#"{"jsonrpc":"2.0","method":"notifications/resources/list_changed","params":{"_meta":{"io.modelcontextprotocol/subscriptionId":8}}}"#.to_vec();
        assert_eq!(
            dispatch_subscription_event(
                &mut wrong_stream,
                &mut acknowledged,
                &request_id,
                &NoopNotificationObserver,
            ),
            Err(TransportError::Correlation)
        );
    }
}
