//! Durable Provider discovery/test workers and active Revision projection.

use std::{
    collections::{BTreeMap, BTreeSet},
    fmt,
    sync::{Arc, RwLock},
    time::{Duration, Instant},
};

use async_trait::async_trait;
use chrono::Utc;
use futures::StreamExt;
use insight_api::v1::ProviderRevisionReadiness;
use insight_durable::{
    ClaimProviderOperationsCommand, CompleteProviderConnectionTestCommand,
    CompleteProviderConnectionTestResult, CompleteProviderDiscoveryCommand,
    CompleteProviderDiscoveryResult, ManagedProvider, ProviderConnectionTestClaim,
    ProviderConnectionTestMode, ProviderDiscoveryClaim, ProviderManagementDurableRepository,
    ProviderOperationFailure, ProviderOperationalState, ProviderRevision,
};
use insight_engine::{author::CompileError, execution::RunError};
use insight_resources::{
    models::{
        ChatEventStream, ChatModel, ChatRequest, ChatRequestMode, ChatResponse, ChatStream,
        ModelCapability, ModelDeploymentIdentity, ModelRegistry, ModelRequestCapability,
        ModelSelector, ProviderModelRegistration,
    },
    openai_chat::{
        client_builder_with_transport_policy, OpenAiChatLimits, OpenAiChatModel,
        OpenAiTransportPolicy,
    },
};
use insight_runtime::{ProviderRunAdmissionAuthority, ServiceError};
use serde::Deserialize;
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use tokio::task::JoinSet;
use tokio::time::MissedTickBehavior;
use tokio_util::sync::CancellationToken;

#[derive(Clone)]
pub struct ProviderManagementRuntimeConfig {
    pub enabled: bool,
    pub workers: usize,
    pub poll_interval: Duration,
    pub lease_duration: Duration,
    pub retention: chrono::Duration,
    pub max_response_bytes: usize,
    pub allow_loopback_development: bool,
    /// Environment variable names that the managed Provider resolver may
    /// dereference. Values are deliberately absent from this configuration.
    pub allowed_secret_names: BTreeSet<String>,
}

impl fmt::Debug for ProviderManagementRuntimeConfig {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ProviderManagementRuntimeConfig")
            .field("enabled", &self.enabled)
            .field("workers", &self.workers)
            .field("poll_interval", &self.poll_interval)
            .field("lease_duration", &self.lease_duration)
            .field("retention", &self.retention)
            .field("max_response_bytes", &self.max_response_bytes)
            .field(
                "allow_loopback_development",
                &self.allow_loopback_development,
            )
            .field("allowed_secret_names", &"[REDACTED]")
            .finish()
    }
}

/// Trusted manifest for the built-in managed OpenAI-compatible adapter.
/// Provider Revisions freeze its digest so runtimes with different adapter
/// contracts fail closed before activation or projection.
pub fn managed_openai_adapter_manifest() -> Value {
    json!({
        "adapter":{
            "type":"open_ai_compatible",
            "version":insight_resources::openai_chat::OPENAI_CHAT_ADAPTER_VERSION
        },
        "worker":{"version":insight_resources::openai_chat::OPENAI_CHAT_WORKER_VERSION},
        "transport":{"tls":"required","redirects":"deny"},
        "credential_slots":["bearer","api_key"],
        "model_discovery":"models_endpoint"
    })
}

pub fn managed_openai_adapter_manifest_digest() -> String {
    canonical_hash(&managed_openai_adapter_manifest())
        .expect("the built-in Provider adapter manifest is canonicalizable")
}

#[derive(Debug)]
pub struct ProviderManagementRuntime {
    cancellation: CancellationToken,
    task: Option<tokio::task::JoinHandle<()>>,
    projection_health: ProviderProjectionHealth,
}

#[derive(Debug, Clone, Default)]
pub struct ProviderProjectionHealth {
    state: Arc<RwLock<ProviderProjectionHealthState>>,
}

#[derive(Debug, Default)]
struct ProviderProjectionHealthState {
    last_success: Option<Instant>,
    healthy: bool,
}

impl ProviderProjectionHealth {
    fn record_success(&self) {
        if let Ok(mut state) = self.state.write() {
            state.last_success = Some(Instant::now());
            state.healthy = true;
        }
    }

    fn record_failure(&self) {
        if let Ok(mut state) = self.state.write() {
            state.healthy = false;
        }
    }

    pub fn observation(&self) -> (bool, Option<Duration>) {
        self.state.read().map_or((false, None), |state| {
            (
                state.healthy,
                state.last_success.map(|success| success.elapsed()),
            )
        })
    }
}

#[derive(Clone)]
pub struct DurableProviderRunAdmissionAuthority {
    repository: Arc<dyn ProviderManagementDurableRepository>,
}

impl DurableProviderRunAdmissionAuthority {
    pub fn new(repository: Arc<dyn ProviderManagementDurableRepository>) -> Self {
        Self { repository }
    }
}

#[async_trait]
impl ProviderRunAdmissionAuthority for DurableProviderRunAdmissionAuthority {
    async fn enabled_fences(
        &self,
        provider_ids: &BTreeSet<String>,
    ) -> Result<BTreeMap<String, u64>, ServiceError> {
        let mut fences = BTreeMap::new();
        for provider_id in provider_ids {
            let fence = self
                .repository
                .load_provider_fence(provider_id)
                .await
                .map_err(|_| {
                    ServiceError::new(
                        "RUN_SERVICE_UNAVAILABLE",
                        "Provider Run admission authority is unavailable",
                    )
                })?
                .filter(|fence| fence.operational_state == ProviderOperationalState::Enabled)
                .ok_or_else(|| {
                    ServiceError::new(
                        "PROVIDER_SUSPENDED",
                        "Provider route is not operational for Run admission",
                    )
                })?;
            fences.insert(provider_id.clone(), fence.suspension_fence);
        }
        Ok(fences)
    }
}

impl ProviderManagementRuntime {
    pub async fn start(
        repository: Arc<dyn ProviderManagementDurableRepository>,
        models: ModelRegistry,
        config: ProviderManagementRuntimeConfig,
    ) -> Result<Self, &'static str> {
        let cancellation = CancellationToken::new();
        let projection_health = ProviderProjectionHealth::default();
        if !config.enabled {
            return Ok(Self {
                cancellation,
                task: None,
                projection_health,
            });
        }
        let mut installed = BTreeMap::new();
        let mut archived = BTreeMap::new();
        let mut notifications = repository
            .open_provider_management_notification_stream()
            .await
            .map_err(|_| "notification")?;
        reconcile_active_revisions(&repository, &models, &config, &mut installed, &mut archived)
            .await?;
        projection_health.record_success();
        let task_cancellation = cancellation.clone();
        let task_projection_health = projection_health.clone();
        let task = tokio::spawn(async move {
            let mut poll = tokio::time::interval(config.poll_interval);
            poll.set_missed_tick_behavior(MissedTickBehavior::Skip);
            let mut retention = tokio::time::interval(Duration::from_secs(60));
            retention.set_missed_tick_behavior(MissedTickBehavior::Skip);
            let mut running = JoinSet::new();
            loop {
                tokio::select! {
                    _ = task_cancellation.cancelled() => break,
                    _ = retention.tick() => {
                        let _ = repository.cleanup_terminal_provider_operations(Utc::now() - config.retention, 1_000).await;
                    }
                    notification = async {
                        match notifications.as_mut() {
                            Some(stream) => stream.recv().await,
                            None => std::future::pending().await,
                        }
                    } => {
                        if notification.is_err() {
                            notifications = None;
                            task_projection_health.record_failure();
                            tracing::warn!(code="PROVIDER_NOTIFICATION_STREAM_UNAVAILABLE", "Provider management notification stream failed; safety poll remains authoritative");
                        } else if reconcile_active_revisions(&repository, &models, &config, &mut installed, &mut archived).await.is_err() {
                            task_projection_health.record_failure();
                            tracing::warn!(code="PROVIDER_REVISION_RECONCILE_FAILED", "Provider notification reconciliation failed");
                        } else {
                            task_projection_health.record_success();
                        }
                    }
                    _ = poll.tick() => {
                        if notifications.is_none() {
                            notifications = repository
                                .open_provider_management_notification_stream()
                                .await
                                .ok()
                                .flatten();
                        }
                        if reconcile_active_revisions(&repository, &models, &config, &mut installed, &mut archived).await.is_err() {
                            task_projection_health.record_failure();
                            tracing::warn!(code="PROVIDER_REVISION_RECONCILE_FAILED", "Provider active Revision reconciliation failed");
                        } else {
                            task_projection_health.record_success();
                        }
                        let available = config.workers.saturating_sub(running.len());
                        if available > 0 {
                            let now = Utc::now();
                            let lease = chrono::Duration::from_std(config.lease_duration).unwrap_or_else(|_| chrono::Duration::seconds(630));
                            if let Ok(claims) = repository.claim_provider_discoveries(ClaimProviderOperationsCommand {
                                worker_id: format!("provider-discovery-{}", std::process::id()), now,
                                lease_expires_at: now + lease, limit: u32::try_from(available).unwrap_or(u32::MAX),
                            }).await {
                                for claim in claims {
                                    let repository = Arc::clone(&repository);
                                    let config = config.clone();
                                    running.spawn(async move { process_discovery(repository, config, claim).await; });
                                }
                            }
                        }
                        let available = config.workers.saturating_sub(running.len());
                        if available > 0 {
                            let now = Utc::now();
                            let lease = chrono::Duration::from_std(config.lease_duration).unwrap_or_else(|_| chrono::Duration::seconds(630));
                            if let Ok(claims) = repository.claim_provider_connection_tests(ClaimProviderOperationsCommand {
                                worker_id: format!("provider-test-{}", std::process::id()), now,
                                lease_expires_at: now + lease, limit: u32::try_from(available).unwrap_or(u32::MAX),
                            }).await {
                                for claim in claims {
                                    let repository = Arc::clone(&repository);
                                    let config = config.clone();
                                    running.spawn(async move { process_connection_test(repository, config, claim).await; });
                                }
                            }
                        }
                    }
                    Some(_) = running.join_next(), if !running.is_empty() => {}
                }
            }
            task_cancellation.cancel();
            while running.join_next().await.is_some() {}
        });
        Ok(Self {
            cancellation,
            task: Some(task),
            projection_health,
        })
    }

    pub fn projection_health(&self) -> ProviderProjectionHealth {
        self.projection_health.clone()
    }

    pub async fn shutdown(&mut self) {
        self.cancellation.cancel();
        if let Some(task) = self.task.take() {
            let _ = task.await;
        }
    }
}

impl Drop for ProviderManagementRuntime {
    fn drop(&mut self) {
        self.cancellation.cancel();
        if let Some(task) = &self.task {
            task.abort();
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
struct RuntimeDraft {
    adapter: RuntimeAdapter,
    endpoint: String,
    credential: RuntimeCredential,
    transport: RuntimeTransport,
    models: Vec<RuntimeModel>,
    #[serde(default, rename = "description")]
    _description: Option<String>,
    #[serde(default, rename = "operator_note")]
    _operator_note: Option<String>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
struct RuntimeAdapter {
    #[serde(rename = "type")]
    adapter_type: String,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
enum RuntimeCredential {
    Bearer { reference: String },
    ApiKey { reference: String },
    None,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
struct RuntimeTransport {
    tls: String,
    redirects: String,
    connect_timeout_ms: u64,
    request_timeout_ms: u64,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
struct RuntimeModel {
    id: String,
    input: Vec<String>,
    capabilities: Vec<String>,
    provenance: Value,
}

fn canonical_hash(value: &Value) -> Result<String, ()> {
    let bytes = serde_jcs::to_vec(value).map_err(|_| ())?;
    let mut result = String::from("sha256:");
    for byte in Sha256::digest(bytes) {
        use std::fmt::Write as _;
        write!(&mut result, "{byte:02x}").map_err(|_| ())?;
    }
    Ok(result)
}

fn failure(code: &str, stage: &str, retryable: bool) -> ProviderOperationFailure {
    ProviderOperationFailure {
        code: code.to_owned(),
        stage: stage.to_owned(),
        retryable,
        correlation_id: None,
    }
}

fn environment_secret<'a>(
    reference: &'a str,
    config: &ProviderManagementRuntimeConfig,
) -> Option<(&'a str, Option<String>)> {
    reference
        .strip_prefix("secret://environment/")
        .filter(|name| !name.is_empty() && config.allowed_secret_names.contains(*name))
        .map(|name| (name, std::env::var(name).ok()))
}

fn transport_policy(
    config: &ProviderManagementRuntimeConfig,
    endpoint: &str,
) -> OpenAiTransportPolicy {
    if config.allow_loopback_development
        && reqwest::Url::parse(endpoint)
            .ok()
            .and_then(|url| url.host_str().map(str::to_owned))
            .is_some_and(|host| matches!(host.as_str(), "localhost" | "127.0.0.1" | "::1"))
    {
        OpenAiTransportPolicy::AllowLoopbackHttp
    } else {
        OpenAiTransportPolicy::HttpsOnly
    }
}

fn provider_client(
    draft: &RuntimeDraft,
    config: &ProviderManagementRuntimeConfig,
) -> Result<reqwest::Client, ProviderOperationFailure> {
    if draft.transport.tls != "required" || draft.transport.redirects != "deny" {
        return Err(failure("transport_policy_denied", "configuration", false));
    }
    client_builder_with_transport_policy(&draft.endpoint, transport_policy(config, &draft.endpoint))
        .map_err(|_| failure("transport_policy_denied", "configuration", false))?
        .connect_timeout(Duration::from_millis(draft.transport.connect_timeout_ms))
        .timeout(Duration::from_millis(draft.transport.request_timeout_ms))
        .build()
        .map_err(|_| failure("client_build_failed", "configuration", false))
}

fn models_url(endpoint: &str) -> Result<reqwest::Url, ProviderOperationFailure> {
    let mut url = reqwest::Url::parse(endpoint)
        .map_err(|_| failure("endpoint_invalid", "configuration", false))?;
    url.set_query(None);
    url.set_fragment(None);
    url.set_path(&format!("{}/models", url.path().trim_end_matches('/')));
    Ok(url)
}

fn completions_url(endpoint: &str) -> Result<reqwest::Url, ProviderOperationFailure> {
    let mut url = reqwest::Url::parse(endpoint)
        .map_err(|_| failure("endpoint_invalid", "configuration", false))?;
    url.set_query(None);
    url.set_fragment(None);
    url.set_path(&format!(
        "{}/chat/completions",
        url.path().trim_end_matches('/')
    ));
    Ok(url)
}

fn authorize(
    request: reqwest::RequestBuilder,
    credential: &RuntimeCredential,
    config: &ProviderManagementRuntimeConfig,
) -> Result<reqwest::RequestBuilder, ProviderOperationFailure> {
    match credential {
        RuntimeCredential::None => Ok(request),
        RuntimeCredential::Bearer { reference } => {
            let (_, value) = environment_secret(reference, config)
                .ok_or_else(|| failure("credential_reference_denied", "authentication", false))?;
            value
                .filter(|value| !value.trim().is_empty())
                .map(|value| request.bearer_auth(value))
                .ok_or_else(|| failure("credential_missing", "authentication", false))
        }
        RuntimeCredential::ApiKey { reference } => {
            let (_, value) = environment_secret(reference, config)
                .ok_or_else(|| failure("credential_reference_denied", "authentication", false))?;
            value
                .filter(|value| !value.trim().is_empty())
                .map(|value| request.header("x-api-key", value))
                .ok_or_else(|| failure("credential_missing", "authentication", false))
        }
    }
}

async fn bounded_body(
    response: reqwest::Response,
    max: usize,
) -> Result<Vec<u8>, ProviderOperationFailure> {
    let status = response.status();
    if !status.is_success() {
        return Err(failure(
            if status.as_u16() == 401 || status.as_u16() == 403 {
                "authentication_failed"
            } else {
                "provider_http_error"
            },
            "response_status",
            status.is_server_error(),
        ));
    }
    let mut stream = response.bytes_stream();
    let mut body = Vec::new();
    while let Some(chunk) = stream.next().await {
        let chunk = chunk.map_err(|_| failure("response_read_failed", "response_body", true))?;
        if chunk.len() > max.saturating_sub(body.len()) {
            return Err(failure("response_too_large", "response_body", false));
        }
        body.extend_from_slice(&chunk);
    }
    Ok(body)
}

async fn process_discovery(
    repository: Arc<dyn ProviderManagementDurableRepository>,
    config: ProviderManagementRuntimeConfig,
    claim: ProviderDiscoveryClaim,
) {
    let result = async {
        let draft: RuntimeDraft = serde_json::from_value(claim.draft_document.clone())
            .map_err(|_| failure("draft_invalid", "configuration", false))?;
        if draft.adapter.adapter_type != "open_ai_compatible" {
            return Err(failure("discovery_not_supported", "adapter", false));
        }
        let client = provider_client(&draft, &config)?;
        let request = authorize(
            client.get(models_url(&draft.endpoint)?),
            &draft.credential,
            &config,
        )?;
        let response = request
            .send()
            .await
            .map_err(|_| failure("provider_unreachable", "network", true))?;
        let body = bounded_body(response, config.max_response_bytes).await?;
        let document: Value = serde_json::from_slice(&body)
            .map_err(|_| failure("response_invalid", "response_decode", false))?;
        let data = document
            .get("data")
            .and_then(Value::as_array)
            .ok_or_else(|| failure("response_invalid", "response_decode", false))?;
        let mut ids = data
            .iter()
            .filter_map(|item| item.get("id").and_then(Value::as_str))
            .map(str::to_owned)
            .collect::<Vec<_>>();
        ids.sort();
        ids.dedup();
        if ids.is_empty()
            || ids.len() > 10_000
            || ids
                .iter()
                .any(|id| id.is_empty() || id.len() > 512 || id.chars().any(char::is_control))
        {
            return Err(failure(
                "model_catalog_invalid",
                "response_validation",
                false,
            ));
        }
        let snapshot =
            json!({"models":ids.into_iter().map(|id| json!({"id":id})).collect::<Vec<_>>()});
        Ok((
            canonical_hash(&snapshot)
                .map_err(|_| failure("snapshot_hash_failed", "snapshot", false))?,
            snapshot,
        ))
    }
    .await;
    let result = match result {
        Ok((catalog_fingerprint, snapshot_document)) => {
            CompleteProviderDiscoveryResult::Succeeded {
                catalog_fingerprint,
                snapshot_document,
            }
        }
        Err(failure) => CompleteProviderDiscoveryResult::Failed(failure),
    };
    if repository
        .complete_provider_discovery(CompleteProviderDiscoveryCommand {
            discovery_id: claim.operation.discovery_id,
            claim_token: claim.claim_token,
            now: Utc::now(),
            result,
        })
        .await
        .is_err()
    {
        tracing::warn!(
            code = "PROVIDER_DISCOVERY_COMPLETION_FAILED",
            "Provider discovery completion failed"
        );
    }
}

async fn process_connection_test(
    repository: Arc<dyn ProviderManagementDurableRepository>,
    config: ProviderManagementRuntimeConfig,
    claim: ProviderConnectionTestClaim,
) {
    let result = async {
        let draft: RuntimeDraft = serde_json::from_value(claim.draft_document.clone())
            .map_err(|_| failure("draft_invalid", "configuration", false))?;
        if draft.adapter.adapter_type != "open_ai_compatible" {
            return Err(failure("test_not_supported", "adapter", false));
        }
        let client = provider_client(&draft, &config)?;
        let started = std::time::Instant::now();
        let (request, fixture) = match claim.operation.mode {
            ProviderConnectionTestMode::Metadata => (
                authorize(
                    client.get(models_url(&draft.endpoint)?),
                    &draft.credential,
                    &config,
                )?,
                "metadata",
            ),
            ProviderConnectionTestMode::Canary | ProviderConnectionTestMode::CapabilityProbe => {
                let model = draft.models.first().ok_or_else(|| failure("model_required", "fixture", false))?;
                let payload = if claim.operation.mode == ProviderConnectionTestMode::Canary {
                    json!({"model":model.id,"messages":[{"role":"user","content":"ping"}],"max_tokens":1,"stream":false})
                } else {
                    json!({"model":model.id,"messages":[{"role":"user","content":"Return JSON null."}],"max_tokens":2,"stream":false,"response_format":{"type":"json_object"}})
                };
                (authorize(client.post(completions_url(&draft.endpoint)?).json(&payload), &draft.credential, &config)?, if claim.operation.mode == ProviderConnectionTestMode::Canary { "canary" } else { "capability_probe" })
            }
        };
        let response = request.send().await.map_err(|_| failure("provider_unreachable", "network", true))?;
        let status = response.status().as_u16();
        let body = bounded_body(response, config.max_response_bytes).await?;
        // Parse only enough to establish that a JSON response was received;
        // the Provider body itself is deliberately discarded.
        let _: Value = serde_json::from_slice(&body).map_err(|_| failure("response_invalid", "response_decode", false))?;
        let result = json!({"fixture":fixture,"http_status":status,"response_bytes":body.len(),"elapsed_ms":started.elapsed().as_millis(),"body_persisted":false});
        Ok((canonical_hash(&result).map_err(|_| failure("result_hash_failed", "result", false))?, result))
    }.await;
    let result = match result {
        Ok((result_hash, result)) => CompleteProviderConnectionTestResult::Succeeded {
            result_hash,
            result,
        },
        Err(failure) => CompleteProviderConnectionTestResult::Failed(failure),
    };
    if repository
        .complete_provider_connection_test(CompleteProviderConnectionTestCommand {
            test_id: claim.operation.test_id,
            claim_token: claim.claim_token,
            now: Utc::now(),
            result,
        })
        .await
        .is_err()
    {
        tracing::warn!(
            code = "PROVIDER_CONNECTION_TEST_COMPLETION_FAILED",
            "Provider connection test completion failed"
        );
    }
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
struct RuntimeRevision {
    provider_id: String,
    adapter: RuntimeRevisionAdapter,
    endpoint: String,
    credential: RuntimeRevisionCredential,
    transport: RuntimeTransport,
    models: Vec<RuntimeModel>,
    policy_fingerprint: String,
    source: Value,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
struct RuntimeRevisionAdapter {
    #[serde(rename = "type")]
    adapter_type: String,
    version: String,
    worker_version: String,
    manifest_digest: String,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
struct RuntimeRevisionCredential {
    #[serde(rename = "type")]
    credential_type: String,
    reference: Option<String>,
    reference_hash: Option<String>,
}

#[derive(Clone)]
struct FencedChatModel {
    inner: Arc<dyn ChatModel>,
    fence: Arc<dyn ProviderExecutionFence>,
    provider_id: String,
    revision_id: String,
    suspension_fence: u64,
}

#[async_trait]
trait ProviderExecutionFence: Send + Sync {
    async fn permits(&self, provider_id: &str, suspension_fence: u64) -> Result<bool, ()>;
}

struct DurableProviderExecutionFence {
    repository: Arc<dyn ProviderManagementDurableRepository>,
}

#[async_trait]
impl ProviderExecutionFence for DurableProviderExecutionFence {
    async fn permits(&self, provider_id: &str, suspension_fence: u64) -> Result<bool, ()> {
        self.repository
            .load_provider_fence(provider_id)
            .await
            .map(|fence| {
                fence.is_some_and(|fence| {
                    fence.operational_state == ProviderOperationalState::Enabled
                        && fence.suspension_fence == suspension_fence
                })
            })
            .map_err(|_| ())
    }
}

impl fmt::Debug for FencedChatModel {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("FencedChatModel")
            .field("provider_id", &self.provider_id)
            .field("revision_id", &self.revision_id)
            .field("suspension_fence", &self.suspension_fence)
            .field("inner", &"[REDACTED]")
            .finish()
    }
}

impl FencedChatModel {
    async fn check_fence(&self) -> Result<(), RunError> {
        let permitted = self
            .fence
            .permits(&self.provider_id, self.suspension_fence)
            .await
            .map_err(|_| {
                RunError::operation(
                    "PROVIDER_FENCE_UNAVAILABLE",
                    "Provider execution fence is unavailable",
                )
            })?;
        if !permitted {
            return Err(RunError::operation(
                "PROVIDER_SUSPENDED",
                "Provider route is not operational",
            ));
        }
        Ok(())
    }
}

#[async_trait]
impl ChatModel for FencedChatModel {
    fn capabilities(&self) -> BTreeSet<ModelCapability> {
        self.inner.capabilities()
    }
    fn request_capabilities(&self) -> BTreeSet<ModelRequestCapability> {
        self.inner.request_capabilities()
    }
    fn validate_parameters(&self, parameters: &Value) -> Result<(), CompileError> {
        self.inner.validate_parameters(parameters)
    }
    fn max_accumulated_text_bytes(&self) -> usize {
        self.inner.max_accumulated_text_bytes()
    }
    fn request_body_within_limit(&self, request: &ChatRequest, max_bytes: usize) -> bool {
        self.inner.request_body_within_limit(request, max_bytes)
    }
    fn request_body_within_limit_for_mode(
        &self,
        request: &ChatRequest,
        mode: ChatRequestMode,
        max_bytes: usize,
    ) -> bool {
        self.inner
            .request_body_within_limit_for_mode(request, mode, max_bytes)
    }
    async fn chat(&self, request: ChatRequest) -> Result<ChatResponse, RunError> {
        self.check_fence().await?;
        self.inner.chat(request).await
    }
    async fn stream_chat_events(&self, request: ChatRequest) -> Result<ChatEventStream, RunError> {
        self.check_fence().await?;
        self.inner.stream_chat_events(request).await
    }
    async fn stream_chat(&self, request: ChatRequest) -> Result<ChatStream, RunError> {
        self.check_fence().await?;
        self.inner.stream_chat(request).await
    }
}

type FencedProviderModelRegistration = ProviderModelRegistration<FencedChatModel>;

struct BuiltProviderRevision {
    registrations: Vec<FencedProviderModelRegistration>,
    legacy_registrations: Vec<FencedProviderModelRegistration>,
    legacy_binding_hashes: Vec<String>,
    credential_value_hash: Option<String>,
}

async fn build_provider_revision_with_legacy(
    repository: Arc<dyn ProviderManagementDurableRepository>,
    provider: &ManagedProvider,
    revision: &ProviderRevision,
    config: &ProviderManagementRuntimeConfig,
) -> Result<BuiltProviderRevision, CompileError> {
    let mut built = build_provider_revision(Arc::clone(&repository), provider, revision, config)?;
    let aliases = repository
        .load_provider_legacy_model_bindings(&revision.revision_id)
        .await
        .map_err(|_| {
            CompileError::new(
                "PROVIDER_LEGACY_BINDING_UNAVAILABLE",
                "legacy Provider binding evidence is unavailable",
            )
        })?;
    for alias in aliases {
        if alias.provider_id != provider.provider_id || alias.revision_id != revision.revision_id {
            return Err(CompileError::new(
                "PROVIDER_LEGACY_BINDING_INVALID",
                "legacy Provider binding identity is inconsistent",
            ));
        }
        let current = built
            .registrations
            .iter()
            .find(|(selector, _, _, _, _)| selector.id() == alias.model_id)
            .ok_or_else(|| {
                CompileError::new(
                    "PROVIDER_LEGACY_BINDING_INVALID",
                    "legacy Provider model is absent from its Revision",
                )
            })?;
        let identity = ModelDeploymentIdentity::new(
            current.1.worker_version().to_owned(),
            alias.legacy_binding_evidence,
        )?;
        if identity.binding_hash() != alias.legacy_binding_hash {
            return Err(CompileError::new(
                "PROVIDER_LEGACY_BINDING_INVALID",
                "legacy Provider binding hash does not match its immutable evidence",
            ));
        }
        built.legacy_binding_hashes.push(alias.legacy_binding_hash);
        built.legacy_registrations.push((
            current.0.clone(),
            identity,
            current.2.clone(),
            current.3.clone(),
            current.4.clone(),
        ));
    }
    built.legacy_binding_hashes.sort();
    Ok(built)
}

fn build_provider_revision(
    repository: Arc<dyn ProviderManagementDurableRepository>,
    provider: &ManagedProvider,
    revision: &ProviderRevision,
    config: &ProviderManagementRuntimeConfig,
) -> Result<BuiltProviderRevision, CompileError> {
    let document: RuntimeRevision =
        serde_json::from_value(revision.document.clone()).map_err(|_| {
            CompileError::new(
                "PROVIDER_REVISION_INVALID",
                "Provider Revision document is invalid",
            )
        })?;
    if document.provider_id != provider.provider_id
        || document.adapter.adapter_type != "open_ai_compatible"
        || document.adapter.version != insight_resources::openai_chat::OPENAI_CHAT_ADAPTER_VERSION
        || document.adapter.worker_version
            != insight_resources::openai_chat::OPENAI_CHAT_WORKER_VERSION
        || document.adapter.manifest_digest != managed_openai_adapter_manifest_digest()
        || document.models.is_empty()
    {
        return Err(CompileError::new(
            "PROVIDER_REVISION_INVALID",
            "Provider Revision identity is invalid",
        ));
    }
    let (credential_environment_variable, credential_value) =
        match document.credential.credential_type.as_str() {
            "none" if document.credential.reference.is_none() => (None, None),
            "bearer" | "api_key" => {
                let reference = document.credential.reference.as_deref().ok_or_else(|| {
                    CompileError::new(
                        "PROVIDER_REVISION_INVALID",
                        "Provider credential reference is missing",
                    )
                })?;
                if document.credential.reference_hash.as_deref()
                    != Some(sha256_string(reference).as_str())
                {
                    return Err(CompileError::new(
                        "PROVIDER_REVISION_INVALID",
                        "Provider credential reference hash is invalid",
                    ));
                }
                let (name, value) = environment_secret(reference, config).ok_or_else(|| {
                    CompileError::new(
                        "PROVIDER_SECRET_REFERENCE_DENIED",
                        "Provider credential reference is unsupported",
                    )
                })?;
                (Some(name.to_owned()), value)
            }
            _ => {
                return Err(CompileError::new(
                    "PROVIDER_REVISION_INVALID",
                    "Provider credential type is invalid",
                ))
            }
        };
    let credential_value_hash = credential_value.as_deref().map(sha256_string);
    let mut registrations = Vec::with_capacity(document.models.len());
    for model in document.models {
        let selector = ModelSelector::new(provider.provider_id.clone(), model.id.clone())?;
        let mut capabilities = BTreeSet::new();
        if model.input.iter().any(|input| input == "image") {
            capabilities.insert(ModelCapability::Vision);
        }
        if model
            .capabilities
            .iter()
            .any(|capability| capability == "structured_output")
        {
            capabilities.insert(ModelCapability::JsonObjectOutput);
            capabilities.insert(ModelCapability::JsonSchemaOutput);
        }
        let transport = transport_policy(config, &document.endpoint);
        let inner = OpenAiChatModel::new_with_limits_and_transport_policy(
            credential_value.clone(),
            document.endpoint.clone(),
            model.id.clone(),
            capabilities,
            Duration::from_millis(document.transport.connect_timeout_ms),
            Duration::from_millis(document.transport.request_timeout_ms),
            OpenAiChatLimits::default(),
            transport,
        )?;
        let deployment = ModelDeploymentIdentity::new(
            document.adapter.worker_version.clone(),
            json!({"provider_id":provider.provider_id,"provider_revision_id":revision.revision_id,
            "provider_revision_hash":revision.revision_hash,"adapter_type":document.adapter.adapter_type,
                "adapter_version":document.adapter.version,"worker_version":document.adapter.worker_version,
                "adapter_manifest_digest":document.adapter.manifest_digest,
                "model_id":model.id,"input":model.input,
                "capabilities":model.capabilities,"provenance":model.provenance,
                "policy_fingerprint":document.policy_fingerprint,"source":document.source}),
        )?;
        registrations.push((
            selector,
            deployment,
            credential_environment_variable.clone(),
            credential_value.clone(),
            FencedChatModel {
                inner: Arc::new(inner),
                fence: Arc::new(DurableProviderExecutionFence {
                    repository: Arc::clone(&repository),
                }),
                provider_id: provider.provider_id.clone(),
                revision_id: revision.revision_id.clone(),
                suspension_fence: provider.suspension_fence,
            },
        ));
    }
    Ok(BuiltProviderRevision {
        registrations,
        legacy_registrations: Vec::new(),
        legacy_binding_hashes: Vec::new(),
        credential_value_hash,
    })
}

fn sha256_string(value: &str) -> String {
    let mut result = String::from("sha256:");
    for byte in Sha256::digest(value.as_bytes()) {
        use std::fmt::Write as _;
        let _ = write!(&mut result, "{byte:02x}");
    }
    result
}

#[derive(Debug)]
struct InstalledRevision {
    revision_id: String,
    suspension_fence: u64,
    credential_value_hash: Option<String>,
    legacy_binding_hashes: Vec<String>,
}

struct ArchivedRevision {
    suspension_fence: u64,
    credential_value_hash: Option<String>,
    legacy_binding_hashes: Vec<String>,
}

type ArchivedProviderRevisions = BTreeMap<(String, String), ArchivedRevision>;

async fn reconcile_active_revisions(
    repository: &Arc<dyn ProviderManagementDurableRepository>,
    models: &ModelRegistry,
    config: &ProviderManagementRuntimeConfig,
    installed: &mut BTreeMap<String, InstalledRevision>,
    archived: &mut ArchivedProviderRevisions,
) -> Result<(), &'static str> {
    let archive = repository
        .load_provider_revision_archive()
        .await
        .map_err(|_| "repository")?;
    let archive_ids = archive
        .iter()
        .map(|(provider, revision)| (provider.provider_id.clone(), revision.revision_id.clone()))
        .collect::<BTreeSet<_>>();
    archived.retain(|identity, _| archive_ids.contains(identity));
    for (provider, revision) in archive {
        let credential_hash =
            revision_credential_value_hash(&revision, config).map_err(|_| "projection")?;
        let identity = (provider.provider_id.clone(), revision.revision_id.clone());
        let built = match build_provider_revision_with_legacy(
            Arc::clone(repository),
            &provider,
            &revision,
            config,
        )
        .await
        {
            Ok(built) => built,
            Err(_) => {
                let _ = models.withdraw_provider_all(&provider.provider_id);
                installed.remove(&provider.provider_id);
                archived.retain(|(provider_id, _), _| provider_id != &provider.provider_id);
                tracing::warn!(code="PROVIDER_REVISION_ARCHIVE_INVALID", provider_id=%provider.provider_id, revision_id=%revision.revision_id, "Provider Revision archive projection was rejected");
                return Err("projection");
            }
        };
        if archived.get(&identity).is_some_and(|current| {
            current.suspension_fence == provider.suspension_fence
                && current.credential_value_hash == credential_hash
                && current.legacy_binding_hashes == built.legacy_binding_hashes
        }) {
            continue;
        }
        models
            .archive_provider_models(&provider.provider_id, built.registrations)
            .map_err(|_| "registry")?;
        models
            .archive_provider_models(&provider.provider_id, built.legacy_registrations)
            .map_err(|_| "registry")?;
        archived.insert(
            identity,
            ArchivedRevision {
                suspension_fence: provider.suspension_fence,
                credential_value_hash: built.credential_value_hash,
                legacy_binding_hashes: built.legacy_binding_hashes,
            },
        );
    }
    let active = repository
        .load_active_provider_revisions()
        .await
        .map_err(|_| "repository")?;
    let active_ids = active
        .iter()
        .map(|(provider, _)| provider.provider_id.clone())
        .collect::<BTreeSet<_>>();
    for provider_id in installed
        .keys()
        .filter(|id| !active_ids.contains(*id))
        .cloned()
        .collect::<Vec<_>>()
    {
        models
            .withdraw_provider(&provider_id)
            .map_err(|_| "registry")?;
        installed.remove(&provider_id);
    }
    for (provider, revision) in active {
        let built = match build_provider_revision_with_legacy(
            Arc::clone(repository),
            &provider,
            &revision,
            config,
        )
        .await
        {
            Ok(built) => built,
            Err(_) => {
                let _ = models.withdraw_provider_all(&provider.provider_id);
                installed.remove(&provider.provider_id);
                tracing::warn!(code="PROVIDER_REVISION_PROJECTION_INVALID", provider_id=%provider.provider_id, revision_id=%revision.revision_id, "Provider Revision projection was rejected");
                return Err("projection");
            }
        };
        if installed.get(&provider.provider_id).is_some_and(|current| {
            current.revision_id == revision.revision_id
                && current.suspension_fence == provider.suspension_fence
                && current.credential_value_hash == built.credential_value_hash
                && current.legacy_binding_hashes == built.legacy_binding_hashes
        }) {
            continue;
        }
        models
            .replace_provider_models(&provider.provider_id, built.registrations)
            .map_err(|_| "registry")?;
        models
            .archive_provider_models(&provider.provider_id, built.legacy_registrations)
            .map_err(|_| "registry")?;
        installed.insert(
            provider.provider_id,
            InstalledRevision {
                revision_id: revision.revision_id,
                suspension_fence: provider.suspension_fence,
                credential_value_hash: built.credential_value_hash,
                legacy_binding_hashes: built.legacy_binding_hashes,
            },
        );
    }
    Ok(())
}

fn revision_credential_value_hash(
    revision: &ProviderRevision,
    config: &ProviderManagementRuntimeConfig,
) -> Result<Option<String>, ()> {
    let document: RuntimeRevision =
        serde_json::from_value(revision.document.clone()).map_err(|_| ())?;
    Ok(document
        .credential
        .reference
        .as_deref()
        .and_then(|reference| environment_secret(reference, config))
        .and_then(|(_, value)| value)
        .as_deref()
        .map(sha256_string))
}

#[derive(Debug, Clone)]
pub struct ManagedProviderRevisionReadiness {
    config: ProviderManagementRuntimeConfig,
}

impl ManagedProviderRevisionReadiness {
    pub fn new(config: ProviderManagementRuntimeConfig) -> Self {
        Self { config }
    }
}

#[async_trait]
impl ProviderRevisionReadiness for ManagedProviderRevisionReadiness {
    async fn probe(&self, revision: &ProviderRevision) -> Result<String, ()> {
        let document: RuntimeRevision =
            serde_json::from_value(revision.document.clone()).map_err(|_| ())?;
        if document.provider_id != revision.provider_id
            || document.adapter.adapter_type != "open_ai_compatible"
            || document.adapter.version
                != insight_resources::openai_chat::OPENAI_CHAT_ADAPTER_VERSION
            || document.adapter.worker_version
                != insight_resources::openai_chat::OPENAI_CHAT_WORKER_VERSION
            || document.adapter.manifest_digest != managed_openai_adapter_manifest_digest()
            || document.models.is_empty()
            || document.transport.tls != "required"
            || document.transport.redirects != "deny"
            || (transport_policy(&self.config, &document.endpoint)
                == OpenAiTransportPolicy::HttpsOnly
                && !document.endpoint.starts_with("https://"))
        {
            return Err(());
        }
        match (
            document.credential.credential_type.as_str(),
            document.credential.reference.as_deref(),
        ) {
            ("none", None) => {}
            ("bearer" | "api_key", Some(reference)) => {
                let (_, value) = environment_secret(reference, &self.config).ok_or(())?;
                if value.is_none_or(|value| value.trim().is_empty()) {
                    return Err(());
                }
            }
            _ => return Err(()),
        }
        canonical_hash(
            &json!({"revision_id":revision.revision_id,"revision_hash":revision.revision_hash,
                    "adapter_version":document.adapter.version,"worker_version":document.adapter.worker_version,
                    "adapter_manifest_digest":document.adapter.manifest_digest}),
        )
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

    use futures::stream;
    use insight_resources::models::ChatToolChoice;

    use super::*;

    #[derive(Debug, Default)]
    struct CountingModel {
        calls: AtomicU64,
    }

    #[async_trait]
    impl ChatModel for CountingModel {
        fn capabilities(&self) -> BTreeSet<ModelCapability> {
            BTreeSet::new()
        }

        fn validate_parameters(&self, _parameters: &Value) -> Result<(), CompileError> {
            Ok(())
        }

        async fn stream_chat(&self, _request: ChatRequest) -> Result<ChatStream, RunError> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            Ok(Box::pin(stream::empty()))
        }
    }

    struct AtomicProviderFence {
        permitted: AtomicBool,
    }

    #[async_trait]
    impl ProviderExecutionFence for AtomicProviderFence {
        async fn permits(&self, _provider_id: &str, _suspension_fence: u64) -> Result<bool, ()> {
            Ok(self.permitted.load(Ordering::SeqCst))
        }
    }

    fn request() -> ChatRequest {
        ChatRequest {
            messages: Vec::new(),
            parameters: json!({}),
            response_format: None,
            tools: Vec::new(),
            tool_choice: ChatToolChoice::Auto,
        }
    }

    fn runtime_config() -> ProviderManagementRuntimeConfig {
        ProviderManagementRuntimeConfig {
            enabled: true,
            workers: 1,
            poll_interval: Duration::from_millis(10),
            lease_duration: Duration::from_secs(1),
            retention: chrono::Duration::hours(1),
            max_response_bytes: 64 * 1024,
            allow_loopback_development: false,
            allowed_secret_names: BTreeSet::new(),
        }
    }

    fn revision(document: Value) -> ProviderRevision {
        ProviderRevision {
            revision_id: "prev_exact_adapter".to_owned(),
            provider_id: "provider-exact".to_owned(),
            revision_number: 1,
            source_draft_version: 1,
            validation_id: "pval_exact".to_owned(),
            discovery_id: None,
            connection_test_id: None,
            revision_hash: sha256_string("revision"),
            document,
            created_at: Utc::now(),
            created_by: "operator".to_owned(),
        }
    }

    fn valid_revision_document() -> Value {
        json!({
            "provider_id":"provider-exact",
            "adapter":{
                "type":"open_ai_compatible",
                "version":insight_resources::openai_chat::OPENAI_CHAT_ADAPTER_VERSION,
                "worker_version":insight_resources::openai_chat::OPENAI_CHAT_WORKER_VERSION,
                "manifest_digest":managed_openai_adapter_manifest_digest()
            },
            "endpoint":"https://models.example.test/v1",
            "credential":{"type":"none","reference":null,"reference_hash":null},
            "transport":{
                "tls":"required","redirects":"deny",
                "connect_timeout_ms":1000,"request_timeout_ms":5000
            },
            "models":[{
                "id":"model-a","input":["text"],"capabilities":[],
                "provenance":{"type":"manual"}
            }],
            "policy_fingerprint":sha256_string("policy"),
            "source":{"draft_version":1}
        })
    }

    #[tokio::test]
    async fn readiness_requires_exact_adapter_manifest_and_allowlisted_available_secret() {
        let readiness = ManagedProviderRevisionReadiness::new(runtime_config());
        assert!(readiness
            .probe(&revision(valid_revision_document()))
            .await
            .is_ok());

        let mut wrong_version = valid_revision_document();
        wrong_version["adapter"]["version"] = json!("mixed-runtime-version");
        assert!(readiness.probe(&revision(wrong_version)).await.is_err());

        let mut wrong_manifest = valid_revision_document();
        wrong_manifest["adapter"]["manifest_digest"] = json!(sha256_string("mixed-manifest"));
        assert!(readiness.probe(&revision(wrong_manifest)).await.is_err());

        let mut wrong_worker = valid_revision_document();
        wrong_worker["adapter"]["worker_version"] = json!("mixed-worker-version");
        assert!(readiness.probe(&revision(wrong_worker)).await.is_err());

        let mut denied_secret = valid_revision_document();
        denied_secret["credential"] = json!({
            "type":"bearer",
            "reference":"secret://environment/UNLISTED_PROVIDER_SECRET",
            "reference_hash":sha256_string("secret://environment/UNLISTED_PROVIDER_SECRET")
        });
        assert!(readiness.probe(&revision(denied_secret)).await.is_err());
    }

    #[tokio::test]
    async fn suspension_after_admission_rejects_before_provider_leaf_dispatch() {
        let inner = Arc::new(CountingModel::default());
        let fence = Arc::new(AtomicProviderFence {
            permitted: AtomicBool::new(true),
        });
        let model = FencedChatModel {
            inner: inner.clone(),
            fence: fence.clone(),
            provider_id: "provider-race".to_owned(),
            revision_id: "revision-race".to_owned(),
            suspension_fence: 7,
        };

        // Admission observed generation 7; suspension wins before the leaf
        // starts. The mutable execution fence must prevent any adapter call.
        fence.permitted.store(false, Ordering::SeqCst);
        let error = model.chat(request()).await.unwrap_err();
        assert_eq!(error.code(), "PROVIDER_SUSPENDED");
        assert_eq!(inner.calls.load(Ordering::SeqCst), 0);
    }
}
