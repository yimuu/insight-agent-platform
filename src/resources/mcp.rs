use std::{
    collections::{BTreeMap, BTreeSet},
    fmt::Write as _,
    sync::Arc,
    time::Duration,
};

use async_trait::async_trait;
use base64::{engine::general_purpose::STANDARD as BASE64_STANDARD, Engine as _};
use chrono::{DateTime, Duration as ChronoDuration, Utc};
use insight_durable::{
    ClaimMcpOAuthRefreshCommand, ClaimMcpRemoteTasksCommand, CreateMcpInteractionCommand,
    CreateMcpRemoteTaskCommand, FinalizeMcpRemoteTaskCommand, McpInteraction,
    McpInteractionDurableRepository, McpInteractionId, McpInteractionOutcome,
    McpInteractionPrincipal, McpInteractionRequest, McpInteractionState, McpOAuthDurableRepository,
    McpProtectedSecret, McpRemoteTaskDurableRepository, McpRemoteTaskId, McpRemoteTaskStatus,
    McpSecretProtector, McpSecretPurpose, McpSecretScope, ObserveMcpRemoteTaskCommand,
    StoreMcpOAuthCredentialCommand, TransitionMcpInteractionCommand,
};
use insight_engine::{execution::RunError, worker::WorkerArtifactPayload};
use insight_mcp::{
    ClientCapabilities, ClientError, ClientInfo, ElicitAction, ElicitRequestParams, ElicitResult,
    GetTaskResult, HttpCredential, InputRequest, InputRequiredResult, InputResponse,
    LegacyCompatibilityTransport, ListPromptsResult, ListResourceTemplatesResult,
    ListResourcesResult, ListToolsResult, McpClient, McpClientLimits, McpNotificationObserver,
    McpServerBindingIdentity, McpToolBinding, McpTransport, McpTransportKind, MetaMap,
    NoopNotificationObserver, OAuthClient, OAuthClientRegistration, PrincipalScope, PromptCatalog,
    ProtocolLimits, ReadResourceResult, ResourceCatalog, ResourceContents, ResourceTemplateCatalog,
    StdioTransport, StdioTransportPolicy, StreamableHttpPolicy, StreamableHttpTransport,
    SubscriptionFilter, TransportError, TransportKind, MCP_LEGACY_PROTOCOL_VERSION,
    MCP_PROTOCOL_VERSION,
};
use insight_resources::actions::{
    ActionCapability, ActionRegistry, CancellationClass, EffectClass, IdempotencyClass,
    ToolPublicPolicy,
};
use insight_resources::retrievals::{
    Retrieval, RetrievalContext, RetrievalDescriptor, RetrievalExecutionResult, RetrievalRegistry,
};
use insight_runtime::{RuntimeReadinessProbe, ServiceError};
use ring::signature;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use tokio_util::sync::CancellationToken;

use crate::config::{
    McpApprovalConfig, McpCancellationConfig, McpClientAuthorizationConfig, McpClientConfig,
    McpClientTransportConfig, McpDescriptionSourceConfig, McpDiscoveryConfig, McpEffectConfig,
    McpIdempotencyConfig, McpInteractionConfig, McpToolImportConfig,
};

pub use insight_resources::mcp::{
    McpApprovalPolicy, McpDescriptionPolicy, McpImportedTool, McpInteractionContext,
    McpInteractionHandler, McpInteractionHandlerError, McpInteractionResolution, McpProviderError,
    McpRecoveredRemoteTask, McpRemoteTaskHandle, McpRemoteTaskHandler, McpRemoteTaskHandlerError,
    McpRemoteTaskLocalTerminal, McpRemoteTaskObservation, McpRemoteTaskPollAuthority,
    McpRuntimeClientResolver, McpRuntimeClientResolverError, McpToolImport, McpToolProvider,
};
pub use insight_resources::mcp_context::{
    McpCatalogInvalidation, McpCatalogInvalidationState, McpContextError, McpContextOutcome,
    McpContextProvider, McpImportedPrompt, McpImportedResource, McpImportedResourceDescriptor,
    McpPrincipalClientResolver, McpPrincipalClientResolverError, McpPromptImportPolicy,
    McpPromptSnapshot, McpResourceImportPolicy, McpResourceSnapshot,
};

#[derive(Debug, Clone)]
pub struct McpClientPublication {
    pub server_id: String,
    pub binding: McpServerBindingIdentity,
    pub capabilities: insight_mcp::ServerCapabilities,
    pub server_info: insight_mcp::ServerInfo,
    pub required_scopes: BTreeSet<String>,
    pub discovery_fingerprint: String,
    pub tool_catalog_fingerprint: Option<String>,
    pub tool_bindings: Vec<McpToolBinding>,
    pub catalog_invalidation: Option<Arc<McpCatalogInvalidation>>,
}

#[derive(Clone)]
pub struct McpClientSubscription {
    pub server_id: String,
    pub context: Arc<McpContextProvider>,
    pub filter: SubscriptionFilter,
    pub invalidation: Arc<McpCatalogInvalidation>,
}

#[derive(Clone)]
pub struct McpLoadedProviders {
    pub publications: Vec<McpClientPublication>,
    pub contexts: BTreeMap<String, Arc<McpContextProvider>>,
    pub subscriptions: Vec<McpClientSubscription>,
    pub readiness: Arc<McpClientReadinessProbe>,
}

#[derive(Clone)]
enum RequiredMcpReadiness {
    Service {
        client: Arc<McpClient>,
        capabilities: Box<insight_mcp::ServerCapabilities>,
    },
    OAuthUser {
        client: OAuthClient,
        resource: String,
        registration: OAuthClientRegistration,
    },
}

#[derive(Clone, Default)]
pub struct McpClientReadinessProbe {
    required: Vec<RequiredMcpReadiness>,
}

#[async_trait]
impl RuntimeReadinessProbe for McpClientReadinessProbe {
    async fn check_readiness(&self, timeout: Duration) -> Result<(), ServiceError> {
        let cancellation = CancellationToken::new();
        let check = async {
            for required in &self.required {
                match required {
                    RequiredMcpReadiness::Service {
                        client,
                        capabilities,
                    } => {
                        client
                            .discover(&cancellation, &NoopNotificationObserver)
                            .await
                            .map_err(|_| {
                                ServiceError::new(
                                    "MCP_BINDING_UNAVAILABLE",
                                    "a required MCP Server is unavailable",
                                )
                            })?;
                        // Modern discovery is a live request. The legacy
                        // adapter intentionally caches its initialize
                        // evidence, so exercise one non-deprecated primitive
                        // as the liveness probe.
                        if client.protocol_version() == MCP_LEGACY_PROTOCOL_VERSION {
                            if capabilities.tools.is_some() {
                                client
                                    .list_tools(&cancellation, &NoopNotificationObserver)
                                    .await
                                    .map_err(|_| {
                                        ServiceError::new(
                                            "MCP_BINDING_UNAVAILABLE",
                                            "a required legacy MCP Server is unavailable",
                                        )
                                    })?;
                            } else if capabilities.resources.is_some() {
                                client
                                    .list_resources(&cancellation, &NoopNotificationObserver)
                                    .await
                                    .map_err(|_| {
                                        ServiceError::new(
                                            "MCP_BINDING_UNAVAILABLE",
                                            "a required legacy MCP Server is unavailable",
                                        )
                                    })?;
                            } else if capabilities.prompts.is_some() {
                                client
                                    .list_prompts(&cancellation, &NoopNotificationObserver)
                                    .await
                                    .map_err(|_| {
                                        ServiceError::new(
                                            "MCP_BINDING_UNAVAILABLE",
                                            "a required legacy MCP Server is unavailable",
                                        )
                                    })?;
                            }
                        }
                    }
                    RequiredMcpReadiness::OAuthUser {
                        client,
                        resource,
                        registration,
                    } => {
                        // Publication discovery for a user-authorized binding
                        // is frozen in a signed manifest. A credential-free
                        // MCP request would correctly receive 401, so
                        // readiness instead validates the live OAuth metadata
                        // chain and the configured PKCE/scope contract.
                        let protected = client
                            .discover_protected_resource(resource)
                            .await
                            .map_err(|_| {
                                ServiceError::new(
                                    "MCP_OAUTH_METADATA_UNAVAILABLE",
                                    "required MCP OAuth metadata is unavailable",
                                )
                            })?;
                        let mut compatible_issuer = false;
                        for issuer in &protected.authorization_servers {
                            let authorization_server = client
                                .discover_authorization_server(issuer)
                                .await
                                .map_err(|_| {
                                    ServiceError::new(
                                        "MCP_OAUTH_METADATA_UNAVAILABLE",
                                        "required MCP OAuth metadata is unavailable",
                                    )
                                })?;
                            if client
                                .begin_authorization(
                                    &protected,
                                    &authorization_server,
                                    registration,
                                )
                                .is_ok()
                            {
                                compatible_issuer = true;
                            }
                        }
                        if !compatible_issuer {
                            return Err(ServiceError::new(
                                "MCP_OAUTH_METADATA_UNAVAILABLE",
                                "required MCP OAuth metadata is incompatible",
                            ));
                        }
                    }
                }
            }
            Ok(())
        };
        match tokio::time::timeout(timeout, check).await {
            Ok(result) => result,
            Err(_) => {
                cancellation.cancel();
                Err(ServiceError::new(
                    "MCP_BINDING_UNAVAILABLE",
                    "required MCP readiness probe timed out",
                ))
            }
        }
    }
}

#[derive(Clone)]
struct McpResourceRetrieval {
    id: String,
    imported: McpImportedResource,
    context: Arc<McpContextProvider>,
    principal_repository: Option<Arc<dyn McpInteractionDurableRepository>>,
    interaction_handler: Option<Arc<dyn McpInteractionHandler>>,
}

impl McpResourceRetrieval {
    fn new(
        imported: McpImportedResource,
        context: Arc<McpContextProvider>,
        principal_repository: Option<Arc<dyn McpInteractionDurableRepository>>,
        interaction_handler: Option<Arc<dyn McpInteractionHandler>>,
    ) -> Self {
        let digest = Sha256::digest(imported.binding.remote_uri.as_bytes());
        let suffix = digest[..8]
            .iter()
            .map(|byte| format!("{byte:02x}"))
            .collect::<String>();
        Self {
            id: format!(
                "mcp.{}.resource_{}",
                imported.binding.server.server_id, suffix
            ),
            imported,
            context,
            principal_repository,
            interaction_handler,
        }
    }

    async fn run_principal(
        &self,
        run_id: &str,
    ) -> Result<Option<McpInteractionPrincipal>, RunError> {
        if !matches!(
            self.imported.binding.server.principal_scope,
            PrincipalScope::User(_)
        ) {
            return Ok(None);
        }
        self.principal_repository
            .as_ref()
            .ok_or_else(|| {
                RunError::infrastructure(
                    "MCP_RESOURCE_PRINCIPAL_UNAVAILABLE",
                    "MCP Resource principal authority is unavailable",
                )
            })?
            .load_mcp_run_principal(run_id)
            .await
            .map_err(|_| {
                RunError::infrastructure(
                    "MCP_RESOURCE_PRINCIPAL_UNAVAILABLE",
                    "MCP Resource principal authority is unavailable",
                )
            })?
            .map(Some)
            .ok_or_else(|| {
                RunError::operation(
                    "MCP_RESOURCE_AUTHORIZATION_REQUIRED",
                    "MCP Resource authorization is required",
                )
            })
    }

    async fn read(
        &self,
        uri: &str,
        principal: Option<&McpInteractionPrincipal>,
        continuation: Option<(BTreeMap<String, InputResponse>, Option<String>)>,
        cancellation: &CancellationToken,
    ) -> Result<McpContextOutcome<McpResourceSnapshot>, RunError> {
        let result = match (principal, continuation) {
            (Some(principal), Some((responses, state))) => {
                self.context
                    .read_resource_for_principal_with_inputs(
                        principal.tenant_id(),
                        principal.user_id(),
                        uri,
                        responses,
                        state,
                        cancellation,
                        &NoopNotificationObserver,
                    )
                    .await
            }
            (Some(principal), None) => {
                self.context
                    .read_resource_for_principal(
                        principal.tenant_id(),
                        principal.user_id(),
                        uri,
                        cancellation,
                        &NoopNotificationObserver,
                    )
                    .await
            }
            (None, Some((responses, state))) => {
                self.context
                    .read_resource_with_inputs(
                        uri,
                        responses,
                        state,
                        cancellation,
                        &NoopNotificationObserver,
                    )
                    .await
            }
            (None, None) => {
                self.context
                    .read_resource(uri, cancellation, &NoopNotificationObserver)
                    .await
            }
        };
        result.map_err(|error| match error {
            McpContextError::Unauthorized => RunError::operation(
                "MCP_RESOURCE_AUTHORIZATION_REQUIRED",
                "MCP Resource authorization is required",
            ),
            McpContextError::Content | McpContextError::Mime => RunError::operation(
                "MCP_RESOURCE_CONTENT_INVALID",
                "MCP Resource content failed its frozen contract",
            ),
            _ => {
                RunError::infrastructure("MCP_RESOURCE_UNAVAILABLE", "MCP Resource is unavailable")
            }
        })
    }
}

#[async_trait]
impl Retrieval for McpResourceRetrieval {
    fn descriptor(&self) -> RetrievalDescriptor {
        RetrievalDescriptor {
            id: self.id.clone(),
            version: "0.0.0+mcp".to_owned(),
            input_schema: json!({
                "type": "object",
                "properties": {
                    "uri": {
                        "type": "string",
                        "minLength": 1,
                        "maxLength": 8192
                    }
                },
                "required": ["uri"],
                "additionalProperties": false
            }),
            output_schema: json!({
                "type": "object",
                "properties": {
                    "binding_hash": {"type": "string"},
                    "canonical_uri": {"type": "string"},
                    "content_hash": {"type": "string"},
                    "result": {}
                },
                "required": ["binding_hash", "canonical_uri", "content_hash", "result"],
                "additionalProperties": false
            }),
            query_field: "uri".to_owned(),
            effect: insight_resources::actions::EffectClass::ReadOnly,
            idempotency: insight_resources::actions::IdempotencyClass::Idempotent,
            cancellation: insight_resources::actions::CancellationClass::Cooperative,
            required_capabilities: BTreeSet::new(),
        }
    }

    async fn retrieve(
        &self,
        input: Value,
        context: RetrievalContext,
    ) -> Result<RetrievalExecutionResult, RunError> {
        let uri = input
            .get("uri")
            .and_then(Value::as_str)
            .ok_or_else(|| {
                RunError::operation("MCP_RESOURCE_INPUT_INVALID", "MCP Resource URI is invalid")
            })?
            .to_owned();
        let principal = self.run_principal(&context.run_id).await?;
        let cancellation = CancellationToken::new();
        let cancellation_on_stop = cancellation.clone();
        let control = context.control.clone();
        let stop_watcher = tokio::spawn(async move {
            control.stopped().await;
            cancellation_on_stop.cancel();
        });
        let mut interaction_context = McpInteractionContext {
            run_id: context.run_id.clone(),
            operation_id: context.operation_id.clone(),
            logical_request_key: context.idempotency_key.clone(),
            server_id: self.imported.binding.server.server_id.clone(),
            binding_hash: self.imported.binding.descriptor_hash.clone(),
            generation: 1,
            remaining: context.control.remaining(),
            operation_permit: context.operation_permit().cloned(),
        };
        let mut continuation = None;
        let mut open_interactions = Vec::new();
        let execution = async {
            loop {
                let outcome = self
                    .read(&uri, principal.as_ref(), continuation.take(), &cancellation)
                    .await?;
                match outcome {
                    McpContextOutcome::Complete(snapshot) => break Ok(snapshot),
                    McpContextOutcome::InputRequired(required) => {
                        let handler = self.interaction_handler.as_ref().ok_or_else(|| {
                            RunError::operation(
                                "MCP_RESOURCE_INTERACTION_REQUIRED",
                                "MCP Resource requires an unavailable interaction",
                            )
                        })?;
                        if !open_interactions.is_empty() {
                            handler
                                .complete(&interaction_context, &open_interactions, true)
                                .await
                                .map_err(map_resource_interaction_error)?;
                            open_interactions.clear();
                            interaction_context.generation =
                                interaction_context.generation.saturating_add(1);
                        }
                        let resolution = handler
                            .resolve_input(&interaction_context, required, &cancellation)
                            .await
                            .map_err(map_resource_interaction_error)?;
                        open_interactions = resolution.interaction_ids;
                        continuation = Some((resolution.input_responses, resolution.request_state));
                    }
                }
            }
        }
        .await;
        let result = match execution {
            Ok(result) => result,
            Err(error) => {
                if let Some(handler) = &self.interaction_handler {
                    if !open_interactions.is_empty() {
                        let _ = handler
                            .complete(&interaction_context, &open_interactions, false)
                            .await;
                    }
                }
                stop_watcher.abort();
                return Err(error);
            }
        };
        if let Some(handler) = &self.interaction_handler {
            if !open_interactions.is_empty() {
                let completion = handler
                    .complete(&interaction_context, &open_interactions, true)
                    .await
                    .map_err(map_resource_interaction_error);
                if let Err(error) = completion {
                    stop_watcher.abort();
                    return Err(error);
                }
            }
        }
        stop_watcher.abort();
        let (projected_result, artifact_payloads) = externalize_resource_result(result.result)
            .map_err(|_| {
                RunError::operation(
                    "MCP_RESOURCE_CONTENT_INVALID",
                    "MCP Resource binary content could not be externalized",
                )
            })?;
        Ok(RetrievalExecutionResult::with_artifact_payloads(
            json!({
                "binding_hash": result.binding.descriptor_hash,
                "canonical_uri": result.canonical_uri,
                "content_hash": result.content_hash,
                "result": projected_result,
            }),
            None,
            artifact_payloads,
        ))
    }
}

fn externalize_resource_result(
    result: ReadResourceResult,
) -> Result<(Value, Vec<WorkerArtifactPayload>), ()> {
    let mut artifact_payloads = Vec::new();
    let mut contents = Vec::with_capacity(result.contents.len());
    for content in result.contents {
        match content {
            ResourceContents::Blob {
                uri,
                blob,
                mime_type,
                ..
            } => {
                let bytes = BASE64_STANDARD.decode(blob).map_err(|_| ())?;
                let payload =
                    WorkerArtifactPayload::new(bytes, mime_type.clone()).map_err(|_| ())?;
                let artifact = serde_json::to_value(payload.artifact()).map_err(|_| ())?;
                contents.push(json!({
                    "type": "artifact",
                    "artifact": artifact,
                    "source": {
                        "kind": "mcp_resource",
                        "uri": uri,
                        "mime_type": mime_type
                    }
                }));
                artifact_payloads.push(payload);
            }
            text @ ResourceContents::Text { .. } => {
                contents.push(serde_json::to_value(text).map_err(|_| ())?);
            }
        }
    }
    Ok((
        json!({
            "resultType": result.result_type,
            "contents": contents,
            "ttlMs": result.ttl_ms,
            "cacheScope": result.cache_scope,
            "_meta": result.metadata
        }),
        artifact_payloads,
    ))
}

fn map_resource_interaction_error(error: McpInteractionHandlerError) -> RunError {
    match error {
        McpInteractionHandlerError::Declined => RunError::operation(
            "MCP_RESOURCE_INTERACTION_DECLINED",
            "MCP Resource interaction was declined",
        ),
        McpInteractionHandlerError::Cancelled => RunError::operation(
            "MCP_RESOURCE_INTERACTION_CANCELLED",
            "MCP Resource interaction was cancelled",
        ),
        McpInteractionHandlerError::Expired => RunError::operation(
            "MCP_RESOURCE_INTERACTION_EXPIRED",
            "MCP Resource interaction expired",
        ),
        McpInteractionHandlerError::Unavailable | McpInteractionHandlerError::Invalid => {
            RunError::infrastructure(
                "MCP_RESOURCE_INTERACTION_UNAVAILABLE",
                "MCP Resource interaction is unavailable",
            )
        }
    }
}

#[derive(Clone)]
pub struct McpOAuthRuntimeAuthority {
    interaction_repository: Arc<dyn McpInteractionDurableRepository>,
    oauth_repository: Arc<dyn McpOAuthDurableRepository>,
    protector: Arc<dyn McpSecretProtector>,
    refresh_owner: String,
}

impl McpOAuthRuntimeAuthority {
    pub fn new(
        interaction_repository: Arc<dyn McpInteractionDurableRepository>,
        oauth_repository: Arc<dyn McpOAuthDurableRepository>,
        protector: Arc<dyn McpSecretProtector>,
    ) -> Self {
        Self {
            interaction_repository,
            oauth_repository,
            protector,
            refresh_owner: format!("mcp-oauth-refresh-{}", uuid::Uuid::new_v4().simple()),
        }
    }
}

#[derive(Clone)]
pub struct McpRemoteTaskRuntimeAuthority {
    interaction_repository: Arc<dyn McpInteractionDurableRepository>,
    task_repository: Arc<dyn McpRemoteTaskDurableRepository>,
    protector: Arc<dyn McpSecretProtector>,
    owner: String,
}

impl McpRemoteTaskRuntimeAuthority {
    pub fn new(
        interaction_repository: Arc<dyn McpInteractionDurableRepository>,
        task_repository: Arc<dyn McpRemoteTaskDurableRepository>,
        protector: Arc<dyn McpSecretProtector>,
    ) -> Self {
        Self {
            interaction_repository,
            task_repository,
            protector,
            owner: format!("mcp-task-worker-{}", uuid::Uuid::new_v4().simple()),
        }
    }

    async fn principal(
        &self,
        run_id: &str,
    ) -> Result<McpInteractionPrincipal, McpRemoteTaskHandlerError> {
        self.interaction_repository
            .load_mcp_run_principal(run_id)
            .await
            .map_err(|_| McpRemoteTaskHandlerError::Unavailable)?
            .ok_or(McpRemoteTaskHandlerError::Unavailable)
    }

    fn open_secret(
        &self,
        task: &insight_durable::McpRemoteTask,
        purpose: McpSecretPurpose,
        ciphertext: insight_durable::McpSecretCiphertext,
        content_hash: String,
    ) -> Result<Vec<u8>, McpRemoteTaskHandlerError> {
        let scope = McpSecretScope::new(
            task.principal(),
            task.server_id(),
            task.task_id().as_str(),
            purpose,
        )
        .map_err(|_| McpRemoteTaskHandlerError::Invalid)?;
        self.protector
            .open(
                &scope,
                &McpProtectedSecret::new(ciphertext, content_hash)
                    .map_err(|_| McpRemoteTaskHandlerError::Invalid)?,
            )
            .map_err(|_| McpRemoteTaskHandlerError::Unavailable)
    }
}

#[async_trait]
impl McpRemoteTaskHandler for McpRemoteTaskRuntimeAuthority {
    async fn recover(
        &self,
        context: &McpInteractionContext,
        binding: &McpToolBinding,
    ) -> Result<Option<McpRecoveredRemoteTask>, McpRemoteTaskHandlerError> {
        let local_id = remote_task_local_id(context, binding)?;
        let task_id = McpRemoteTaskId::new(local_id.clone())
            .map_err(|_| McpRemoteTaskHandlerError::Invalid)?;
        let Some(task) = self
            .task_repository
            .load_mcp_remote_task(&task_id)
            .await
            .map_err(|_| McpRemoteTaskHandlerError::Unavailable)?
        else {
            return Ok(None);
        };
        if task.run_id() != context.run_id
            || task.operation_id() != context.operation_id
            || task.logical_request_key() != context.logical_request_key
            || task.server_id() != context.server_id
            || task.binding_hash() != binding.descriptor_hash
            || task.protocol_version() != binding.server.protocol_version
            || task.capability_id() != insight_mcp::MCP_TASKS_EXTENSION_ID
        {
            return Err(McpRemoteTaskHandlerError::Conflict);
        }
        let secret = self
            .task_repository
            .load_mcp_remote_task_secret(&task_id)
            .await
            .map_err(|_| McpRemoteTaskHandlerError::Unavailable)?
            .ok_or(McpRemoteTaskHandlerError::Conflict)?;
        let remote_task_id = String::from_utf8(self.open_secret(
            &task,
            McpSecretPurpose::RemoteTaskId,
            secret.remote_task_id,
            secret.remote_task_id_hash,
        )?)
        .map_err(|_| McpRemoteTaskHandlerError::Invalid)?;
        let ttl_ms = (task.ttl_deadline() - task.remote_created_at()).num_milliseconds();
        if ttl_ms <= 0 {
            return Err(McpRemoteTaskHandlerError::Invalid);
        }
        let status = match task.status() {
            McpRemoteTaskStatus::Working => insight_mcp::TaskStatus::Working,
            McpRemoteTaskStatus::InputRequired => insight_mcp::TaskStatus::InputRequired,
            McpRemoteTaskStatus::Completed => insight_mcp::TaskStatus::Completed,
            McpRemoteTaskStatus::Failed | McpRemoteTaskStatus::Expired => {
                insight_mcp::TaskStatus::Failed
            }
            McpRemoteTaskStatus::Cancelled => insight_mcp::TaskStatus::Cancelled,
        };
        let ttl_ms = u64::try_from(ttl_ms).map_err(|_| McpRemoteTaskHandlerError::Invalid)?;
        Ok(Some(McpRecoveredRemoteTask {
            handle: McpRemoteTaskHandle {
                local_task_id: local_id,
            },
            created: insight_mcp::CreateTaskResult {
                result_type: "task".to_owned(),
                task: insight_mcp::Task {
                    task_id: remote_task_id,
                    status,
                    status_message: Some("Recovered durable MCP task".to_owned()),
                    created_at: task.remote_created_at(),
                    last_updated_at: task.remote_updated_at(),
                    ttl_ms: Some(ttl_ms),
                    poll_interval_ms: Some(task.poll_interval_ms()),
                },
                metadata: None,
            },
        }))
    }

    async fn record_created(
        &self,
        context: &McpInteractionContext,
        binding: &McpToolBinding,
        created: &insight_mcp::CreateTaskResult,
    ) -> Result<McpRemoteTaskHandle, McpRemoteTaskHandlerError> {
        if created.task.status != insight_mcp::TaskStatus::Working {
            return Err(McpRemoteTaskHandlerError::Invalid);
        }
        let principal = self.principal(&context.run_id).await?;
        let local_id = remote_task_local_id(context, binding)?;
        let task_id = McpRemoteTaskId::new(local_id.clone())
            .map_err(|_| McpRemoteTaskHandlerError::Invalid)?;
        let remote_scope = McpSecretScope::new(
            &principal,
            &context.server_id,
            &local_id,
            McpSecretPurpose::RemoteTaskId,
        )
        .map_err(|_| McpRemoteTaskHandlerError::Invalid)?;
        let remote_secret = self
            .protector
            .seal(&remote_scope, created.task.task_id.as_bytes())
            .map_err(|_| McpRemoteTaskHandlerError::Unavailable)?;
        let payload = serde_jcs::to_vec(created).map_err(|_| McpRemoteTaskHandlerError::Invalid)?;
        let payload_scope = McpSecretScope::new(
            &principal,
            &context.server_id,
            &local_id,
            McpSecretPurpose::RemoteTaskPayload,
        )
        .map_err(|_| McpRemoteTaskHandlerError::Invalid)?;
        let payload_secret = self
            .protector
            .seal(&payload_scope, &payload)
            .map_err(|_| McpRemoteTaskHandlerError::Unavailable)?;
        let now = Utc::now();
        let ttl = created
            .task
            .ttl_ms
            .map(Duration::from_millis)
            .unwrap_or(context.remaining)
            .min(Duration::from_secs(30 * 24 * 60 * 60));
        let ttl_deadline = created.task.created_at
            + ChronoDuration::from_std(ttl).map_err(|_| McpRemoteTaskHandlerError::Invalid)?;
        let (remote_ciphertext, remote_hash) = remote_secret.into_parts();
        let (payload_ciphertext, payload_hash) = payload_secret.into_parts();
        let command = CreateMcpRemoteTaskCommand::new(
            task_id,
            principal,
            &context.run_id,
            &context.operation_id,
            &context.logical_request_key,
            &context.server_id,
            &binding.descriptor_hash,
            &binding.server.protocol_version,
            insight_mcp::MCP_TASKS_EXTENSION_ID,
            remote_ciphertext,
            remote_hash,
            payload_ciphertext,
            payload_hash,
            created.task.created_at,
            created.task.last_updated_at,
            ttl_deadline,
            created.task.poll_interval_ms.unwrap_or(1_000).max(100),
            now,
        )
        .map_err(|_| McpRemoteTaskHandlerError::Invalid)?;
        match self
            .task_repository
            .create_mcp_remote_task(command)
            .await
            .map_err(|_| McpRemoteTaskHandlerError::Unavailable)?
        {
            insight_engine::TransitionOutcome::Committed { .. } => {
                let gauge = match created.task.status {
                    insight_mcp::TaskStatus::Working => {
                        insight_mcp::McpOperationalGauge::RemoteTasksWorking
                    }
                    insight_mcp::TaskStatus::InputRequired => {
                        insight_mcp::McpOperationalGauge::RemoteTasksInputRequired
                    }
                    insight_mcp::TaskStatus::Completed
                    | insight_mcp::TaskStatus::Failed
                    | insight_mcp::TaskStatus::Cancelled => {
                        insight_mcp::McpOperationalGauge::RemoteTasksTerminal
                    }
                };
                insight_mcp::adjust_operational_gauge(&context.server_id, gauge, 1);
                insight_mcp::set_operational_gauge(
                    &context.server_id,
                    insight_mcp::McpOperationalGauge::OldestRemoteTaskAgeSeconds,
                    0,
                );
                Ok(McpRemoteTaskHandle {
                    local_task_id: local_id,
                })
            }
            insight_engine::TransitionOutcome::ExactReplay { .. } => Ok(McpRemoteTaskHandle {
                local_task_id: local_id,
            }),
            insight_engine::TransitionOutcome::StateConflict
            | insight_engine::TransitionOutcome::StaleLease => {
                Err(McpRemoteTaskHandlerError::Conflict)
            }
        }
    }

    async fn claim_poll(
        &self,
        handle: &McpRemoteTaskHandle,
    ) -> Result<McpRemoteTaskPollAuthority, McpRemoteTaskHandlerError> {
        let task_id = McpRemoteTaskId::new(handle.local_task_id.clone())
            .map_err(|_| McpRemoteTaskHandlerError::Invalid)?;
        let now = Utc::now();
        let claim = self
            .task_repository
            .claim_mcp_remote_task(
                &task_id,
                ClaimMcpRemoteTasksCommand::new(
                    self.owner.clone(),
                    now,
                    now + ChronoDuration::seconds(15),
                    1,
                )
                .map_err(|_| McpRemoteTaskHandlerError::Invalid)?,
            )
            .await
            .map_err(|_| McpRemoteTaskHandlerError::Unavailable)?
            .ok_or(McpRemoteTaskHandlerError::Busy)?;
        let remote_task_id = self.open_secret(
            &claim.task,
            McpSecretPurpose::RemoteTaskId,
            claim.secret.remote_task_id,
            claim.secret.remote_task_id_hash,
        )?;
        let remote_task_id =
            String::from_utf8(remote_task_id).map_err(|_| McpRemoteTaskHandlerError::Invalid)?;
        Ok(McpRemoteTaskPollAuthority {
            local_task_id: handle.local_task_id.clone(),
            owner: claim.owner,
            lease_epoch: claim.lease_epoch,
            expected_version: claim.task.version(),
            remote_task_id,
        })
    }

    async fn record_observation(
        &self,
        authority: &McpRemoteTaskPollAuthority,
        observation: &GetTaskResult,
    ) -> Result<McpRemoteTaskObservation, McpRemoteTaskHandlerError> {
        observation
            .validate()
            .map_err(|_| McpRemoteTaskHandlerError::Invalid)?;
        if observation.task.task_id != authority.remote_task_id {
            return Err(McpRemoteTaskHandlerError::Invalid);
        }
        let task_id = McpRemoteTaskId::new(authority.local_task_id.clone())
            .map_err(|_| McpRemoteTaskHandlerError::Invalid)?;
        let task = self
            .task_repository
            .load_mcp_remote_task(&task_id)
            .await
            .map_err(|_| McpRemoteTaskHandlerError::Unavailable)?
            .ok_or(McpRemoteTaskHandlerError::Conflict)?;
        let payload =
            serde_jcs::to_vec(observation).map_err(|_| McpRemoteTaskHandlerError::Invalid)?;
        let scope = McpSecretScope::new(
            task.principal(),
            task.server_id(),
            task.task_id().as_str(),
            McpSecretPurpose::RemoteTaskPayload,
        )
        .map_err(|_| McpRemoteTaskHandlerError::Invalid)?;
        let protected = self
            .protector
            .seal(&scope, &payload)
            .map_err(|_| McpRemoteTaskHandlerError::Unavailable)?;
        let now = Utc::now();
        let status = if now >= task.ttl_deadline() {
            McpRemoteTaskStatus::Expired
        } else {
            match observation.task.status {
                insight_mcp::TaskStatus::Working => McpRemoteTaskStatus::Working,
                insight_mcp::TaskStatus::InputRequired => McpRemoteTaskStatus::InputRequired,
                insight_mcp::TaskStatus::Completed => McpRemoteTaskStatus::Completed,
                insight_mcp::TaskStatus::Failed => McpRemoteTaskStatus::Failed,
                insight_mcp::TaskStatus::Cancelled => McpRemoteTaskStatus::Cancelled,
            }
        };
        let poll_interval = observation.task.poll_interval_ms.unwrap_or(1_000).max(100);
        let poll_delay = ChronoDuration::milliseconds(
            i64::try_from(poll_interval).map_err(|_| McpRemoteTaskHandlerError::Invalid)?,
        );
        let next_poll_at = (!status.is_terminal()).then(|| now + poll_delay);
        let terminal_receipt = status
            .is_terminal()
            .then(|| canonical_sha256(&(status, observation)))
            .transpose()
            .map_err(|_| McpRemoteTaskHandlerError::Invalid)?;
        let (payload, payload_hash) = protected.into_parts();
        let command = ObserveMcpRemoteTaskCommand::new(
            task_id,
            format!(
                "poll-{}-{}-{}",
                authority.local_task_id,
                observation.task.last_updated_at.timestamp_micros(),
                status_wire(status)
            ),
            &authority.owner,
            authority.lease_epoch,
            authority.expected_version,
            status,
            observation.task.last_updated_at,
            poll_interval,
            next_poll_at,
            payload,
            payload_hash,
            terminal_receipt,
            now,
        )
        .map_err(|_| McpRemoteTaskHandlerError::Invalid)?;
        match self
            .task_repository
            .observe_mcp_remote_task(command)
            .await
            .map_err(|_| McpRemoteTaskHandlerError::Unavailable)?
        {
            insight_engine::TransitionOutcome::Committed { result } => {
                if task.status() != result.status() {
                    insight_mcp::adjust_operational_gauge(
                        task.server_id(),
                        remote_task_operational_gauge(task.status()),
                        -1,
                    );
                    insight_mcp::adjust_operational_gauge(
                        task.server_id(),
                        remote_task_operational_gauge(result.status()),
                        1,
                    );
                }
                let age = Utc::now()
                    .signed_duration_since(task.created_at())
                    .num_seconds()
                    .max(0) as u64;
                insight_mcp::set_operational_gauge(
                    task.server_id(),
                    insight_mcp::McpOperationalGauge::OldestRemoteTaskAgeSeconds,
                    age,
                );
                Ok(McpRemoteTaskObservation {
                    result: observation.clone(),
                    local_version: result.version(),
                    local_terminal: match result.status() {
                        McpRemoteTaskStatus::Expired => Some(McpRemoteTaskLocalTerminal::Expired),
                        McpRemoteTaskStatus::Cancelled => {
                            Some(McpRemoteTaskLocalTerminal::Cancelled)
                        }
                        _ => None,
                    },
                })
            }
            insight_engine::TransitionOutcome::ExactReplay {
                authoritative: result,
            } => Ok(McpRemoteTaskObservation {
                result: observation.clone(),
                local_version: result.version(),
                local_terminal: match result.status() {
                    McpRemoteTaskStatus::Expired => Some(McpRemoteTaskLocalTerminal::Expired),
                    McpRemoteTaskStatus::Cancelled => Some(McpRemoteTaskLocalTerminal::Cancelled),
                    _ => None,
                },
            }),
            insight_engine::TransitionOutcome::StateConflict
            | insight_engine::TransitionOutcome::StaleLease => {
                Err(McpRemoteTaskHandlerError::Conflict)
            }
        }
    }

    async fn load_observation(
        &self,
        handle: &McpRemoteTaskHandle,
    ) -> Result<Option<McpRemoteTaskObservation>, McpRemoteTaskHandlerError> {
        let task_id = McpRemoteTaskId::new(handle.local_task_id.clone())
            .map_err(|_| McpRemoteTaskHandlerError::Invalid)?;
        let task = self
            .task_repository
            .load_mcp_remote_task(&task_id)
            .await
            .map_err(|_| McpRemoteTaskHandlerError::Unavailable)?
            .ok_or(McpRemoteTaskHandlerError::Conflict)?;
        if task.version() == 1 {
            return Ok(None);
        }
        let secret = self
            .task_repository
            .load_mcp_remote_task_secret(&task_id)
            .await
            .map_err(|_| McpRemoteTaskHandlerError::Unavailable)?
            .ok_or(McpRemoteTaskHandlerError::Conflict)?;
        let plaintext = self.open_secret(
            &task,
            McpSecretPurpose::RemoteTaskPayload,
            secret.latest_payload,
            secret.latest_payload_hash,
        )?;
        let observation: GetTaskResult =
            serde_json::from_slice(&plaintext).map_err(|_| McpRemoteTaskHandlerError::Invalid)?;
        observation
            .validate()
            .map_err(|_| McpRemoteTaskHandlerError::Invalid)?;
        Ok(Some(McpRemoteTaskObservation {
            result: observation,
            local_version: task.version(),
            local_terminal: match task.status() {
                McpRemoteTaskStatus::Expired => Some(McpRemoteTaskLocalTerminal::Expired),
                McpRemoteTaskStatus::Cancelled => Some(McpRemoteTaskLocalTerminal::Cancelled),
                _ => None,
            },
        }))
    }

    async fn record_local_terminal(
        &self,
        handle: &McpRemoteTaskHandle,
        terminal: McpRemoteTaskLocalTerminal,
    ) -> Result<bool, McpRemoteTaskHandlerError> {
        let task_id = McpRemoteTaskId::new(handle.local_task_id.clone())
            .map_err(|_| McpRemoteTaskHandlerError::Invalid)?;
        let task = self
            .task_repository
            .load_mcp_remote_task(&task_id)
            .await
            .map_err(|_| McpRemoteTaskHandlerError::Unavailable)?
            .ok_or(McpRemoteTaskHandlerError::Conflict)?;
        let secret = self
            .task_repository
            .load_mcp_remote_task_secret(&task_id)
            .await
            .map_err(|_| McpRemoteTaskHandlerError::Unavailable)?
            .ok_or(McpRemoteTaskHandlerError::Conflict)?;
        let remote_task_id = String::from_utf8(self.open_secret(
            &task,
            McpSecretPurpose::RemoteTaskId,
            secret.remote_task_id,
            secret.remote_task_id_hash,
        )?)
        .map_err(|_| McpRemoteTaskHandlerError::Invalid)?;
        let now = Utc::now();
        let (status, durable_status, error) = match terminal {
            McpRemoteTaskLocalTerminal::Cancelled => (
                insight_mcp::TaskStatus::Cancelled,
                McpRemoteTaskStatus::Cancelled,
                None,
            ),
            McpRemoteTaskLocalTerminal::Expired => (
                insight_mcp::TaskStatus::Failed,
                McpRemoteTaskStatus::Expired,
                Some(json!({"code": "TASK_EXPIRED"})),
            ),
        };
        let ttl_ms = (task.ttl_deadline() - task.remote_created_at())
            .num_milliseconds()
            .try_into()
            .map_err(|_| McpRemoteTaskHandlerError::Invalid)?;
        let observation = GetTaskResult {
            result_type: "complete".to_owned(),
            task: insight_mcp::Task {
                task_id: remote_task_id,
                status,
                status_message: Some(match terminal {
                    McpRemoteTaskLocalTerminal::Cancelled => {
                        "Cancelled by the local run authority".to_owned()
                    }
                    McpRemoteTaskLocalTerminal::Expired => {
                        "Expired by the local task authority".to_owned()
                    }
                }),
                created_at: task.remote_created_at(),
                last_updated_at: now.max(task.remote_created_at()),
                ttl_ms: Some(ttl_ms),
                poll_interval_ms: Some(task.poll_interval_ms()),
            },
            input_requests: None,
            result: None,
            error,
            metadata: None,
        };
        observation
            .validate()
            .map_err(|_| McpRemoteTaskHandlerError::Invalid)?;
        let payload =
            serde_jcs::to_vec(&observation).map_err(|_| McpRemoteTaskHandlerError::Invalid)?;
        let scope = McpSecretScope::new(
            task.principal(),
            task.server_id(),
            task.task_id().as_str(),
            McpSecretPurpose::RemoteTaskPayload,
        )
        .map_err(|_| McpRemoteTaskHandlerError::Invalid)?;
        let protected = self
            .protector
            .seal(&scope, &payload)
            .map_err(|_| McpRemoteTaskHandlerError::Unavailable)?;
        let terminal_receipt =
            canonical_sha256(&(task.task_id().as_str(), durable_status, &observation))
                .map_err(|_| McpRemoteTaskHandlerError::Invalid)?;
        let (payload, payload_hash) = protected.into_parts();
        let command = FinalizeMcpRemoteTaskCommand::new(
            task_id,
            format!(
                "local-{}-{}",
                handle.local_task_id,
                status_wire(durable_status)
            ),
            durable_status,
            payload,
            payload_hash,
            terminal_receipt,
            now,
        )
        .map_err(|_| McpRemoteTaskHandlerError::Invalid)?;
        match self
            .task_repository
            .finalize_mcp_remote_task(command)
            .await
            .map_err(|_| McpRemoteTaskHandlerError::Unavailable)?
        {
            insight_engine::TransitionOutcome::Committed { .. }
            | insight_engine::TransitionOutcome::ExactReplay { .. } => Ok(true),
            insight_engine::TransitionOutcome::StateConflict
            | insight_engine::TransitionOutcome::StaleLease => Ok(false),
        }
    }
}

struct DurableOAuthRuntimeClientResolver {
    authority: McpOAuthRuntimeAuthority,
    server_id: String,
    resource: String,
    client_id: String,
    redirect_uri: String,
    required_scopes: BTreeSet<String>,
    policy: StreamableHttpPolicy,
    limits: McpClientLimits,
    max_message_bytes: usize,
    allow_tasks: bool,
    protocol_version: String,
}

#[async_trait]
impl McpRuntimeClientResolver for DurableOAuthRuntimeClientResolver {
    async fn client_for(
        &self,
        context: &McpInteractionContext,
        cancellation: &CancellationToken,
    ) -> Result<Arc<McpClient>, McpRuntimeClientResolverError> {
        let principal = self
            .authority
            .interaction_repository
            .load_mcp_run_principal(&context.run_id)
            .await
            .map_err(|_| McpRuntimeClientResolverError::Unavailable)?
            .ok_or(McpRuntimeClientResolverError::AuthorizationRequired)?;
        match self.client_for_mcp_principal(&principal).await {
            Ok(client) => Ok(client),
            Err(
                error @ (McpRuntimeClientResolverError::AuthorizationRequired
                | McpRuntimeClientResolverError::InsufficientScope),
            ) => {
                self.await_authorization(
                    context,
                    &principal,
                    error == McpRuntimeClientResolverError::InsufficientScope,
                    cancellation,
                )
                .await?;
                self.client_for_mcp_principal(&principal).await
            }
            Err(error) => Err(error),
        }
    }
}

#[async_trait]
impl McpPrincipalClientResolver for DurableOAuthRuntimeClientResolver {
    async fn client_for_principal(
        &self,
        tenant_id: &str,
        user_id: &str,
    ) -> Result<Arc<McpClient>, McpPrincipalClientResolverError> {
        let principal = McpInteractionPrincipal::new(tenant_id, user_id)
            .map_err(|_| McpPrincipalClientResolverError::AuthorizationRequired)?;
        self.client_for_mcp_principal(&principal)
            .await
            .map_err(|error| match error {
                McpRuntimeClientResolverError::AuthorizationRequired => {
                    McpPrincipalClientResolverError::AuthorizationRequired
                }
                McpRuntimeClientResolverError::InsufficientScope => {
                    McpPrincipalClientResolverError::InsufficientScope
                }
                McpRuntimeClientResolverError::Declined
                | McpRuntimeClientResolverError::Cancelled
                | McpRuntimeClientResolverError::Expired
                | McpRuntimeClientResolverError::Unavailable => {
                    McpPrincipalClientResolverError::Unavailable
                }
            })
    }
}

impl DurableOAuthRuntimeClientResolver {
    async fn await_authorization(
        &self,
        context: &McpInteractionContext,
        principal: &McpInteractionPrincipal,
        step_up: bool,
        cancellation: &CancellationToken,
    ) -> Result<(), McpRuntimeClientResolverError> {
        let request = McpInteractionRequest::Authorization {
            message: if step_up {
                format!(
                    "Additional authorization is required for MCP server {}",
                    context.server_id
                )
            } else {
                format!("Authorize access to MCP server {}", context.server_id)
            },
            required_scopes: self.required_scopes.iter().cloned().collect(),
            step_up,
        };
        let handler = DurableMcpInteractionHandler::new(
            Arc::clone(&self.authority.interaction_repository),
            Arc::clone(&self.authority.protector),
        );
        let interaction = handler
            .open_or_create(
                context,
                principal,
                "oauth-authorization",
                request,
                json!({
                    "kind": "oauth_authorization",
                    "step_up": step_up,
                }),
            )
            .await
            .map_err(map_authorization_interaction_error)?;
        self.await_authorization_interaction(interaction, principal, context, cancellation)
            .await
    }

    async fn await_authorization_interaction(
        &self,
        mut interaction: McpInteraction,
        principal: &McpInteractionPrincipal,
        context: &McpInteractionContext,
        cancellation: &CancellationToken,
    ) -> Result<(), McpRuntimeClientResolverError> {
        loop {
            match (interaction.state(), interaction.outcome()) {
                (McpInteractionState::Closed, Some(McpInteractionOutcome::RetryCompleted)) => {
                    return Ok(())
                }
                (McpInteractionState::Closed, Some(McpInteractionOutcome::Declined)) => {
                    return Err(McpRuntimeClientResolverError::Declined)
                }
                (McpInteractionState::Closed, Some(McpInteractionOutcome::Cancelled)) => {
                    return Err(McpRuntimeClientResolverError::Cancelled)
                }
                (McpInteractionState::Closed, Some(McpInteractionOutcome::Expired)) => {
                    return Err(McpRuntimeClientResolverError::Expired)
                }
                (McpInteractionState::Closed, _) => {
                    return Err(McpRuntimeClientResolverError::Unavailable)
                }
                (McpInteractionState::Responded | McpInteractionState::Retrying, _) => {
                    return Err(McpRuntimeClientResolverError::Unavailable)
                }
                (McpInteractionState::Requested, _) => {}
            }

            if self.authorization_satisfied(principal).await? {
                interaction = self
                    .close_authorization_interaction(
                        interaction,
                        McpInteractionOutcome::RetryCompleted,
                        "authorized",
                    )
                    .await?;
                continue;
            }
            if Utc::now() >= interaction.deadline() {
                interaction = self
                    .close_authorization_interaction(
                        interaction,
                        McpInteractionOutcome::Expired,
                        "expired",
                    )
                    .await?;
                continue;
            }
            if let Some(permit) = context.operation_permit.as_ref() {
                permit.suspend();
            }
            let cancelled = tokio::select! {
                _ = cancellation.cancelled() => true,
                _ = tokio::time::sleep(Duration::from_millis(250)) => false,
            };
            if let Some(permit) = context.operation_permit.as_ref() {
                permit
                    .resume()
                    .await
                    .map_err(|_| McpRuntimeClientResolverError::Unavailable)?;
            }
            if cancelled {
                interaction = self
                    .close_authorization_interaction(
                        interaction,
                        McpInteractionOutcome::Cancelled,
                        "cancelled",
                    )
                    .await?;
            } else {
                interaction = self
                    .authority
                    .interaction_repository
                    .load_mcp_interaction(interaction.interaction_id())
                    .await
                    .map_err(|_| McpRuntimeClientResolverError::Unavailable)?
                    .ok_or(McpRuntimeClientResolverError::Unavailable)?;
            }
        }
    }

    async fn authorization_satisfied(
        &self,
        principal: &McpInteractionPrincipal,
    ) -> Result<bool, McpRuntimeClientResolverError> {
        Ok(self
            .authority
            .oauth_repository
            .load_mcp_oauth_credential(principal, &self.server_id)
            .await
            .map_err(|_| McpRuntimeClientResolverError::Unavailable)?
            .is_some_and(|credential| {
                credential.revoked_at().is_none()
                    && credential.resource() == self.resource
                    && credential.client_id() == self.client_id
                    && self
                        .required_scopes
                        .iter()
                        .all(|scope| credential.scopes().iter().any(|granted| granted == scope))
            }))
    }

    async fn close_authorization_interaction(
        &self,
        interaction: McpInteraction,
        outcome: McpInteractionOutcome,
        reason: &str,
    ) -> Result<McpInteraction, McpRuntimeClientResolverError> {
        let transition = TransitionMcpInteractionCommand::close(
            interaction.interaction_id().clone(),
            format!("oauth-{reason}-v{}", interaction.version()),
            interaction.version(),
            outcome,
            Utc::now(),
        )
        .map_err(|_| McpRuntimeClientResolverError::Unavailable)?;
        match self
            .authority
            .interaction_repository
            .transition_mcp_interaction(transition)
            .await
            .map_err(|_| McpRuntimeClientResolverError::Unavailable)?
        {
            insight_engine::TransitionOutcome::Committed { result } => {
                insight_mcp::adjust_operational_gauge(
                    &self.server_id,
                    insight_mcp::McpOperationalGauge::OpenInteractions,
                    -1,
                );
                Ok(result)
            }
            insight_engine::TransitionOutcome::ExactReplay {
                authoritative: result,
            } => Ok(result),
            insight_engine::TransitionOutcome::StateConflict
            | insight_engine::TransitionOutcome::StaleLease => self
                .authority
                .interaction_repository
                .load_mcp_interaction(interaction.interaction_id())
                .await
                .map_err(|_| McpRuntimeClientResolverError::Unavailable)?
                .ok_or(McpRuntimeClientResolverError::Unavailable),
        }
    }

    async fn client_for_mcp_principal(
        &self,
        principal: &McpInteractionPrincipal,
    ) -> Result<Arc<McpClient>, McpRuntimeClientResolverError> {
        let credential = self
            .authority
            .oauth_repository
            .load_mcp_oauth_credential(principal, &self.server_id)
            .await
            .map_err(|_| McpRuntimeClientResolverError::Unavailable)?
            .filter(|credential| credential.revoked_at().is_none())
            .ok_or(McpRuntimeClientResolverError::AuthorizationRequired)?;
        if credential.resource() != self.resource || credential.client_id() != self.client_id {
            return Err(McpRuntimeClientResolverError::AuthorizationRequired);
        }
        if !self
            .required_scopes
            .iter()
            .all(|scope| credential.scopes().iter().any(|granted| granted == scope))
        {
            return Err(McpRuntimeClientResolverError::InsufficientScope);
        }
        let secret = self
            .authority
            .oauth_repository
            .load_mcp_oauth_credential_secret(principal, &self.server_id)
            .await
            .map_err(|_| McpRuntimeClientResolverError::Unavailable)?
            .ok_or(McpRuntimeClientResolverError::AuthorizationRequired)?;
        let token = if credential
            .access_expires_at()
            .is_some_and(|expires_at| expires_at <= Utc::now() + ChronoDuration::seconds(30))
        {
            self.refresh_access_token(principal, &credential, secret)
                .await?
        } else {
            self.open_token(
                principal,
                credential.generation(),
                McpSecretPurpose::AccessToken,
                secret.access_token,
                secret.access_token_hash,
            )?
        };
        self.client_with_token(&token)
    }
}

fn map_authorization_interaction_error(
    error: McpInteractionHandlerError,
) -> McpRuntimeClientResolverError {
    match error {
        McpInteractionHandlerError::Declined => McpRuntimeClientResolverError::Declined,
        McpInteractionHandlerError::Cancelled => McpRuntimeClientResolverError::Cancelled,
        McpInteractionHandlerError::Expired => McpRuntimeClientResolverError::Expired,
        McpInteractionHandlerError::Unavailable | McpInteractionHandlerError::Invalid => {
            McpRuntimeClientResolverError::Unavailable
        }
    }
}

impl DurableOAuthRuntimeClientResolver {
    fn open_token(
        &self,
        principal: &McpInteractionPrincipal,
        generation: u64,
        purpose: McpSecretPurpose,
        ciphertext: insight_durable::McpSecretCiphertext,
        content_hash: String,
    ) -> Result<String, McpRuntimeClientResolverError> {
        let scope = McpSecretScope::new(
            principal,
            &self.server_id,
            format!("credential-{generation}"),
            purpose,
        )
        .map_err(|_| McpRuntimeClientResolverError::Unavailable)?;
        let protected = McpProtectedSecret::new(ciphertext, content_hash)
            .map_err(|_| McpRuntimeClientResolverError::Unavailable)?;
        let plaintext = self
            .authority
            .protector
            .open(&scope, &protected)
            .map_err(|_| McpRuntimeClientResolverError::Unavailable)?;
        let token =
            String::from_utf8(plaintext).map_err(|_| McpRuntimeClientResolverError::Unavailable)?;
        Ok(token)
    }

    fn protect_token(
        &self,
        principal: &McpInteractionPrincipal,
        generation: u64,
        purpose: McpSecretPurpose,
        token: &str,
    ) -> Result<McpProtectedSecret, McpRuntimeClientResolverError> {
        let scope = McpSecretScope::new(
            principal,
            &self.server_id,
            format!("credential-{generation}"),
            purpose,
        )
        .map_err(|_| McpRuntimeClientResolverError::Unavailable)?;
        self.authority
            .protector
            .seal(&scope, token.as_bytes())
            .map_err(|_| McpRuntimeClientResolverError::Unavailable)
    }

    fn client_with_token(
        &self,
        token: &str,
    ) -> Result<Arc<McpClient>, McpRuntimeClientResolverError> {
        let credential = HttpCredential::bearer(token)
            .map_err(|_| McpRuntimeClientResolverError::Unavailable)?;
        let transport = StreamableHttpTransport::new(self.policy.clone(), Some(credential))
            .map_err(|_| McpRuntimeClientResolverError::Unavailable)?;
        build_mcp_client_for_protocol(
            Arc::new(transport),
            self.limits,
            self.max_message_bytes,
            self.allow_tasks,
            &self.protocol_version,
        )
        .map_err(|_| McpRuntimeClientResolverError::Unavailable)
    }

    async fn refresh_access_token(
        &self,
        principal: &McpInteractionPrincipal,
        credential: &insight_durable::McpOAuthCredential,
        secret: insight_durable::McpOAuthCredentialSecret,
    ) -> Result<String, McpRuntimeClientResolverError> {
        let refresh_owner = format!(
            "{}-{}",
            self.authority.refresh_owner,
            uuid::Uuid::new_v4().simple()
        );
        let now = Utc::now();
        let refresh_lease = self
            .policy
            .request_timeout()
            .saturating_add(Duration::from_secs(5))
            .min(Duration::from_secs(5 * 60));
        let claimed = self
            .authority
            .oauth_repository
            .claim_mcp_oauth_refresh(
                ClaimMcpOAuthRefreshCommand::new(
                    principal.clone(),
                    &self.server_id,
                    credential.generation(),
                    &refresh_owner,
                    now,
                    now + ChronoDuration::from_std(refresh_lease)
                        .map_err(|_| McpRuntimeClientResolverError::Unavailable)?,
                )
                .map_err(|_| McpRuntimeClientResolverError::Unavailable)?,
            )
            .await
            .map_err(|_| McpRuntimeClientResolverError::Unavailable)?;
        if !claimed {
            return self
                .await_refresh_winner(principal, credential.generation())
                .await;
        }
        let mut dispatched = false;
        let result = async {
            let refresh_ciphertext = secret
                .refresh_token
                .ok_or(McpRuntimeClientResolverError::AuthorizationRequired)?;
            let refresh_hash = secret
                .refresh_token_hash
                .ok_or(McpRuntimeClientResolverError::AuthorizationRequired)?;
            let refresh_token = self.open_token(
                principal,
                credential.generation(),
                McpSecretPurpose::RefreshToken,
                refresh_ciphertext,
                refresh_hash,
            )?;
            let oauth = OAuthClient::new(self.policy.request_timeout())
                .map_err(|_| McpRuntimeClientResolverError::Unavailable)?;
            let metadata = oauth
                .discover_authorization_server(credential.issuer())
                .await
                .map_err(|_| McpRuntimeClientResolverError::Unavailable)?;
            let registration = OAuthClientRegistration::new(
                &self.client_id,
                &self.redirect_uri,
                self.required_scopes.iter().cloned().collect(),
            )
            .map_err(|_| McpRuntimeClientResolverError::Unavailable)?;
            if !self
                .authority
                .oauth_repository
                .mark_mcp_oauth_refresh_dispatched(
                    principal,
                    &self.server_id,
                    credential.generation(),
                    &refresh_owner,
                    Utc::now(),
                )
                .await
                .map_err(|_| McpRuntimeClientResolverError::Unavailable)?
            {
                return Err(McpRuntimeClientResolverError::Unavailable);
            }
            dispatched = true;
            let token = oauth
                .refresh(&metadata, &registration, &self.resource, &refresh_token)
                .await
                .map_err(|_| McpRuntimeClientResolverError::AuthorizationRequired)?;
            if !self
                .required_scopes
                .iter()
                .all(|scope| token.scopes().iter().any(|granted| granted == scope))
            {
                return Err(McpRuntimeClientResolverError::InsufficientScope);
            }
            let generation = credential.generation() + 1;
            let access = self.protect_token(
                principal,
                generation,
                McpSecretPurpose::AccessToken,
                token.access_token(),
            )?;
            let rotated_refresh = token.refresh_token().unwrap_or(&refresh_token);
            let refresh = self.protect_token(
                principal,
                generation,
                McpSecretPurpose::RefreshToken,
                rotated_refresh,
            )?;
            let (access_ciphertext, access_hash) = access.into_parts();
            let (refresh_ciphertext, refresh_hash) = refresh.into_parts();
            let now = Utc::now();
            let command = StoreMcpOAuthCredentialCommand::new(
                principal.clone(),
                &self.server_id,
                credential.issuer(),
                &self.client_id,
                &self.resource,
                token.scopes().to_vec(),
                token.token_type(),
                generation,
                Some(credential.generation()),
                format!("refresh-{}-{generation}", self.server_id),
                access_ciphertext,
                access_hash,
                Some(refresh_ciphertext),
                Some(refresh_hash),
                token
                    .expires_in()
                    .and_then(|duration| ChronoDuration::from_std(duration).ok())
                    .map(|duration| now + duration),
                now,
            )
            .and_then(|command| command.with_refresh_lease_owner(&refresh_owner))
            .map_err(|_| McpRuntimeClientResolverError::Unavailable)?;
            match self
                .authority
                .oauth_repository
                .store_mcp_oauth_credential(command)
                .await
                .map_err(|_| McpRuntimeClientResolverError::Unavailable)?
            {
                insight_engine::TransitionOutcome::Committed { .. }
                | insight_engine::TransitionOutcome::ExactReplay { .. } => {
                    Ok(token.access_token().to_owned())
                }
                insight_engine::TransitionOutcome::StateConflict
                | insight_engine::TransitionOutcome::StaleLease => {
                    let winner = self
                        .authority
                        .oauth_repository
                        .load_mcp_oauth_credential(principal, &self.server_id)
                        .await
                        .map_err(|_| McpRuntimeClientResolverError::Unavailable)?
                        .filter(|winner| {
                            winner.revoked_at().is_none()
                                && winner.generation() > credential.generation()
                        })
                        .ok_or(McpRuntimeClientResolverError::AuthorizationRequired)?;
                    let winner_secret = self
                        .authority
                        .oauth_repository
                        .load_mcp_oauth_credential_secret(principal, &self.server_id)
                        .await
                        .map_err(|_| McpRuntimeClientResolverError::Unavailable)?
                        .ok_or(McpRuntimeClientResolverError::AuthorizationRequired)?;
                    self.open_token(
                        principal,
                        winner.generation(),
                        McpSecretPurpose::AccessToken,
                        winner_secret.access_token,
                        winner_secret.access_token_hash,
                    )
                }
            }
        }
        .await;
        if dispatched {
            insight_mcp::record_operational_event(
                &self.server_id,
                if result.is_ok() {
                    insight_mcp::McpOperationalEvent::OAuthRefreshSucceeded
                } else {
                    insight_mcp::McpOperationalEvent::OAuthRefreshFailed
                },
            );
        }
        if dispatched && result.is_err() {
            let _ = self
                .authority
                .oauth_repository
                .quarantine_mcp_oauth_refresh(
                    principal,
                    &self.server_id,
                    credential.generation(),
                    &refresh_owner,
                    Utc::now(),
                )
                .await;
        } else if !dispatched {
            let _ = self
                .authority
                .oauth_repository
                .release_mcp_oauth_refresh(
                    principal,
                    &self.server_id,
                    credential.generation(),
                    &refresh_owner,
                )
                .await;
        }
        result
    }

    async fn await_refresh_winner(
        &self,
        principal: &McpInteractionPrincipal,
        previous_generation: u64,
    ) -> Result<String, McpRuntimeClientResolverError> {
        let deadline = tokio::time::Instant::now() + self.policy.request_timeout();
        loop {
            let credential = self
                .authority
                .oauth_repository
                .load_mcp_oauth_credential(principal, &self.server_id)
                .await
                .map_err(|_| McpRuntimeClientResolverError::Unavailable)?;
            if credential
                .as_ref()
                .is_some_and(|credential| credential.revoked_at().is_some())
            {
                return Err(McpRuntimeClientResolverError::AuthorizationRequired);
            }
            if let Some(credential) =
                credential.filter(|credential| credential.generation() > previous_generation)
            {
                let secret = self
                    .authority
                    .oauth_repository
                    .load_mcp_oauth_credential_secret(principal, &self.server_id)
                    .await
                    .map_err(|_| McpRuntimeClientResolverError::Unavailable)?
                    .ok_or(McpRuntimeClientResolverError::AuthorizationRequired)?;
                return self.open_token(
                    principal,
                    credential.generation(),
                    McpSecretPurpose::AccessToken,
                    secret.access_token,
                    secret.access_token_hash,
                );
            }
            if tokio::time::Instant::now() >= deadline {
                return Err(McpRuntimeClientResolverError::Unavailable);
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    }
}

const MAX_SIGNED_MANIFEST_BYTES: usize = 32 * 1024 * 1024;
const MAX_MANIFEST_LIFETIME: ChronoDuration = ChronoDuration::days(30);

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct SignedMcpManifest {
    schema_version: u32,
    server_id: String,
    protocol_version: String,
    generated_at: DateTime<Utc>,
    expires_at: DateTime<Utc>,
    discover: insight_mcp::DiscoverResult,
    #[serde(default)]
    tools: Vec<ListToolsResult>,
    #[serde(default)]
    resources: Vec<ListResourcesResult>,
    #[serde(default)]
    resource_templates: Vec<ListResourceTemplatesResult>,
    #[serde(default)]
    prompts: Vec<ListPromptsResult>,
    signer_key_id: String,
    content_hash: String,
    signature: String,
}

#[derive(Serialize)]
struct SignedMcpManifestPayload<'a> {
    schema_version: u32,
    server_id: &'a str,
    protocol_version: &'a str,
    generated_at: DateTime<Utc>,
    expires_at: DateTime<Utc>,
    discover: &'a insight_mcp::DiscoverResult,
    tools: &'a [ListToolsResult],
    resources: &'a [ListResourcesResult],
    resource_templates: &'a [ListResourceTemplatesResult],
    prompts: &'a [ListPromptsResult],
    signer_key_id: &'a str,
}

struct SignedManifestTransport {
    kind: TransportKind,
    responses: BTreeMap<(String, Option<String>), Value>,
}

#[async_trait]
impl McpTransport for SignedManifestTransport {
    fn kind(&self) -> TransportKind {
        self.kind
    }

    async fn exchange(
        &self,
        request: &Value,
        parameter_headers: &BTreeMap<String, String>,
        cancellation: &CancellationToken,
        _observer: &dyn McpNotificationObserver,
    ) -> Result<Value, TransportError> {
        if cancellation.is_cancelled() {
            return Err(TransportError::Cancelled);
        }
        if !parameter_headers.is_empty() {
            return Err(TransportError::Header);
        }
        let method = request
            .get("method")
            .and_then(Value::as_str)
            .ok_or(TransportError::Request)?;
        let cursor = request
            .get("params")
            .and_then(|params| params.get("cursor"))
            .and_then(Value::as_str)
            .map(str::to_owned);
        let result = self
            .responses
            .get(&(method.to_owned(), cursor))
            .cloned()
            .ok_or(TransportError::Response)?;
        Ok(json!({
            "jsonrpc":"2.0",
            "id":request.get("id").cloned().ok_or(TransportError::Request)?,
            "result":result
        }))
    }
}

async fn signed_manifest_transport(
    server_id: &str,
    path: &std::path::Path,
    trust_keys_json: &str,
    kind: TransportKind,
) -> Result<Arc<dyn McpTransport>, McpResourceConfigError> {
    let bytes = tokio::fs::read(path)
        .await
        .map_err(|_| McpResourceConfigError::ManifestRequired)?;
    if bytes.is_empty() || bytes.len() > MAX_SIGNED_MANIFEST_BYTES {
        return Err(McpResourceConfigError::ManifestInvalid);
    }
    let manifest: SignedMcpManifest =
        serde_json::from_slice(&bytes).map_err(|_| McpResourceConfigError::ManifestInvalid)?;
    let now = Utc::now();
    if manifest.schema_version != 1
        || manifest.server_id != server_id
        || manifest.protocol_version != MCP_PROTOCOL_VERSION
        || manifest.generated_at > now + ChronoDuration::minutes(5)
        || manifest.expires_at <= now
        || manifest.expires_at - manifest.generated_at > MAX_MANIFEST_LIFETIME
        || manifest.discover.result_type != "complete"
        || !manifest
            .discover
            .supported_versions
            .iter()
            .any(|version| version == MCP_PROTOCOL_VERSION)
    {
        return Err(McpResourceConfigError::ManifestInvalid);
    }
    let payload = SignedMcpManifestPayload {
        schema_version: manifest.schema_version,
        server_id: &manifest.server_id,
        protocol_version: &manifest.protocol_version,
        generated_at: manifest.generated_at,
        expires_at: manifest.expires_at,
        discover: &manifest.discover,
        tools: &manifest.tools,
        resources: &manifest.resources,
        resource_templates: &manifest.resource_templates,
        prompts: &manifest.prompts,
        signer_key_id: &manifest.signer_key_id,
    };
    let canonical =
        serde_jcs::to_vec(&payload).map_err(|_| McpResourceConfigError::ManifestInvalid)?;
    if canonical_sha256_bytes(&canonical) != manifest.content_hash {
        return Err(McpResourceConfigError::ManifestInvalid);
    }
    let trust_keys = serde_json::from_str::<BTreeMap<String, String>>(trust_keys_json)
        .map_err(|_| McpResourceConfigError::ManifestInvalid)?;
    let public_key = trust_keys
        .get(&manifest.signer_key_id)
        .ok_or(McpResourceConfigError::ManifestInvalid)
        .and_then(|encoded| {
            BASE64_STANDARD
                .decode(encoded)
                .map_err(|_| McpResourceConfigError::ManifestInvalid)
        })?;
    let signature_bytes = BASE64_STANDARD
        .decode(&manifest.signature)
        .map_err(|_| McpResourceConfigError::ManifestInvalid)?;
    signature::UnparsedPublicKey::new(&signature::ED25519, public_key)
        .verify(&canonical, &signature_bytes)
        .map_err(|_| McpResourceConfigError::ManifestInvalid)?;

    let mut responses = BTreeMap::new();
    responses.insert(
        ("server/discover".to_owned(), None),
        serde_json::to_value(&manifest.discover)
            .map_err(|_| McpResourceConfigError::ManifestInvalid)?,
    );
    insert_manifest_pages(&mut responses, "tools/list", &manifest.tools)?;
    insert_manifest_pages(&mut responses, "resources/list", &manifest.resources)?;
    insert_manifest_pages(
        &mut responses,
        "resources/templates/list",
        &manifest.resource_templates,
    )?;
    insert_manifest_pages(&mut responses, "prompts/list", &manifest.prompts)?;
    Ok(Arc::new(SignedManifestTransport { kind, responses }))
}

fn insert_manifest_pages<T: Serialize>(
    responses: &mut BTreeMap<(String, Option<String>), Value>,
    method: &str,
    pages: &[T],
) -> Result<(), McpResourceConfigError> {
    let mut cursor = None;
    for (index, page) in pages.iter().enumerate() {
        let value =
            serde_json::to_value(page).map_err(|_| McpResourceConfigError::ManifestInvalid)?;
        if responses
            .insert((method.to_owned(), cursor.clone()), value.clone())
            .is_some()
        {
            return Err(McpResourceConfigError::ManifestInvalid);
        }
        cursor = value
            .get("nextCursor")
            .and_then(Value::as_str)
            .map(str::to_owned);
        if cursor.is_none() && index + 1 != pages.len() {
            return Err(McpResourceConfigError::ManifestInvalid);
        }
    }
    if !pages.is_empty() && cursor.is_some() {
        return Err(McpResourceConfigError::ManifestInvalid);
    }
    Ok(())
}

fn build_mcp_client(
    transport: Arc<dyn McpTransport>,
    limits: McpClientLimits,
    max_message_bytes: usize,
    allow_tasks: bool,
) -> Result<Arc<McpClient>, McpResourceConfigError> {
    build_mcp_client_for_protocol(
        transport,
        limits,
        max_message_bytes,
        allow_tasks,
        MCP_PROTOCOL_VERSION,
    )
}

fn build_mcp_client_for_protocol(
    transport: Arc<dyn McpTransport>,
    limits: McpClientLimits,
    max_message_bytes: usize,
    allow_tasks: bool,
    protocol_version: &str,
) -> Result<Arc<McpClient>, McpResourceConfigError> {
    if protocol_version == MCP_LEGACY_PROTOCOL_VERSION && allow_tasks {
        return Err(McpResourceConfigError::Capability);
    }
    let extensions = allow_tasks
        .then(|| {
            MetaMap::new(BTreeMap::from([(
                insight_mcp::MCP_TASKS_EXTENSION_ID.to_owned(),
                json!({}),
            )]))
        })
        .transpose()
        .map_err(|_| McpResourceConfigError::Client)?;
    let client_info = ClientInfo {
        name: "insight-agent-platform".to_owned(),
        version: env!("CARGO_PKG_VERSION").to_owned(),
        title: Some("Insight Agent Platform".to_owned()),
        description: None,
        website_url: None,
        icons: Vec::new(),
    };
    let capabilities = ClientCapabilities {
        elicitation: Some(json!({})),
        extensions,
        ..ClientCapabilities::default()
    };
    let client = if protocol_version == MCP_LEGACY_PROTOCOL_VERSION {
        let transport = Arc::new(
            LegacyCompatibilityTransport::new(transport, client_info.clone(), capabilities.clone())
                .map_err(|_| McpResourceConfigError::Client)?,
        ) as Arc<dyn McpTransport>;
        McpClient::new_legacy(transport, client_info, capabilities)
    } else if protocol_version == MCP_PROTOCOL_VERSION {
        McpClient::new(transport, client_info, capabilities)
    } else {
        return Err(McpResourceConfigError::Client);
    }
    .and_then(|client| client.with_limits(limits))
    .and_then(|client| {
        client.with_protocol_limits(ProtocolLimits {
            max_message_bytes,
            ..ProtocolLimits::default()
        })
    })
    .map_err(|_| McpResourceConfigError::Client)?;
    Ok(Arc::new(client))
}

#[derive(Clone)]
pub struct DurableMcpInteractionHandler {
    repository: Arc<dyn McpInteractionDurableRepository>,
    protector: Arc<dyn McpSecretProtector>,
    poll_interval: Duration,
    max_lifetime: chrono::Duration,
}

impl DurableMcpInteractionHandler {
    pub fn new(
        repository: Arc<dyn McpInteractionDurableRepository>,
        protector: Arc<dyn McpSecretProtector>,
    ) -> Self {
        Self {
            repository,
            protector,
            poll_interval: Duration::from_millis(250),
            max_lifetime: chrono::Duration::hours(24),
        }
    }

    async fn principal(
        &self,
        context: &McpInteractionContext,
    ) -> Result<McpInteractionPrincipal, McpInteractionHandlerError> {
        self.repository
            .load_mcp_run_principal(&context.run_id)
            .await
            .map_err(|_| McpInteractionHandlerError::Unavailable)?
            .ok_or(McpInteractionHandlerError::Unavailable)
    }

    async fn open_or_create(
        &self,
        context: &McpInteractionContext,
        principal: &McpInteractionPrincipal,
        request_key: &str,
        request: McpInteractionRequest,
        private_request: Value,
    ) -> Result<McpInteraction, McpInteractionHandlerError> {
        let interaction_id = deterministic_interaction_id(context, request_key, &request)?;
        if let Some(existing) = self
            .repository
            .load_mcp_interaction(&interaction_id)
            .await
            .map_err(|_| McpInteractionHandlerError::Unavailable)?
        {
            return Ok(existing);
        }
        let scope = McpSecretScope::new(
            principal,
            &context.server_id,
            interaction_id.as_str(),
            McpSecretPurpose::ElicitationRequest,
        )
        .map_err(|_| McpInteractionHandlerError::Invalid)?;
        let private_request =
            serde_jcs::to_vec(&private_request).map_err(|_| McpInteractionHandlerError::Invalid)?;
        let protected = self
            .protector
            .seal(&scope, &private_request)
            .map_err(|_| McpInteractionHandlerError::Unavailable)?;
        let (request_secret, request_hash) = protected.into_parts();
        let now = Utc::now();
        let command = CreateMcpInteractionCommand::new(
            interaction_id.clone(),
            principal.clone(),
            &context.run_id,
            &context.operation_id,
            &context.server_id,
            &context.binding_hash,
            request_key,
            context.generation,
            request,
            request_secret,
            request_hash,
            now + self.max_lifetime,
            now,
        )
        .map_err(|_| McpInteractionHandlerError::Invalid)?;
        match self
            .repository
            .create_mcp_interaction(command)
            .await
            .map_err(|_| McpInteractionHandlerError::Unavailable)?
        {
            insight_engine::TransitionOutcome::Committed { result } => {
                insight_mcp::adjust_operational_gauge(
                    &context.server_id,
                    insight_mcp::McpOperationalGauge::OpenInteractions,
                    1,
                );
                insight_mcp::set_operational_gauge(
                    &context.server_id,
                    insight_mcp::McpOperationalGauge::OldestInteractionAgeSeconds,
                    0,
                );
                Ok(result)
            }
            insight_engine::TransitionOutcome::ExactReplay {
                authoritative: result,
            } => Ok(result),
            insight_engine::TransitionOutcome::StateConflict
            | insight_engine::TransitionOutcome::StaleLease => self
                .repository
                .load_mcp_interaction(&interaction_id)
                .await
                .map_err(|_| McpInteractionHandlerError::Unavailable)?
                .ok_or(McpInteractionHandlerError::Unavailable),
        }
    }

    async fn await_accepted(
        &self,
        mut interaction: McpInteraction,
        cancellation: &CancellationToken,
        operation_permit: Option<&insight_engine::worker::WorkerOperationPermitHandle>,
    ) -> Result<Value, McpInteractionHandlerError> {
        loop {
            match (interaction.state(), interaction.outcome()) {
                (
                    McpInteractionState::Responded | McpInteractionState::Retrying,
                    Some(McpInteractionOutcome::Accepted),
                ) => {
                    let authority = self
                        .repository
                        .load_mcp_interaction_secret(interaction.interaction_id())
                        .await
                        .map_err(|_| McpInteractionHandlerError::Unavailable)?
                        .ok_or(McpInteractionHandlerError::Unavailable)?;
                    let ciphertext = authority
                        .response_secret
                        .ok_or(McpInteractionHandlerError::Invalid)?;
                    let content_hash = authority
                        .response_hash
                        .ok_or(McpInteractionHandlerError::Invalid)?;
                    let protected = McpProtectedSecret::new(ciphertext, content_hash)
                        .map_err(|_| McpInteractionHandlerError::Invalid)?;
                    let scope = McpSecretScope::new(
                        interaction.principal(),
                        interaction.server_id(),
                        interaction.interaction_id().as_str(),
                        McpSecretPurpose::ElicitationResponse,
                    )
                    .map_err(|_| McpInteractionHandlerError::Invalid)?;
                    let plaintext = self
                        .protector
                        .open(&scope, &protected)
                        .map_err(|_| McpInteractionHandlerError::Invalid)?;
                    return serde_json::from_slice(&plaintext)
                        .map_err(|_| McpInteractionHandlerError::Invalid);
                }
                (McpInteractionState::Closed, Some(McpInteractionOutcome::Declined)) => {
                    return Err(McpInteractionHandlerError::Declined)
                }
                (McpInteractionState::Closed, Some(McpInteractionOutcome::Cancelled)) => {
                    return Err(McpInteractionHandlerError::Cancelled)
                }
                (McpInteractionState::Closed, Some(McpInteractionOutcome::Expired)) => {
                    return Err(McpInteractionHandlerError::Expired)
                }
                (McpInteractionState::Closed, _) => {
                    return Err(McpInteractionHandlerError::Unavailable)
                }
                _ => {}
            }
            if Utc::now() >= interaction.deadline() {
                let transition = TransitionMcpInteractionCommand::close(
                    interaction.interaction_id().clone(),
                    format!("expire-v{}", interaction.version()),
                    interaction.version(),
                    McpInteractionOutcome::Expired,
                    Utc::now(),
                )
                .map_err(|_| McpInteractionHandlerError::Invalid)?;
                interaction = match self
                    .repository
                    .transition_mcp_interaction(transition)
                    .await
                    .map_err(|_| McpInteractionHandlerError::Unavailable)?
                {
                    insight_engine::TransitionOutcome::Committed { result } => {
                        insight_mcp::record_operational_event(
                            interaction.server_id(),
                            insight_mcp::McpOperationalEvent::InteractionExpired,
                        );
                        insight_mcp::adjust_operational_gauge(
                            interaction.server_id(),
                            insight_mcp::McpOperationalGauge::OpenInteractions,
                            -1,
                        );
                        result
                    }
                    insight_engine::TransitionOutcome::ExactReplay {
                        authoritative: result,
                    } => result,
                    insight_engine::TransitionOutcome::StateConflict
                    | insight_engine::TransitionOutcome::StaleLease => self
                        .repository
                        .load_mcp_interaction(interaction.interaction_id())
                        .await
                        .map_err(|_| McpInteractionHandlerError::Unavailable)?
                        .ok_or(McpInteractionHandlerError::Unavailable)?,
                };
                continue;
            }
            if let Some(permit) = operation_permit {
                permit.suspend();
            }
            let cancelled = tokio::select! {
                _ = cancellation.cancelled() => true,
                _ = tokio::time::sleep(self.poll_interval) => false,
            };
            if let Some(permit) = operation_permit {
                permit
                    .resume()
                    .await
                    .map_err(|_| McpInteractionHandlerError::Unavailable)?;
            }
            if cancelled {
                let transition = TransitionMcpInteractionCommand::close(
                    interaction.interaction_id().clone(),
                    format!("cancel-v{}", interaction.version()),
                    interaction.version(),
                    McpInteractionOutcome::Cancelled,
                    Utc::now(),
                )
                .map_err(|_| McpInteractionHandlerError::Invalid)?;
                interaction = match self
                    .repository
                    .transition_mcp_interaction(transition)
                    .await
                    .map_err(|_| McpInteractionHandlerError::Unavailable)?
                {
                    insight_engine::TransitionOutcome::Committed { result } => {
                        insight_mcp::record_operational_event(
                            interaction.server_id(),
                            insight_mcp::McpOperationalEvent::InteractionCancelled,
                        );
                        insight_mcp::adjust_operational_gauge(
                            interaction.server_id(),
                            insight_mcp::McpOperationalGauge::OpenInteractions,
                            -1,
                        );
                        result
                    }
                    insight_engine::TransitionOutcome::ExactReplay {
                        authoritative: result,
                    } => result,
                    insight_engine::TransitionOutcome::StateConflict
                    | insight_engine::TransitionOutcome::StaleLease => self
                        .repository
                        .load_mcp_interaction(interaction.interaction_id())
                        .await
                        .map_err(|_| McpInteractionHandlerError::Unavailable)?
                        .ok_or(McpInteractionHandlerError::Unavailable)?,
                };
            }
            if interaction.state() != McpInteractionState::Closed {
                interaction = self
                    .repository
                    .load_mcp_interaction(interaction.interaction_id())
                    .await
                    .map_err(|_| McpInteractionHandlerError::Unavailable)?
                    .ok_or(McpInteractionHandlerError::Unavailable)?;
            }
        }
    }

    async fn close_interaction(
        &self,
        interaction_id: &str,
        succeeded: bool,
    ) -> Result<(), McpInteractionHandlerError> {
        let interaction_id = McpInteractionId::new(interaction_id)
            .map_err(|_| McpInteractionHandlerError::Invalid)?;
        let Some(interaction) = self
            .repository
            .load_mcp_interaction(&interaction_id)
            .await
            .map_err(|_| McpInteractionHandlerError::Unavailable)?
        else {
            return Err(McpInteractionHandlerError::Unavailable);
        };
        if interaction.state() == McpInteractionState::Closed {
            return Ok(());
        }
        if !matches!(
            interaction.state(),
            McpInteractionState::Responded | McpInteractionState::Retrying
        ) {
            return Err(McpInteractionHandlerError::Unavailable);
        }
        let outcome = if succeeded {
            McpInteractionOutcome::RetryCompleted
        } else {
            McpInteractionOutcome::RetryFailed
        };
        let transition = TransitionMcpInteractionCommand::close(
            interaction_id,
            format!("complete-v{}", interaction.version()),
            interaction.version(),
            outcome,
            Utc::now(),
        )
        .map_err(|_| McpInteractionHandlerError::Invalid)?;
        match self
            .repository
            .transition_mcp_interaction(transition)
            .await
            .map_err(|_| McpInteractionHandlerError::Unavailable)?
        {
            insight_engine::TransitionOutcome::Committed { .. } => {
                insight_mcp::record_operational_event(
                    interaction.server_id(),
                    if succeeded {
                        insight_mcp::McpOperationalEvent::InteractionRetryCompleted
                    } else {
                        insight_mcp::McpOperationalEvent::InteractionRetryFailed
                    },
                );
                insight_mcp::adjust_operational_gauge(
                    interaction.server_id(),
                    insight_mcp::McpOperationalGauge::OpenInteractions,
                    -1,
                );
                Ok(())
            }
            insight_engine::TransitionOutcome::ExactReplay { .. } => Ok(()),
            insight_engine::TransitionOutcome::StateConflict
            | insight_engine::TransitionOutcome::StaleLease => {
                Err(McpInteractionHandlerError::Unavailable)
            }
        }
    }
}

#[async_trait]
impl McpInteractionHandler for DurableMcpInteractionHandler {
    async fn request_approval(
        &self,
        context: &McpInteractionContext,
        effect: EffectClass,
        cancellation: &CancellationToken,
    ) -> Result<Vec<String>, McpInteractionHandlerError> {
        let principal = self.principal(context).await?;
        let effect = match effect {
            EffectClass::Pure => "pure",
            EffectClass::ReadOnly => "read_only",
            EffectClass::Mutating => "mutating",
        };
        let interaction = self
            .open_or_create(
                context,
                &principal,
                "local-approval",
                McpInteractionRequest::Approval {
                    message: format!(
                        "Approve {effect} MCP tool execution on server {}",
                        context.server_id
                    ),
                    effect: effect.to_owned(),
                },
                json!({"kind":"approval"}),
            )
            .await?;
        self.await_accepted(
            interaction.clone(),
            cancellation,
            context.operation_permit.as_ref(),
        )
        .await?;
        Ok(vec![interaction.interaction_id().as_str().to_owned()])
    }

    async fn resolve_input(
        &self,
        context: &McpInteractionContext,
        required: InputRequiredResult,
        cancellation: &CancellationToken,
    ) -> Result<McpInteractionResolution, McpInteractionHandlerError> {
        required
            .validate()
            .map_err(|_| McpInteractionHandlerError::Invalid)?;
        let principal = self.principal(context).await?;
        let mut pending = Vec::with_capacity(required.input_requests.len());
        for (request_key, input_request) in &required.input_requests {
            let (request, private_request) =
                safe_interaction_request(input_request, required.request_state.as_deref())?;
            let interaction = self
                .open_or_create(
                    context,
                    &principal,
                    &format!("input-{request_key}"),
                    request,
                    private_request,
                )
                .await?;
            pending.push((request_key.clone(), interaction));
        }
        let mut input_responses = BTreeMap::new();
        let mut interaction_ids = Vec::with_capacity(pending.len());
        for (request_key, interaction) in pending {
            let response = self
                .await_accepted(
                    interaction.clone(),
                    cancellation,
                    context.operation_permit.as_ref(),
                )
                .await?;
            let content = response
                .as_object()
                .cloned()
                .ok_or(McpInteractionHandlerError::Invalid)?
                .into_iter()
                .collect();
            input_responses.insert(
                request_key,
                InputResponse::Elicitation(ElicitResult {
                    action: ElicitAction::Accept,
                    content: Some(content),
                }),
            );
            interaction_ids.push(interaction.interaction_id().as_str().to_owned());
        }
        Ok(McpInteractionResolution {
            input_responses,
            request_state: required.request_state,
            interaction_ids,
        })
    }

    async fn complete(
        &self,
        _context: &McpInteractionContext,
        interaction_ids: &[String],
        succeeded: bool,
    ) -> Result<(), McpInteractionHandlerError> {
        for interaction_id in interaction_ids {
            self.close_interaction(interaction_id, succeeded).await?;
        }
        Ok(())
    }
}

fn safe_interaction_request(
    request: &InputRequest,
    request_state: Option<&str>,
) -> Result<(McpInteractionRequest, Value), McpInteractionHandlerError> {
    match request {
        InputRequest::ElicitationCreate(ElicitRequestParams::Form {
            message,
            requested_schema,
        }) => Ok((
            McpInteractionRequest::Form {
                message: message.clone(),
                requested_schema: requested_schema.clone(),
            },
            json!({"request_state": request_state}),
        )),
        InputRequest::ElicitationCreate(ElicitRequestParams::Url { message, url }) => {
            let parsed =
                reqwest::Url::parse(url).map_err(|_| McpInteractionHandlerError::Invalid)?;
            if parsed.scheme() != "https"
                || !parsed.username().is_empty()
                || parsed.password().is_some()
                || parsed.host_str().is_none()
                || parsed.fragment().is_some()
            {
                return Err(McpInteractionHandlerError::Invalid);
            }
            Ok((
                McpInteractionRequest::Url {
                    message: message.clone(),
                    scheme: "https".to_owned(),
                    host: parsed
                        .host_str()
                        .ok_or(McpInteractionHandlerError::Invalid)?
                        .to_owned(),
                    port: parsed.port(),
                },
                json!({"request_state": request_state, "url": url}),
            ))
        }
    }
}

fn deterministic_interaction_id(
    context: &McpInteractionContext,
    request_key: &str,
    request: &McpInteractionRequest,
) -> Result<McpInteractionId, McpInteractionHandlerError> {
    let canonical = serde_jcs::to_vec(&json!({
        "run_id": context.run_id,
        "operation_id": context.operation_id,
        "server_id": context.server_id,
        "binding_hash": context.binding_hash,
        "logical_request_key": context.logical_request_key,
        "request_key": request_key,
        "generation": context.generation,
        "request": request,
    }))
    .map_err(|_| McpInteractionHandlerError::Invalid)?;
    let digest = Sha256::digest(canonical);
    let mut encoded = String::from("mcp_");
    for byte in digest {
        write!(&mut encoded, "{byte:02x}").expect("writing to String cannot fail");
    }
    McpInteractionId::new(encoded).map_err(|_| McpInteractionHandlerError::Invalid)
}

/// Resolve service-account MCP catalogs at publication time and register only
/// explicitly imported tools. Runtime execution reuses these frozen bindings.
pub async fn load_mcp_action_providers(
    config: &McpClientConfig,
    allow_legacy_fallback: bool,
    registry: &mut ActionRegistry,
    retrievals: &mut RetrievalRegistry,
    interaction_handler: Option<Arc<dyn McpInteractionHandler>>,
    oauth_runtime: Option<McpOAuthRuntimeAuthority>,
    remote_task_runtime: Option<McpRemoteTaskRuntimeAuthority>,
) -> Result<McpLoadedProviders, McpResourceConfigError> {
    if !config.enabled {
        return Ok(McpLoadedProviders {
            publications: Vec::new(),
            contexts: BTreeMap::new(),
            subscriptions: Vec::new(),
            readiness: Arc::new(McpClientReadinessProbe::default()),
        });
    }
    let mut publications = Vec::with_capacity(config.servers.len());
    let mut contexts = BTreeMap::new();
    let mut subscriptions = Vec::new();
    let mut readiness_clients = Vec::new();
    for (server_id, server) in &config.servers {
        let (transport, transport_kind, http_policy) = match &server.transport {
            McpClientTransportConfig::StreamableHttp {
                endpoint,
                allow_plaintext_loopback,
                connect_timeout,
                request_timeout,
            } => {
                let policy = if *allow_plaintext_loopback {
                    StreamableHttpPolicy::for_loopback_test(endpoint)
                } else {
                    StreamableHttpPolicy::new(endpoint)
                }
                .map_err(|_| McpResourceConfigError::Transport)?
                .with_timeouts(*connect_timeout, *request_timeout)
                .map_err(|_| McpResourceConfigError::Transport)?
                .with_server_id(server_id)
                .map_err(|_| McpResourceConfigError::Transport)?
                .with_request_limit(server.limits.max_request_bytes)
                .map_err(|_| McpResourceConfigError::Transport)?
                .with_limits(
                    server.limits.max_response_bytes,
                    server.limits.max_sse_line_bytes,
                    server.limits.max_sse_event_bytes,
                )
                .map_err(|_| McpResourceConfigError::Transport)?;
                let credential = match &server.authorization {
                    McpClientAuthorizationConfig::None => None,
                    McpClientAuthorizationConfig::BearerEnv { token, .. } => Some(
                        HttpCredential::bearer(token.expose())
                            .map_err(|_| McpResourceConfigError::Credential)?,
                    ),
                    McpClientAuthorizationConfig::OauthUser { .. } => None,
                };
                let transport = StreamableHttpTransport::new(policy.clone(), credential)
                    .map_err(|_| McpResourceConfigError::Transport)?;
                (
                    Arc::new(transport) as Arc<dyn insight_mcp::McpTransport>,
                    McpTransportKind::StreamableHttp,
                    Some(policy),
                )
            }
            McpClientTransportConfig::Stdio {
                executable,
                args,
                working_directory,
                environment,
                isolation_profile: _,
                startup_timeout,
                request_timeout,
                shutdown_timeout,
            } => {
                let environment = environment
                    .iter()
                    .map(|(name, source)| {
                        std::env::var(source)
                            .map(|value| (name.clone(), value))
                            .map_err(|_| McpResourceConfigError::Credential)
                    })
                    .collect::<Result<BTreeMap<_, _>, _>>()?;
                let policy = StdioTransportPolicy::new(
                    executable.clone(),
                    args.clone(),
                    working_directory.clone(),
                    environment,
                )
                .and_then(|policy| {
                    policy.with_timeouts(*startup_timeout, *request_timeout, *shutdown_timeout)
                })
                .and_then(|policy| policy.with_server_id(server_id))
                .and_then(|policy| {
                    policy.with_limits(
                        server.limits.max_request_bytes,
                        server.limits.max_response_bytes,
                        server.limits.max_sse_line_bytes,
                    )
                })
                .map_err(|_| McpResourceConfigError::Transport)?;
                let transport =
                    StdioTransport::new(policy).map_err(|_| McpResourceConfigError::Transport)?;
                (
                    Arc::new(transport) as Arc<dyn insight_mcp::McpTransport>,
                    McpTransportKind::Stdio,
                    None,
                )
            }
        };
        let mut limits = McpClientLimits::default();
        limits.max_tools = server.limits.max_catalog_items;
        limits.max_resources = server.limits.max_catalog_items;
        limits.max_prompts = server.limits.max_catalog_items;
        limits.max_content_items = server.limits.max_content_items;
        limits.max_content_bytes = server.limits.max_response_bytes;
        let tasks_requested = server
            .imports
            .tools
            .iter()
            .any(|tool| tool.tasks == McpInteractionConfig::Allowed);
        let discovery_transport = match &server.discovery {
            McpDiscoveryConfig::LiveServiceAccount => Arc::clone(&transport),
            McpDiscoveryConfig::SignedManifest {
                path, trust_keys, ..
            } => {
                signed_manifest_transport(server_id, path, trust_keys.expose(), transport.kind())
                    .await?
            }
        };
        let max_message_bytes = server
            .limits
            .max_request_bytes
            .max(server.limits.max_response_bytes);
        let modern_discovery_client = build_mcp_client(
            Arc::clone(&discovery_transport),
            limits,
            max_message_bytes,
            tasks_requested,
        )?;
        let cancellation = CancellationToken::new();
        let modern_discovery = modern_discovery_client
            .discover(&cancellation, &NoopNotificationObserver)
            .await;
        let (selected_protocol, discovery_client, client, discovery) = match modern_discovery {
            Ok(discovery) => (
                MCP_PROTOCOL_VERSION,
                modern_discovery_client,
                build_mcp_client(
                    Arc::clone(&transport),
                    limits,
                    max_message_bytes,
                    tasks_requested,
                )?,
                discovery,
            ),
            Err(error)
                if allow_legacy_fallback
                    && matches!(server.discovery, McpDiscoveryConfig::LiveServiceAccount)
                    && legacy_fallback_allowed(&error) =>
            {
                if tasks_requested {
                    return Err(McpResourceConfigError::Capability);
                }
                // A failed modern stdio probe must not leave a half-selected
                // process behind. HTTP modern is stateless and has no legacy
                // session at this point; shutdown remains a no-op there.
                transport
                    .shutdown()
                    .await
                    .map_err(|_| McpResourceConfigError::Transport)?;
                let legacy_client = build_mcp_client_for_protocol(
                    Arc::clone(&transport),
                    limits,
                    max_message_bytes,
                    false,
                    MCP_LEGACY_PROTOCOL_VERSION,
                )?;
                let discovery = legacy_client
                    .discover(&cancellation, &NoopNotificationObserver)
                    .await
                    .map_err(|_| McpResourceConfigError::Discovery)?;
                (
                    MCP_LEGACY_PROTOCOL_VERSION,
                    Arc::clone(&legacy_client),
                    legacy_client,
                    discovery,
                )
            }
            Err(_) => return Err(McpResourceConfigError::Discovery),
        };
        let tasks_discovered = discovery
            .capabilities
            .extensions
            .as_ref()
            .and_then(|extensions| extensions.get(insight_mcp::MCP_TASKS_EXTENSION_ID))
            .is_some();
        if tasks_requested && !tasks_discovered {
            return Err(McpResourceConfigError::Capability);
        }
        let discovery_fingerprint = canonical_sha256(&discovery)?;
        let binding = McpServerBindingIdentity {
            connection_id: format!("mcp.{server_id}"),
            server_id: server_id.clone(),
            protocol_version: selected_protocol.to_owned(),
            transport: transport_kind,
            principal_scope: if matches!(
                server.authorization,
                McpClientAuthorizationConfig::OauthUser { .. }
            ) {
                PrincipalScope::User("oauth_user".to_owned())
            } else {
                PrincipalScope::Service
            },
            discovery_fingerprint: discovery_fingerprint.clone(),
        };
        let (tool_catalog_fingerprint, tool_bindings) = if server.imports.tools.is_empty() {
            (None, Vec::new())
        } else {
            if discovery.capabilities.tools.is_none() {
                return Err(McpResourceConfigError::Capability);
            }
            let catalog = discovery_client
                .list_tools(&cancellation, &NoopNotificationObserver)
                .await
                .map_err(|_| McpResourceConfigError::Catalog)?;
            let imports = server
                .imports
                .tools
                .iter()
                .map(|tool| resolve_tool_import(server_id, tool))
                .collect::<Result<Vec<_>, _>>()?;
            let mut provider = McpToolProvider::freeze(
                binding.clone(),
                Arc::clone(&client),
                catalog.clone(),
                imports,
            )
            .map_err(|_| McpResourceConfigError::Provider)?;
            if let Some(handler) = interaction_handler.as_ref() {
                provider = provider.with_interaction_handler(Arc::clone(handler));
            }
            if tasks_requested {
                let handler = remote_task_runtime
                    .as_ref()
                    .cloned()
                    .ok_or(McpResourceConfigError::Capability)?;
                provider = provider.with_remote_task_handler(Arc::new(handler));
            }
            if let McpClientAuthorizationConfig::OauthUser {
                scopes,
                client_id,
                redirect_uri,
            } = &server.authorization
            {
                let authority = oauth_runtime
                    .as_ref()
                    .cloned()
                    .ok_or(McpResourceConfigError::Credential)?;
                let policy = http_policy
                    .clone()
                    .ok_or(McpResourceConfigError::Credential)?;
                provider = provider.with_runtime_client_resolver(Arc::new(
                    DurableOAuthRuntimeClientResolver {
                        authority,
                        server_id: server_id.clone(),
                        resource: policy.endpoint().to_string(),
                        client_id: client_id.clone(),
                        redirect_uri: redirect_uri.clone(),
                        required_scopes: scopes.clone(),
                        policy,
                        limits,
                        max_message_bytes: server
                            .limits
                            .max_request_bytes
                            .max(server.limits.max_response_bytes),
                        allow_tasks: tasks_requested,
                        protocol_version: selected_protocol.to_owned(),
                    },
                ));
            }
            let tool_bindings = provider
                .imports()
                .iter()
                .map(|imported| imported.binding.clone())
                .collect();
            provider
                .register_into(registry)
                .map_err(|_| McpResourceConfigError::Provider)?;
            (Some(catalog.descriptor_hash), tool_bindings)
        };

        let mut catalog_invalidation = None;
        if !server.imports.tools.is_empty()
            || !server.imports.resources.is_empty()
            || !server.imports.prompts.is_empty()
        {
            let shared_invalidation = Arc::new(McpCatalogInvalidation::default());
            if !server.imports.resources.is_empty() && discovery.capabilities.resources.is_none() {
                return Err(McpResourceConfigError::Capability);
            }
            if !server.imports.prompts.is_empty() && discovery.capabilities.prompts.is_none() {
                return Err(McpResourceConfigError::Capability);
            }
            let resource_catalog = if server.imports.resources.is_empty() {
                ResourceCatalog::empty("resources").map_err(|_| McpResourceConfigError::Catalog)?
            } else {
                discovery_client
                    .list_resources(&cancellation, &NoopNotificationObserver)
                    .await
                    .map_err(|_| McpResourceConfigError::Catalog)?
            };
            let template_catalog = if server.imports.resources.is_empty() {
                ResourceTemplateCatalog::empty("resource_templates")
                    .map_err(|_| McpResourceConfigError::Catalog)?
            } else {
                discovery_client
                    .list_resource_templates(&cancellation, &NoopNotificationObserver)
                    .await
                    .map_err(|_| McpResourceConfigError::Catalog)?
            };
            let prompt_catalog = if server.imports.prompts.is_empty() {
                PromptCatalog::empty("prompts").map_err(|_| McpResourceConfigError::Catalog)?
            } else {
                discovery_client
                    .list_prompts(&cancellation, &NoopNotificationObserver)
                    .await
                    .map_err(|_| McpResourceConfigError::Catalog)?
            };
            let mut context = McpContextProvider::freeze(
                binding.clone(),
                Arc::clone(&client),
                resource_catalog,
                template_catalog,
                prompt_catalog,
                server
                    .imports
                    .resources
                    .iter()
                    .map(|pattern| McpResourceImportPolicy {
                        uri_pattern: pattern.clone(),
                        mime_allowlist: Vec::new(),
                        max_content_bytes: server.limits.max_response_bytes,
                    })
                    .collect(),
                server
                    .imports
                    .prompts
                    .iter()
                    .map(|prompt| McpPromptImportPolicy {
                        remote_name: prompt.remote_name.clone(),
                        allow_user_invocation: prompt.allow_user_invocation,
                        allow_definition_snapshot: prompt.definition_arguments.is_some(),
                        definition_arguments: prompt.definition_arguments.clone(),
                    })
                    .collect(),
            )
            .map_err(|_| McpResourceConfigError::Context)?;
            context = context
                .freeze_definition_prompts(&cancellation, &NoopNotificationObserver)
                .await
                .map_err(|_| McpResourceConfigError::Context)?;
            context = context.with_catalog_invalidation(Arc::clone(&shared_invalidation));
            if let McpClientAuthorizationConfig::OauthUser {
                scopes,
                client_id,
                redirect_uri,
            } = &server.authorization
            {
                let authority = oauth_runtime
                    .as_ref()
                    .cloned()
                    .ok_or(McpResourceConfigError::Credential)?;
                let policy = http_policy
                    .clone()
                    .ok_or(McpResourceConfigError::Credential)?;
                context = context.with_principal_client_resolver(Arc::new(
                    DurableOAuthRuntimeClientResolver {
                        authority,
                        server_id: server_id.clone(),
                        resource: policy.endpoint().to_string(),
                        client_id: client_id.clone(),
                        redirect_uri: redirect_uri.clone(),
                        required_scopes: scopes.clone(),
                        policy,
                        limits,
                        max_message_bytes: server
                            .limits
                            .max_request_bytes
                            .max(server.limits.max_response_bytes),
                        allow_tasks: tasks_requested,
                        protocol_version: selected_protocol.to_owned(),
                    },
                ));
            }
            let context = Arc::new(context);
            for imported in context.resources().iter().cloned() {
                retrievals
                    .register(McpResourceRetrieval::new(
                        imported,
                        Arc::clone(&context),
                        oauth_runtime.as_ref().map(|authority| {
                            Arc::clone(&authority.interaction_repository)
                                as Arc<dyn McpInteractionDurableRepository>
                        }),
                        interaction_handler.as_ref().map(Arc::clone),
                    ))
                    .map_err(|_| McpResourceConfigError::Context)?;
            }
            if !matches!(
                server.authorization,
                McpClientAuthorizationConfig::OauthUser { .. }
            ) {
                let filter = subscription_filter(
                    &discovery.capabilities,
                    context.resources(),
                    !server.imports.tools.is_empty(),
                    !server.imports.prompts.is_empty(),
                );
                if subscription_filter_is_nonempty(&filter) {
                    subscriptions.push(McpClientSubscription {
                        server_id: server_id.clone(),
                        context: Arc::clone(&context),
                        filter,
                        invalidation: Arc::clone(&shared_invalidation),
                    });
                    catalog_invalidation = Some(shared_invalidation);
                }
            }
            contexts.insert(server_id.clone(), context);
        }
        if !server.imports.tools.is_empty()
            || !server.imports.resources.is_empty()
            || !server.imports.prompts.is_empty()
        {
            readiness_clients.push(match (&server.authorization, http_policy.as_ref()) {
                (
                    McpClientAuthorizationConfig::OauthUser {
                        scopes,
                        client_id,
                        redirect_uri,
                    },
                    Some(policy),
                ) => RequiredMcpReadiness::OAuthUser {
                    client: OAuthClient::new(policy.request_timeout())
                        .map_err(|_| McpResourceConfigError::Credential)?,
                    resource: policy.endpoint().to_string(),
                    registration: OAuthClientRegistration::new(
                        client_id,
                        redirect_uri,
                        scopes.iter().cloned().collect(),
                    )
                    .map_err(|_| McpResourceConfigError::Credential)?,
                },
                (McpClientAuthorizationConfig::OauthUser { .. }, None) => {
                    return Err(McpResourceConfigError::Credential);
                }
                (
                    McpClientAuthorizationConfig::None
                    | McpClientAuthorizationConfig::BearerEnv { .. },
                    _,
                ) => RequiredMcpReadiness::Service {
                    client: Arc::clone(&client),
                    capabilities: Box::new(discovery.capabilities.clone()),
                },
            });
        }
        publications.push(McpClientPublication {
            server_id: server_id.clone(),
            binding,
            capabilities: discovery.capabilities,
            server_info: discovery.server_info,
            required_scopes: match &server.authorization {
                McpClientAuthorizationConfig::OauthUser { scopes, .. } => scopes.clone(),
                McpClientAuthorizationConfig::None
                | McpClientAuthorizationConfig::BearerEnv { .. } => BTreeSet::new(),
            },
            discovery_fingerprint,
            tool_catalog_fingerprint,
            tool_bindings,
            catalog_invalidation,
        });
    }
    Ok(McpLoadedProviders {
        publications,
        contexts,
        subscriptions,
        readiness: Arc::new(McpClientReadinessProbe {
            required: readiness_clients,
        }),
    })
}

fn subscription_filter(
    capabilities: &insight_mcp::ServerCapabilities,
    resources: &[McpImportedResource],
    tools_imported: bool,
    prompts_imported: bool,
) -> SubscriptionFilter {
    let tools_list_changed = (tools_imported
        && capability_flag(capabilities.tools.as_ref(), "listChanged"))
    .then_some(true);
    let resources_list_changed = (!resources.is_empty()
        && capability_flag(capabilities.resources.as_ref(), "listChanged"))
    .then_some(true);
    let prompts_list_changed = (prompts_imported
        && capability_flag(capabilities.prompts.as_ref(), "listChanged"))
    .then_some(true);
    let resource_subscriptions = if capability_flag(capabilities.resources.as_ref(), "subscribe") {
        resources
            .iter()
            .filter(|resource| {
                resource.binding.kind == insight_mcp::McpResourceBindingKind::Resource
            })
            .map(|resource| resource.binding.remote_uri.clone())
            .collect()
    } else {
        Vec::new()
    };
    SubscriptionFilter {
        tools_list_changed,
        resources_list_changed,
        prompts_list_changed,
        resource_subscriptions,
        task_ids: Vec::new(),
    }
}

fn remote_task_operational_gauge(status: McpRemoteTaskStatus) -> insight_mcp::McpOperationalGauge {
    match status {
        McpRemoteTaskStatus::Working => insight_mcp::McpOperationalGauge::RemoteTasksWorking,
        McpRemoteTaskStatus::InputRequired => {
            insight_mcp::McpOperationalGauge::RemoteTasksInputRequired
        }
        McpRemoteTaskStatus::Completed
        | McpRemoteTaskStatus::Failed
        | McpRemoteTaskStatus::Cancelled
        | McpRemoteTaskStatus::Expired => insight_mcp::McpOperationalGauge::RemoteTasksTerminal,
    }
}

fn legacy_fallback_allowed(error: &ClientError) -> bool {
    matches!(
        error,
        ClientError::Protocol(-32601)
            | ClientError::Transport(TransportError::Remote { code: -32601, .. })
            | ClientError::Transport(TransportError::Timeout)
    )
}

fn capability_flag(capability: Option<&Value>, name: &str) -> bool {
    capability
        .and_then(Value::as_object)
        .and_then(|capability| capability.get(name))
        .and_then(Value::as_bool)
        == Some(true)
}

fn subscription_filter_is_nonempty(filter: &SubscriptionFilter) -> bool {
    filter.tools_list_changed == Some(true)
        || filter.resources_list_changed == Some(true)
        || filter.prompts_list_changed == Some(true)
        || !filter.resource_subscriptions.is_empty()
}

fn resolve_tool_import(
    server_id: &str,
    tool: &McpToolImportConfig,
) -> Result<McpToolImport, McpResourceConfigError> {
    let description = if let Some(value) = &tool.description_override {
        McpDescriptionPolicy::Override(value.clone())
    } else {
        match tool.description_source {
            McpDescriptionSourceConfig::Remote => McpDescriptionPolicy::Remote,
            McpDescriptionSourceConfig::Disabled => McpDescriptionPolicy::Disabled,
        }
    };
    let effect = match tool.effect {
        McpEffectConfig::Pure => EffectClass::Pure,
        McpEffectConfig::ReadOnly => EffectClass::ReadOnly,
        McpEffectConfig::Mutating => EffectClass::Mutating,
    };
    let idempotency = match tool.idempotency {
        McpIdempotencyConfig::Idempotent => IdempotencyClass::Idempotent,
        McpIdempotencyConfig::NonIdempotent | McpIdempotencyConfig::Unknown => {
            IdempotencyClass::NonIdempotent
        }
    };
    let cancellation = match tool.cancellation {
        McpCancellationConfig::NotSupported => CancellationClass::NotSupported,
        McpCancellationConfig::Cooperative | McpCancellationConfig::Immediate => {
            CancellationClass::Cooperative
        }
    };
    let approval = match tool.approval {
        McpApprovalConfig::Never => McpApprovalPolicy::Never,
        McpApprovalConfig::ModelToolOnly => McpApprovalPolicy::ModelToolOnly,
        McpApprovalConfig::Mutating => McpApprovalPolicy::Mutating,
        McpApprovalConfig::Always => McpApprovalPolicy::Always,
    };
    let mut public_policy = ToolPublicPolicy::private();
    public_policy.call = tool.public_call;
    let mut required_capabilities = tool
        .required_capabilities
        .iter()
        .cloned()
        .map(ActionCapability::new)
        .collect::<BTreeSet<_>>();
    if !tool.terminal_only_compatible {
        required_capabilities.insert(ActionCapability::new("runtime.mcp.full_persistence.v1"));
    }
    if tool.input_required == McpInteractionConfig::Allowed
        || tool.approval != McpApprovalConfig::Never
    {
        required_capabilities.insert(ActionCapability::new("runtime.mcp.interaction.v1"));
    }
    if tool.tasks == McpInteractionConfig::Allowed {
        required_capabilities.insert(ActionCapability::new("runtime.mcp.tasks.v1"));
    }
    Ok(McpToolImport {
        remote_name: tool.remote_name.clone(),
        action_id: action_id(server_id, &tool.alias),
        action_version: "0.0.0+mcp".to_owned(),
        model_tool_name: tool.alias.clone(),
        title: tool.title.clone(),
        description,
        effect,
        idempotency,
        cancellation,
        required_capabilities,
        approval,
        allow_input_required: tool.input_required == McpInteractionConfig::Allowed,
        allow_tasks: tool.tasks == McpInteractionConfig::Allowed,
        terminal_only_compatible: tool.terminal_only_compatible,
        public_policy,
    })
}

fn action_id(server_id: &str, alias: &str) -> String {
    let normalized = alias.replace('.', "_");
    let candidate = format!("mcp.{server_id}.tool_{normalized}");
    if candidate.len() <= 128 {
        return candidate;
    }
    let digest = Sha256::digest(candidate.as_bytes());
    let mut suffix = String::with_capacity(16);
    for byte in &digest[..8] {
        write!(&mut suffix, "{byte:02x}").expect("writing to String cannot fail");
    }
    let prefix_bytes = 128usize.saturating_sub(suffix.len() + 1);
    format!("{}_{}", &candidate[..prefix_bytes], suffix)
}

fn remote_task_local_id(
    context: &McpInteractionContext,
    binding: &McpToolBinding,
) -> Result<String, McpRemoteTaskHandlerError> {
    let canonical = serde_jcs::to_vec(&json!({
        "run_id": context.run_id,
        "operation_id": context.operation_id,
        "logical_request_key": context.logical_request_key,
        "server_id": context.server_id,
        "binding_hash": binding.descriptor_hash
    }))
    .map_err(|_| McpRemoteTaskHandlerError::Invalid)?;
    Ok(format!("mcp_task_{}", canonical_sha256_bytes(&canonical)))
}

const fn status_wire(status: McpRemoteTaskStatus) -> &'static str {
    match status {
        McpRemoteTaskStatus::Working => "working",
        McpRemoteTaskStatus::InputRequired => "input_required",
        McpRemoteTaskStatus::Completed => "completed",
        McpRemoteTaskStatus::Failed => "failed",
        McpRemoteTaskStatus::Cancelled => "cancelled",
        McpRemoteTaskStatus::Expired => "expired",
    }
}

fn canonical_sha256(value: &impl Serialize) -> Result<String, McpResourceConfigError> {
    let canonical =
        serde_jcs::to_vec(value).map_err(|_| McpResourceConfigError::DiscoveryEvidence)?;
    Ok(canonical_sha256_bytes(&canonical))
}

fn canonical_sha256_bytes(value: &[u8]) -> String {
    let digest = Sha256::digest(value);
    let mut encoded = String::with_capacity(64);
    for byte in digest {
        write!(&mut encoded, "{byte:02x}").expect("writing to String cannot fail");
    }
    encoded
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum McpResourceConfigError {
    Transport,
    UnsupportedTransport,
    Credential,
    InteractiveAuthorization,
    ManifestRequired,
    ManifestInvalid,
    Client,
    Discovery,
    DiscoveryEvidence,
    Capability,
    Catalog,
    Policy,
    Provider,
    Context,
}

impl std::fmt::Display for McpResourceConfigError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(match self {
            Self::Transport => "invalid MCP transport configuration",
            Self::UnsupportedTransport => "configured MCP transport is not available",
            Self::Credential => "invalid MCP service credential",
            Self::InteractiveAuthorization => {
                "user OAuth MCP authorization is unavailable during publication"
            }
            Self::ManifestRequired => "configured MCP signed manifest is unavailable",
            Self::ManifestInvalid => "configured MCP signed manifest is invalid",
            Self::Client => "invalid MCP client configuration",
            Self::Discovery => "MCP server discovery failed",
            Self::DiscoveryEvidence => "MCP discovery evidence could not be frozen",
            Self::Capability => "MCP server does not advertise an imported capability",
            Self::Catalog => "MCP server catalog discovery failed",
            Self::Policy => "MCP import policy is invalid",
            Self::Provider => "MCP tool provider publication failed",
            Self::Context => "MCP resource or prompt provider publication failed",
        })
    }
}

impl std::error::Error for McpResourceConfigError {}

#[cfg(test)]
mod tests {
    use std::fs;

    use base64::{engine::general_purpose::STANDARD as BASE64_STANDARD, Engine as _};
    use ring::{
        rand::SystemRandom,
        signature::{Ed25519KeyPair, KeyPair},
    };
    use serde_json::{json, Value};
    use tempfile::tempdir;

    use super::*;

    fn signed_manifest(server_id: &str) -> (Value, String) {
        let generated_at = Utc::now() - ChronoDuration::minutes(1);
        let expires_at = generated_at + ChronoDuration::hours(1);
        let payload = json!({
            "schema_version": 1,
            "server_id": server_id,
            "protocol_version": MCP_PROTOCOL_VERSION,
            "generated_at": generated_at,
            "expires_at": expires_at,
            "discover": {
                "resultType": "complete",
                "supportedVersions": [MCP_PROTOCOL_VERSION],
                "capabilities": {"tools": {}},
                "serverInfo": {"name": "manifest-fixture", "version": "1.0.0"},
                "ttlMs": 1000,
                "cacheScope": "private"
            },
            "tools": [{
                "resultType": "complete",
                "tools": [{"name": "search", "inputSchema": {"type": "object"}}],
                "ttlMs": 1000,
                "cacheScope": "private"
            }],
            "resources": [],
            "resource_templates": [],
            "prompts": [],
            "signer_key_id": "release"
        });
        let canonical = serde_jcs::to_vec(&payload).unwrap();
        let key_document = Ed25519KeyPair::generate_pkcs8(&SystemRandom::new()).unwrap();
        let key_pair = Ed25519KeyPair::from_pkcs8(key_document.as_ref()).unwrap();
        let mut manifest = payload;
        manifest["content_hash"] = json!(canonical_sha256_bytes(&canonical));
        manifest["signature"] = json!(BASE64_STANDARD.encode(key_pair.sign(&canonical).as_ref()));
        let trust_keys = json!({
            "release": BASE64_STANDARD.encode(key_pair.public_key().as_ref())
        })
        .to_string();
        (manifest, trust_keys)
    }

    #[tokio::test]
    async fn signed_manifest_is_verified_and_served_as_frozen_discovery() {
        let directory = tempdir().unwrap();
        let path = directory.path().join("manifest.json");
        let (manifest, trust_keys) = signed_manifest("engineering");
        fs::write(&path, serde_json::to_vec(&manifest).unwrap()).unwrap();

        let transport = signed_manifest_transport(
            "engineering",
            &path,
            &trust_keys,
            TransportKind::StreamableHttp,
        )
        .await
        .unwrap();
        let client =
            build_mcp_client(transport, McpClientLimits::default(), 1024 * 1024, false).unwrap();
        let cancellation = CancellationToken::new();
        let discovery = client
            .discover(&cancellation, &NoopNotificationObserver)
            .await
            .unwrap();
        let tools = client
            .list_tools(&cancellation, &NoopNotificationObserver)
            .await
            .unwrap();

        assert_eq!(discovery.server_info.name, "manifest-fixture");
        assert_eq!(tools.tools.len(), 1);
        assert_eq!(tools.tools[0].name, "search");
    }

    #[tokio::test]
    async fn signed_manifest_tampering_and_wrong_server_fail_closed() {
        let directory = tempdir().unwrap();
        let path = directory.path().join("manifest.json");
        let (mut manifest, trust_keys) = signed_manifest("engineering");
        manifest["discover"]["serverInfo"]["name"] = json!("tampered");
        fs::write(&path, serde_json::to_vec(&manifest).unwrap()).unwrap();

        assert!(matches!(
            signed_manifest_transport(
                "engineering",
                &path,
                &trust_keys,
                TransportKind::StreamableHttp
            )
            .await,
            Err(McpResourceConfigError::ManifestInvalid)
        ));

        let (manifest, trust_keys) = signed_manifest("engineering");
        fs::write(&path, serde_json::to_vec(&manifest).unwrap()).unwrap();
        assert!(matches!(
            signed_manifest_transport("other", &path, &trust_keys, TransportKind::StreamableHttp)
                .await,
            Err(McpResourceConfigError::ManifestInvalid)
        ));
    }

    #[test]
    fn subscription_filter_only_enables_advertised_imported_capabilities() {
        let capabilities = insight_mcp::ServerCapabilities {
            tools: Some(json!({"listChanged": true})),
            resources: Some(json!({"listChanged": true, "subscribe": true})),
            prompts: Some(json!({"listChanged": false})),
            ..Default::default()
        };

        let filter = subscription_filter(&capabilities, &[], true, true);

        assert_eq!(filter.tools_list_changed, Some(true));
        assert_eq!(filter.resources_list_changed, None);
        assert_eq!(filter.prompts_list_changed, None);
        assert!(filter.resource_subscriptions.is_empty());
        assert!(subscription_filter_is_nonempty(&filter));

        let none =
            subscription_filter(&insight_mcp::ServerCapabilities::default(), &[], true, true);
        assert!(!subscription_filter_is_nonempty(&none));
    }
}
