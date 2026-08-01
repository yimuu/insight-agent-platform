use std::{
    collections::{BTreeMap, BTreeSet},
    fmt::Write as _,
    sync::Arc,
    time::Duration,
};

use async_trait::async_trait;
use base64::{engine::general_purpose::STANDARD as BASE64_STANDARD, Engine as _};
use chrono::{Duration as ChronoDuration, Utc};
use insight_durable::{
    ClaimMcpOAuthRefreshCommand, ClaimMcpRemoteTasksCommand, CreateMcpInteractionCommand,
    CreateMcpRemoteTaskCommand, FinalizeMcpRemoteTaskCommand, McpInteraction,
    McpInteractionDurableRepository, McpInteractionId, McpInteractionOutcome,
    McpInteractionPrincipal, McpInteractionRequest, McpInteractionState, McpManagedServer,
    McpOAuthDurableRepository, McpProtectedSecret, McpRemoteTaskDurableRepository, McpRemoteTaskId,
    McpRemoteTaskStatus, McpSecretProtector, McpSecretPurpose, McpSecretScope, McpServerRevision,
    ObserveMcpRemoteTaskCommand, StoreMcpOAuthCredentialCommand, TransitionMcpInteractionCommand,
};
use insight_engine::{execution::RunError, worker::WorkerArtifactPayload};
use insight_mcp::{
    ClientCapabilities, ClientInfo, ElicitAction, ElicitRequestParams, ElicitResult, GetTaskResult,
    HttpCredential, InputRequest, InputRequiredResult, InputResponse, LegacyCompatibilityTransport,
    McpClient, McpClientLimits, McpServerBindingIdentity, McpToolBinding, McpTransport,
    McpTransportKind, MetaMap, NoopNotificationObserver, OAuthClient, OAuthClientRegistration,
    PrincipalScope, PromptCatalog, ProtocolLimits, ReadResourceResult, ResourceCatalog,
    ResourceContents, ResourceTemplateCatalog, StdioTransport, StdioTransportPolicy,
    StreamableHttpPolicy, StreamableHttpTransport, SubscriptionFilter, MCP_LEGACY_PROTOCOL_VERSION,
    MCP_PROTOCOL_VERSION,
};
use insight_resources::actions::{
    ActionCapability, ActionRegistry, CancellationClass, EffectClass, IdempotencyClass,
    ToolPublicPolicy,
};
use insight_resources::retrievals::{
    Retrieval, RetrievalContext, RetrievalDescriptor, RetrievalExecutionResult, RetrievalRegistry,
};
use serde::Serialize;
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use tokio_util::sync::CancellationToken;

use crate::config::McpClientConfig;
use insight_api::v1::{
    McpCatalogServer, McpDraftAuthorization, McpDraftDocument, McpDraftTransport, McpOAuthServer,
    McpToolDescriptionPolicy,
};

pub use insight_resources::mcp::{
    McpApprovalPolicy, McpDescriptionPolicy, McpExecutionFence, McpImportedTool,
    McpInteractionContext, McpInteractionHandler, McpInteractionHandlerError,
    McpInteractionResolution, McpProviderError, McpRecoveredRemoteTask, McpRemoteTaskHandle,
    McpRemoteTaskHandler, McpRemoteTaskHandlerError, McpRemoteTaskLocalTerminal,
    McpRemoteTaskObservation, McpRemoteTaskPollAuthority, McpRuntimeClientResolver,
    McpRuntimeClientResolverError, McpToolImport, McpToolProvider,
};
pub use insight_resources::mcp_context::{
    McpCatalogInvalidation, McpCatalogInvalidationState, McpContextError, McpContextOutcome,
    McpContextProvider, McpImportedPrompt, McpImportedResource, McpImportedResourceDescriptor,
    McpPrincipalClientResolver, McpPrincipalClientResolverError, McpPromptImportPolicy,
    McpPromptSnapshot, McpResourceImportPolicy, McpResourceSnapshot,
};

#[derive(Clone)]
pub struct McpClientSubscription {
    pub server_id: String,
    pub context: Arc<McpContextProvider>,
    pub filter: SubscriptionFilter,
    pub invalidation: Arc<McpCatalogInvalidation>,
}

#[derive(Clone)]
struct McpResourceRetrieval {
    id: String,
    version: String,
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
            version: "0.0.0+mcp".to_owned(),
            imported,
            context,
            principal_repository,
            interaction_handler,
        }
    }

    fn for_revision(
        imported: McpImportedResource,
        context: Arc<McpContextProvider>,
        principal_repository: Option<Arc<dyn McpInteractionDurableRepository>>,
        interaction_handler: Option<Arc<dyn McpInteractionHandler>>,
        revision_suffix: &str,
    ) -> Self {
        let mut retrieval = Self::new(imported, context, principal_repository, interaction_handler);
        retrieval.version = format!("0.0.0+mcp.{revision_suffix}");
        retrieval
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
            version: self.version.clone(),
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

/// Materializes one immutable active management Revision into the shared
/// Action Registry. No remote catalog read occurs: schemas and bindings come
/// only from the frozen revision document.
pub struct ManagedMcpRevisionProjection {
    pub action_identities: Vec<(String, String)>,
    pub retrieval_identities: Vec<(String, String)>,
    pub catalog_server: McpCatalogServer,
    pub oauth_server: Option<McpOAuthServer>,
    pub subscription: Option<McpClientSubscription>,
}

#[allow(clippy::too_many_arguments)]
pub async fn publish_managed_mcp_revision_actions(
    config: &McpClientConfig,
    server: &McpManagedServer,
    revision: &McpServerRevision,
    registry: &ActionRegistry,
    retrievals: &RetrievalRegistry,
    interaction_handler: Option<Arc<dyn McpInteractionHandler>>,
    oauth_runtime: Option<McpOAuthRuntimeAuthority>,
    remote_task_runtime: Option<McpRemoteTaskRuntimeAuthority>,
    execution_fence: Arc<dyn McpExecutionFence>,
) -> Result<ManagedMcpRevisionProjection, McpResourceConfigError> {
    let draft: McpDraftDocument = serde_json::from_value(
        revision
            .document
            .get("draft")
            .cloned()
            .ok_or(McpResourceConfigError::Provider)?,
    )
    .map_err(|_| McpResourceConfigError::Provider)?;
    let limits = &config.default_limits.limits;
    let request_limit = draft
        .limits
        .max_request_bytes
        .unwrap_or(limits.max_request_bytes);
    let response_limit = draft
        .limits
        .max_response_bytes
        .unwrap_or(limits.max_response_bytes);
    let line_limit = draft
        .limits
        .max_sse_line_bytes
        .unwrap_or(limits.max_sse_line_bytes);
    let event_limit = draft
        .limits
        .max_sse_event_bytes
        .unwrap_or(limits.max_sse_event_bytes);
    let content_limit = draft
        .limits
        .max_content_items
        .unwrap_or(limits.max_content_items);
    let catalog_limit = draft
        .limits
        .max_catalog_items
        .unwrap_or(limits.max_catalog_items);
    let (transport, transport_kind, http_policy) = match draft
        .transport
        .as_ref()
        .ok_or(McpResourceConfigError::Transport)?
    {
        McpDraftTransport::StreamableHttp { endpoint } => {
            let policy = if config.network_policy.allow_loopback_development
                && endpoint.starts_with("http://")
            {
                StreamableHttpPolicy::for_loopback_test(endpoint)
            } else {
                StreamableHttpPolicy::new(endpoint)
            }
            .map_err(|_| McpResourceConfigError::Transport)?
            .with_timeouts(
                config.default_limits.connect_timeout,
                config.default_limits.request_timeout,
            )
            .map_err(|_| McpResourceConfigError::Transport)?
            .with_server_id(&server.server_id)
            .map_err(|_| McpResourceConfigError::Transport)?
            .with_request_limit(request_limit)
            .map_err(|_| McpResourceConfigError::Transport)?
            .with_limits(response_limit, line_limit, event_limit)
            .map_err(|_| McpResourceConfigError::Transport)?;
            let credential = match draft
                .authorization
                .as_ref()
                .ok_or(McpResourceConfigError::Credential)?
            {
                McpDraftAuthorization::None | McpDraftAuthorization::OauthUser { .. } => None,
                McpDraftAuthorization::BearerSecretRef { secret_ref } => {
                    let token = std::env::var(secret_ref)
                        .map_err(|_| McpResourceConfigError::Credential)?;
                    Some(
                        HttpCredential::bearer(&token)
                            .map_err(|_| McpResourceConfigError::Credential)?,
                    )
                }
            };
            let transport = StreamableHttpTransport::new(policy.clone(), credential)
                .map_err(|_| McpResourceConfigError::Transport)?;
            (
                Arc::new(transport) as Arc<dyn McpTransport>,
                McpTransportKind::StreamableHttp,
                Some(policy),
            )
        }
        McpDraftTransport::Stdio {
            launch_profile_id,
            parameters,
        } => {
            let profile = config
                .stdio_launch_profiles
                .get(launch_profile_id)
                .ok_or(McpResourceConfigError::Transport)?;
            if parameters
                .keys()
                .any(|name| !profile.allowed_parameters.contains(name))
            {
                return Err(McpResourceConfigError::Transport);
            }
            let mut args = profile.fixed_args.clone();
            for (name, value) in parameters {
                args.push(format!("--{name}"));
                args.push(value.clone());
            }
            let environment = profile
                .secret_environment
                .iter()
                .map(|(name, source)| {
                    std::env::var(source)
                        .map(|value| (name.clone(), value))
                        .map_err(|_| McpResourceConfigError::Credential)
                })
                .collect::<Result<BTreeMap<_, _>, _>>()?;
            let policy = StdioTransportPolicy::new(
                profile.executable.clone(),
                args,
                profile.working_directory.clone(),
                environment,
            )
            .and_then(|policy| {
                policy.with_timeouts(
                    Duration::from_secs(10),
                    config.default_limits.request_timeout,
                    Duration::from_secs(10),
                )
            })
            .and_then(|policy| policy.with_limits(request_limit, response_limit, 64 * 1024))
            .and_then(|policy| policy.with_server_id(&server.server_id))
            .map_err(|_| McpResourceConfigError::Transport)?;
            let transport =
                StdioTransport::new(policy).map_err(|_| McpResourceConfigError::Transport)?;
            (
                Arc::new(transport) as Arc<dyn McpTransport>,
                McpTransportKind::Stdio,
                None,
            )
        }
    };
    let tool_documents = revision
        .document
        .pointer("/bindings/tools")
        .and_then(Value::as_array)
        .ok_or(McpResourceConfigError::Provider)?;
    let tasks_requested = tool_documents
        .iter()
        .any(|item| item.pointer("/import/tasks").and_then(Value::as_str) == Some("allowed"));
    let client_limits = McpClientLimits {
        max_tools: catalog_limit,
        max_resources: catalog_limit,
        max_prompts: catalog_limit,
        max_content_items: content_limit,
        max_content_bytes: response_limit,
        ..McpClientLimits::default()
    };
    let selected_protocol = revision
        .document
        .pointer("/discovery/selected_protocol")
        .and_then(Value::as_str)
        .unwrap_or(&draft.protocol.preferred);
    let client = build_mcp_client_for_protocol(
        Arc::clone(&transport),
        client_limits,
        request_limit.max(response_limit),
        tasks_requested,
        selected_protocol,
    )?;
    let mut tools = Vec::with_capacity(tool_documents.len());
    let mut imports = Vec::with_capacity(tool_documents.len());
    let version_suffix = revision
        .revision_id
        .trim_start_matches("mrev_")
        .replace('_', ".");
    let revision_hash_suffix = revision
        .revision_hash
        .strip_prefix("sha256:")
        .ok_or(McpResourceConfigError::Provider)?;
    for item in tool_documents {
        let policy: insight_api::v1::McpDraftToolImport = serde_json::from_value(
            item.get("import")
                .cloned()
                .ok_or(McpResourceConfigError::Provider)?,
        )
        .map_err(|_| McpResourceConfigError::Provider)?;
        let candidate = item
            .get("candidate")
            .ok_or(McpResourceConfigError::Provider)?;
        let annotations = candidate
            .get("annotations")
            .filter(|value| !value.is_null())
            .cloned()
            .map(serde_json::from_value)
            .transpose()
            .map_err(|_| McpResourceConfigError::Provider)?;
        tools.push(insight_mcp::Tool {
            name: policy.remote.clone(),
            title: candidate
                .get("title")
                .and_then(Value::as_str)
                .map(str::to_owned),
            description: candidate
                .get("description")
                .and_then(Value::as_str)
                .map(str::to_owned),
            input_schema: candidate
                .get("input_schema")
                .cloned()
                .ok_or(McpResourceConfigError::Provider)?,
            output_schema: candidate
                .get("output_schema")
                .filter(|value| !value.is_null())
                .cloned(),
            annotations,
            metadata: None,
            icons: Vec::new(),
        });
        let effect = match policy.effect.as_str() {
            "pure" => EffectClass::Pure,
            "read_only" => EffectClass::ReadOnly,
            "mutating" => EffectClass::Mutating,
            _ => return Err(McpResourceConfigError::Provider),
        };
        let idempotency = match policy.idempotency.as_str() {
            "idempotent" => IdempotencyClass::Idempotent,
            "non_idempotent" | "unknown" => IdempotencyClass::NonIdempotent,
            _ => return Err(McpResourceConfigError::Provider),
        };
        let cancellation = match policy.cancellation.as_str() {
            "cooperative" | "immediate" => CancellationClass::Cooperative,
            "not_supported" => CancellationClass::NotSupported,
            _ => return Err(McpResourceConfigError::Provider),
        };
        let approval = match policy.approval.as_str() {
            "never" => McpApprovalPolicy::Never,
            "model_tool_only" => McpApprovalPolicy::ModelToolOnly,
            "mutating" => McpApprovalPolicy::Mutating,
            "always" => McpApprovalPolicy::Always,
            _ => return Err(McpResourceConfigError::Provider),
        };
        let description = match policy.description {
            McpToolDescriptionPolicy::Remote => McpDescriptionPolicy::Remote,
            McpToolDescriptionPolicy::Disabled => McpDescriptionPolicy::Disabled,
            McpToolDescriptionPolicy::Override { text } => McpDescriptionPolicy::Override(text),
        };
        let mut required_capabilities = policy
            .required_capabilities
            .into_iter()
            .map(ActionCapability::new)
            .collect::<BTreeSet<_>>();
        if !policy.terminal_only_compatible {
            required_capabilities.insert(ActionCapability::new("runtime.mcp.full_persistence.v1"));
        }
        if policy.input_required == "allowed" || policy.approval != "never" {
            required_capabilities.insert(ActionCapability::new("runtime.mcp.interaction.v1"));
        }
        if policy.tasks == "allowed" {
            required_capabilities.insert(ActionCapability::new("runtime.mcp.tasks.v1"));
        }
        let tool_binding_hash_suffix = item
            .get("tool_binding_hash")
            .and_then(Value::as_str)
            .and_then(|value| value.strip_prefix("sha256:"))
            .ok_or(McpResourceConfigError::Provider)?;
        imports.push(McpToolImport {
            remote_name: policy.remote,
            action_id: item
                .get("action_id")
                .and_then(Value::as_str)
                .ok_or(McpResourceConfigError::Provider)?
                .to_owned(),
            action_version: format!(
                "0.0.0+mcp.{version_suffix}.{revision_hash_suffix}.{tool_binding_hash_suffix}"
            ),
            model_tool_name: policy.alias,
            title: None,
            description,
            effect,
            idempotency,
            cancellation,
            required_capabilities,
            approval,
            allow_input_required: policy.input_required == "allowed",
            allow_tasks: policy.tasks == "allowed",
            terminal_only_compatible: policy.terminal_only_compatible,
            public_policy: {
                let mut value = ToolPublicPolicy::private();
                value.call = policy.public.call;
                value
            },
        });
    }
    let binding = McpServerBindingIdentity {
        connection_id: format!("mcp.{}.{}", server.server_id, revision.revision_id),
        server_id: server.server_id.clone(),
        protocol_version: selected_protocol.to_owned(),
        transport: transport_kind,
        principal_scope: if matches!(
            draft.authorization,
            Some(McpDraftAuthorization::OauthUser { .. })
        ) {
            PrincipalScope::User("oauth_user".to_owned())
        } else {
            PrincipalScope::Service
        },
        discovery_fingerprint: revision.catalog_fingerprint.clone(),
    };
    let catalog = insight_mcp::ToolCatalog {
        tools,
        rejected: Vec::new(),
        ttl_ms: 0,
        cache_scope: insight_mcp::CacheScope::Private,
        descriptor_hash: revision.catalog_fingerprint.clone(),
    };
    let mut provider =
        McpToolProvider::freeze(binding.clone(), Arc::clone(&client), catalog, imports)
            .map_err(|_| McpResourceConfigError::Provider)?
            .with_execution_fence(
                execution_fence,
                revision.revision_id.clone(),
                server.disable_fence,
            );
    if let Some(handler) = interaction_handler.as_ref() {
        provider = provider.with_interaction_handler(Arc::clone(handler));
    }
    if tasks_requested {
        provider = provider.with_remote_task_handler(Arc::new(
            remote_task_runtime
                .clone()
                .ok_or(McpResourceConfigError::Capability)?,
        ));
    }
    if let Some(McpDraftAuthorization::OauthUser {
        scopes,
        client_id,
        redirect_uri,
    }) = &draft.authorization
    {
        let authority = oauth_runtime
            .clone()
            .ok_or(McpResourceConfigError::Credential)?;
        let policy = http_policy
            .clone()
            .ok_or(McpResourceConfigError::Credential)?;
        provider =
            provider.with_runtime_client_resolver(Arc::new(DurableOAuthRuntimeClientResolver {
                authority,
                server_id: server.server_id.clone(),
                resource: policy.endpoint().to_string(),
                client_id: client_id.clone(),
                redirect_uri: redirect_uri.clone(),
                required_scopes: scopes.iter().cloned().collect(),
                policy,
                limits: client_limits,
                max_message_bytes: request_limit.max(response_limit),
                allow_tasks: tasks_requested,
                protocol_version: selected_protocol.to_owned(),
            }));
    }
    let identities: Vec<(String, String)> = provider
        .imports()
        .iter()
        .map(|item| {
            (
                item.import.action_id.clone(),
                item.import.action_version.clone(),
            )
        })
        .collect();
    let tool_bindings = provider
        .imports()
        .iter()
        .map(|item| item.binding.clone())
        .collect::<Vec<_>>();
    provider
        .publish_into(registry)
        .map_err(|_| McpResourceConfigError::Provider)?;

    let capabilities: insight_mcp::ServerCapabilities = serde_json::from_value(
        revision
            .document
            .pointer("/discovery/capabilities")
            .cloned()
            .ok_or(McpResourceConfigError::Provider)?,
    )
    .map_err(|_| McpResourceConfigError::Provider)?;
    let server_info: insight_mcp::ServerInfo = serde_json::from_value(
        revision
            .document
            .pointer("/discovery/server_info")
            .cloned()
            .ok_or(McpResourceConfigError::Provider)?,
    )
    .map_err(|_| McpResourceConfigError::Provider)?;
    let resource_items = serde_json::from_value(
        revision
            .document
            .pointer("/bindings/resources/items")
            .cloned()
            .unwrap_or_else(|| json!([])),
    )
    .map_err(|_| McpResourceConfigError::Provider)?;
    let resource_templates = serde_json::from_value(
        revision
            .document
            .pointer("/bindings/resources/templates")
            .cloned()
            .unwrap_or_else(|| json!([])),
    )
    .map_err(|_| McpResourceConfigError::Provider)?;
    let prompt_items = serde_json::from_value(
        revision
            .document
            .pointer("/bindings/prompts/items")
            .cloned()
            .unwrap_or_else(|| json!([])),
    )
    .map_err(|_| McpResourceConfigError::Provider)?;
    let resource_fingerprint = revision
        .document
        .pointer("/bindings/resources/catalog_fingerprint")
        .and_then(Value::as_str)
        .unwrap_or(&revision.catalog_fingerprint)
        .to_owned();
    let prompt_fingerprint = revision
        .document
        .pointer("/bindings/prompts/catalog_fingerprint")
        .and_then(Value::as_str)
        .unwrap_or(&revision.catalog_fingerprint)
        .to_owned();
    let resource_catalog = ResourceCatalog {
        items: resource_items,
        rejected: Vec::new(),
        ttl_ms: 0,
        cache_scope: insight_mcp::CacheScope::Private,
        descriptor_hash: resource_fingerprint.clone(),
    };
    let template_catalog = ResourceTemplateCatalog {
        items: resource_templates,
        rejected: Vec::new(),
        ttl_ms: 0,
        cache_scope: insight_mcp::CacheScope::Private,
        descriptor_hash: resource_fingerprint,
    };
    let prompt_catalog = PromptCatalog {
        items: prompt_items,
        rejected: Vec::new(),
        ttl_ms: 0,
        cache_scope: insight_mcp::CacheScope::Private,
        descriptor_hash: prompt_fingerprint,
    };
    let resource_policies = draft
        .imports
        .resources
        .allow
        .iter()
        .map(|rule| McpResourceImportPolicy {
            uri_pattern: rule.uri_pattern.clone(),
            mime_allowlist: rule.mime_allowlist.clone(),
            max_content_bytes: rule.max_content_bytes.unwrap_or(response_limit),
        })
        .collect::<Vec<_>>();
    let prompt_policies = draft
        .imports
        .prompts
        .iter()
        .map(|prompt| McpPromptImportPolicy {
            remote_name: prompt.remote.clone(),
            allow_user_invocation: prompt.allow_user_invocation,
            allow_definition_snapshot: prompt.definition_arguments.is_some(),
            definition_arguments: prompt.definition_arguments.clone(),
        })
        .collect::<Vec<_>>();
    let mut context = McpContextProvider::freeze(
        binding.clone(),
        Arc::clone(&client),
        resource_catalog,
        template_catalog,
        prompt_catalog,
        resource_policies,
        prompt_policies,
    )
    .map_err(|_| McpResourceConfigError::Context)?;
    let definition_prompt_results = serde_json::from_value(
        revision
            .document
            .pointer("/bindings/prompts/definition_results")
            .cloned()
            .unwrap_or_else(|| json!({})),
    )
    .map_err(|_| McpResourceConfigError::Context)?;
    context = context
        .with_definition_prompt_results(definition_prompt_results)
        .map_err(|_| McpResourceConfigError::Context)?;
    let oauth_server = if let Some(McpDraftAuthorization::OauthUser {
        scopes,
        client_id,
        redirect_uri,
    }) = &draft.authorization
    {
        let authority = oauth_runtime
            .clone()
            .ok_or(McpResourceConfigError::Credential)?;
        let policy = http_policy.ok_or(McpResourceConfigError::Credential)?;
        context =
            context.with_principal_client_resolver(Arc::new(DurableOAuthRuntimeClientResolver {
                authority,
                server_id: server.server_id.clone(),
                resource: policy.endpoint().to_string(),
                client_id: client_id.clone(),
                redirect_uri: redirect_uri.clone(),
                required_scopes: scopes.iter().cloned().collect(),
                policy: policy.clone(),
                limits: client_limits,
                max_message_bytes: request_limit.max(response_limit),
                allow_tasks: tasks_requested,
                protocol_version: selected_protocol.to_owned(),
            }));
        Some(
            McpOAuthServer::new(
                server.server_id.clone(),
                policy.endpoint().to_string(),
                client_id.clone(),
                redirect_uri.clone(),
                scopes.clone(),
            )
            .map_err(|_| McpResourceConfigError::Credential)?,
        )
    } else {
        None
    };
    let catalog_invalidation = Arc::new(McpCatalogInvalidation::default());
    context = context.with_catalog_invalidation(Arc::clone(&catalog_invalidation));
    let context = Arc::new(context);
    let subscription_filter = subscription_filter(
        &capabilities,
        context.resources(),
        !identities.is_empty(),
        context.prompts().next().is_some(),
    );
    let subscription = (!matches!(
        draft.authorization,
        Some(McpDraftAuthorization::OauthUser { .. })
    ) && subscription_filter_is_nonempty(&subscription_filter))
    .then(|| McpClientSubscription {
        server_id: server.server_id.clone(),
        context: Arc::clone(&context),
        filter: subscription_filter,
        invalidation: catalog_invalidation,
    });
    let principal_repository = oauth_runtime.as_ref().map(|authority| {
        Arc::clone(&authority.interaction_repository) as Arc<dyn McpInteractionDurableRepository>
    });
    let mut retrieval_identities = Vec::new();
    for imported in context.resources().iter().cloned() {
        let registered = retrievals
            .publish_revision(McpResourceRetrieval::for_revision(
                imported,
                Arc::clone(&context),
                principal_repository.as_ref().map(Arc::clone),
                interaction_handler.as_ref().map(Arc::clone),
                &version_suffix,
            ))
            .map_err(|_| McpResourceConfigError::Context)?;
        retrieval_identities.push((
            registered.identity().id.clone(),
            registered.identity().version.to_string(),
        ));
    }
    let required_scopes = match &draft.authorization {
        Some(McpDraftAuthorization::OauthUser { scopes, .. }) => scopes.iter().cloned().collect(),
        _ => BTreeSet::new(),
    };
    let catalog_server = McpCatalogServer::new(
        server.server_id.clone(),
        binding,
        capabilities,
        server_info,
        required_scopes,
        tool_bindings,
    )
    .map_err(|_| McpResourceConfigError::Provider)?
    .with_context(Some(context), None);
    Ok(ManagedMcpRevisionProjection {
        action_identities: identities,
        retrieval_identities,
        catalog_server,
        oauth_server,
        subscription,
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
    use serde_json::json;

    use super::*;

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
