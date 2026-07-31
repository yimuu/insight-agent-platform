use std::{
    collections::{BTreeMap, BTreeSet},
    fmt::Write as _,
    sync::Arc,
};

use async_trait::async_trait;
use base64::{engine::general_purpose::STANDARD as BASE64_STANDARD, Engine as _};
use insight_engine::execution::{RunError, StopReason};
use insight_engine::worker::WorkerArtifactPayload;
use insight_engine::worker::WorkerOperationPermitHandle;
use insight_mcp::{
    ClientError, ContentBlock, CreateTaskResult, GetTaskResult, InputRequiredResult, InputResponse,
    JsonRpcNotification, McpCallOutcome, McpClient, McpNotificationObserver,
    McpServerBindingIdentity, McpToolBinding, MetaMap, ResourceContents, SubscriptionFilter,
    TaskStatus, Tool, ToolCallResult, ToolCatalog, TransportError, UpdateTaskParams,
};
use serde::Serialize;
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use tokio_util::sync::CancellationToken;

use crate::actions::{
    Action, ActionCapability, ActionContext, ActionDescriptor, ActionExecutionResult,
    ActionModelTool, ActionRegistry, ActionSchemaDialect, CancellationClass, EffectClass,
    IdempotencyClass, ToolPublicPolicy,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum McpApprovalPolicy {
    Never,
    ModelToolOnly,
    Mutating,
    Always,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case", tag = "kind", content = "value")]
pub enum McpDescriptionPolicy {
    Remote,
    Override(String),
    Disabled,
}

/// Explicit local authority for one remote tool import.
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct McpToolImport {
    pub remote_name: String,
    pub action_id: String,
    pub action_version: String,
    pub model_tool_name: String,
    pub title: Option<String>,
    pub description: McpDescriptionPolicy,
    pub effect: EffectClass,
    pub idempotency: IdempotencyClass,
    pub cancellation: CancellationClass,
    pub required_capabilities: BTreeSet<ActionCapability>,
    pub approval: McpApprovalPolicy,
    pub allow_input_required: bool,
    pub allow_tasks: bool,
    pub terminal_only_compatible: bool,
    pub public_policy: ToolPublicPolicy,
}

#[derive(Debug, Clone)]
pub struct McpImportedTool {
    pub binding: McpToolBinding,
    pub import: McpToolImport,
}

#[derive(Debug, Clone)]
pub struct McpInteractionContext {
    pub run_id: String,
    pub operation_id: String,
    pub logical_request_key: String,
    pub server_id: String,
    pub binding_hash: String,
    pub generation: u32,
    pub remaining: std::time::Duration,
    pub operation_permit: Option<WorkerOperationPermitHandle>,
}

#[derive(Debug, Clone)]
pub struct McpInteractionResolution {
    pub input_responses: BTreeMap<String, InputResponse>,
    pub request_state: Option<String>,
    pub interaction_ids: Vec<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum McpInteractionHandlerError {
    Declined,
    Cancelled,
    Expired,
    Unavailable,
    Invalid,
}

#[async_trait]
pub trait McpInteractionHandler: Send + Sync {
    async fn request_approval(
        &self,
        context: &McpInteractionContext,
        effect: EffectClass,
        cancellation: &CancellationToken,
    ) -> Result<Vec<String>, McpInteractionHandlerError>;

    async fn resolve_input(
        &self,
        context: &McpInteractionContext,
        required: InputRequiredResult,
        cancellation: &CancellationToken,
    ) -> Result<McpInteractionResolution, McpInteractionHandlerError>;

    async fn complete(
        &self,
        context: &McpInteractionContext,
        interaction_ids: &[String],
        succeeded: bool,
    ) -> Result<(), McpInteractionHandlerError>;
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum McpRuntimeClientResolverError {
    AuthorizationRequired,
    InsufficientScope,
    Declined,
    Cancelled,
    Expired,
    Unavailable,
}

#[async_trait]
pub trait McpRuntimeClientResolver: Send + Sync {
    async fn client_for(
        &self,
        context: &McpInteractionContext,
        cancellation: &CancellationToken,
    ) -> Result<Arc<McpClient>, McpRuntimeClientResolverError>;
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct McpRemoteTaskHandle {
    pub local_task_id: String,
}

#[derive(Debug, Clone, PartialEq)]
pub struct McpRecoveredRemoteTask {
    pub handle: McpRemoteTaskHandle,
    pub created: CreateTaskResult,
}

#[derive(Debug, Clone, PartialEq)]
pub struct McpRemoteTaskObservation {
    pub result: GetTaskResult,
    pub local_version: u64,
    pub local_terminal: Option<McpRemoteTaskLocalTerminal>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct McpRemoteTaskPollAuthority {
    pub local_task_id: String,
    pub owner: String,
    pub lease_epoch: u64,
    pub expected_version: u64,
    pub remote_task_id: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum McpRemoteTaskHandlerError {
    Busy,
    Conflict,
    Invalid,
    Unavailable,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum McpRemoteTaskLocalTerminal {
    Cancelled,
    Expired,
}

#[async_trait]
pub trait McpRemoteTaskHandler: Send + Sync {
    async fn recover(
        &self,
        context: &McpInteractionContext,
        binding: &McpToolBinding,
    ) -> Result<Option<McpRecoveredRemoteTask>, McpRemoteTaskHandlerError>;

    async fn record_created(
        &self,
        context: &McpInteractionContext,
        binding: &McpToolBinding,
        created: &CreateTaskResult,
    ) -> Result<McpRemoteTaskHandle, McpRemoteTaskHandlerError>;

    async fn claim_poll(
        &self,
        handle: &McpRemoteTaskHandle,
    ) -> Result<McpRemoteTaskPollAuthority, McpRemoteTaskHandlerError>;

    async fn record_observation(
        &self,
        authority: &McpRemoteTaskPollAuthority,
        observation: &GetTaskResult,
    ) -> Result<McpRemoteTaskObservation, McpRemoteTaskHandlerError>;

    async fn load_observation(
        &self,
        handle: &McpRemoteTaskHandle,
    ) -> Result<Option<McpRemoteTaskObservation>, McpRemoteTaskHandlerError>;

    async fn record_local_terminal(
        &self,
        handle: &McpRemoteTaskHandle,
        terminal: McpRemoteTaskLocalTerminal,
    ) -> Result<bool, McpRemoteTaskHandlerError>;
}

/// Publication-time MCP tool provider. Discovery/list results are immutable
/// here; runtime execution never refreshes or silently relinks this catalog.
#[derive(Clone)]
pub struct McpToolProvider {
    client: Arc<McpClient>,
    imports: Vec<McpImportedTool>,
    tools: BTreeMap<String, Tool>,
    interaction_handler: Option<Arc<dyn McpInteractionHandler>>,
    runtime_client_resolver: Option<Arc<dyn McpRuntimeClientResolver>>,
    remote_task_handler: Option<Arc<dyn McpRemoteTaskHandler>>,
}

impl McpToolProvider {
    pub fn freeze(
        server: McpServerBindingIdentity,
        client: Arc<McpClient>,
        mut catalog: ToolCatalog,
        imports: Vec<McpToolImport>,
    ) -> Result<Self, McpProviderError> {
        server.validate().map_err(|_| McpProviderError::Server)?;
        let catalog_fingerprint = catalog.descriptor_hash.clone();
        if !catalog.rejected.is_empty()
            && imports.iter().any(|policy| {
                catalog
                    .rejected
                    .iter()
                    .any(|rejected| rejected.tool_name == policy.remote_name)
            })
        {
            return Err(McpProviderError::RejectedTool);
        }
        catalog
            .tools
            .sort_by(|left, right| left.name.as_bytes().cmp(right.name.as_bytes()));
        let tools = catalog
            .tools
            .into_iter()
            .map(|tool| (tool.name.clone(), tool))
            .collect::<BTreeMap<_, _>>();
        let mut remote_names = BTreeSet::new();
        let mut action_ids = BTreeSet::new();
        let mut model_names = BTreeSet::new();
        let mut frozen = Vec::with_capacity(imports.len());
        for policy in imports {
            if !remote_names.insert(policy.remote_name.clone())
                || !action_ids.insert(policy.action_id.clone())
                || !model_names.insert(policy.model_tool_name.clone())
                || (policy.terminal_only_compatible
                    && (policy.allow_input_required
                        || policy.allow_tasks
                        || policy.approval != McpApprovalPolicy::Never))
            {
                return Err(McpProviderError::Policy);
            }
            let tool = tools
                .get(&policy.remote_name)
                .ok_or(McpProviderError::MissingTool)?;
            let title = policy.title.clone().or_else(|| tool.title.clone());
            let description = match &policy.description {
                McpDescriptionPolicy::Remote => tool.description.clone(),
                McpDescriptionPolicy::Override(value) => Some(value.clone()),
                McpDescriptionPolicy::Disabled => None,
            };
            if description.as_ref().is_some_and(|value| {
                value.is_empty() || value.len() > 4_096 || value.chars().any(char::is_control)
            }) {
                return Err(McpProviderError::Policy);
            }
            let annotations = tool
                .annotations
                .as_ref()
                .map(serde_json::to_value)
                .transpose()
                .map_err(|_| McpProviderError::Tool)?
                .and_then(|value| value.as_object().cloned())
                .map(|object| {
                    MetaMap::new(object.into_iter().collect()).map_err(|_| McpProviderError::Tool)
                })
                .transpose()?;
            let binding = McpToolBinding::seal(
                server.clone(),
                insight_mcp::McpToolBindingDescriptor {
                    remote_name: tool.name.clone(),
                    action_id: policy.action_id.clone(),
                    model_tool_name: policy.model_tool_name.clone(),
                    title,
                    description,
                    input_schema: tool.input_schema.clone(),
                    output_schema: tool.output_schema.clone(),
                    annotations,
                },
                catalog_fingerprint.clone(),
                canonical_sha256(&policy).map_err(|_| McpProviderError::Policy)?,
            )
            .map_err(|_| McpProviderError::Tool)?;
            frozen.push(McpImportedTool {
                binding,
                import: policy,
            });
        }
        frozen.sort_by(|left, right| {
            left.binding
                .remote_name
                .as_bytes()
                .cmp(right.binding.remote_name.as_bytes())
        });
        Ok(Self {
            client,
            imports: frozen,
            tools,
            interaction_handler: None,
            runtime_client_resolver: None,
            remote_task_handler: None,
        })
    }

    pub fn with_interaction_handler(mut self, handler: Arc<dyn McpInteractionHandler>) -> Self {
        self.interaction_handler = Some(handler);
        self
    }

    pub fn with_runtime_client_resolver(
        mut self,
        resolver: Arc<dyn McpRuntimeClientResolver>,
    ) -> Self {
        self.runtime_client_resolver = Some(resolver);
        self
    }

    pub fn with_remote_task_handler(mut self, handler: Arc<dyn McpRemoteTaskHandler>) -> Self {
        self.remote_task_handler = Some(handler);
        self
    }

    pub fn imports(&self) -> &[McpImportedTool] {
        &self.imports
    }

    pub fn register_into(self, registry: &mut ActionRegistry) -> Result<(), McpProviderError> {
        for imported in self.imports {
            let tool = self
                .tools
                .get(&imported.binding.remote_name)
                .cloned()
                .ok_or(McpProviderError::MissingTool)?;
            registry
                .register(McpToolAction {
                    client: Arc::clone(&self.client),
                    tool,
                    imported,
                    interaction_handler: self.interaction_handler.clone(),
                    runtime_client_resolver: self.runtime_client_resolver.clone(),
                    remote_task_handler: self.remote_task_handler.clone(),
                })
                .map_err(|_| McpProviderError::Registry)?;
        }
        Ok(())
    }
}

fn canonical_sha256(value: &impl Serialize) -> Result<String, serde_json::Error> {
    let canonical = serde_jcs::to_vec(value)?;
    let digest = Sha256::digest(canonical);
    let mut encoded = String::with_capacity(64);
    for byte in digest {
        write!(&mut encoded, "{byte:02x}").expect("writing to String cannot fail");
    }
    Ok(encoded)
}

struct McpToolAction {
    client: Arc<McpClient>,
    tool: Tool,
    imported: McpImportedTool,
    interaction_handler: Option<Arc<dyn McpInteractionHandler>>,
    runtime_client_resolver: Option<Arc<dyn McpRuntimeClientResolver>>,
    remote_task_handler: Option<Arc<dyn McpRemoteTaskHandler>>,
}

#[async_trait]
impl Action for McpToolAction {
    fn descriptor(&self) -> ActionDescriptor {
        ActionDescriptor {
            id: self.imported.import.action_id.clone(),
            version: self.imported.import.action_version.clone(),
            input_schema: self.tool.input_schema.clone(),
            output_schema: mcp_result_envelope_schema(),
            effect: self.imported.import.effect,
            idempotency: self.imported.import.idempotency,
            cancellation: self.imported.import.cancellation,
            required_capabilities: self.imported.import.required_capabilities.clone(),
        }
    }

    fn schema_dialect(&self) -> ActionSchemaDialect {
        ActionSchemaDialect::ProviderDeclared
    }

    fn model_tool(&self) -> ActionModelTool {
        ActionModelTool {
            name: self.imported.binding.model_tool_name.clone(),
            title: self.imported.binding.title.clone(),
            description: self.imported.binding.description.clone(),
        }
    }

    fn public_policy(&self) -> ToolPublicPolicy {
        self.imported.import.public_policy.clone()
    }

    async fn call(&self, input: Value, context: ActionContext) -> Result<Value, RunError> {
        let result = self.call_remote(input, context, false).await?;
        match result {
            McpCallOutcome::Complete(result) if result.is_error == Some(true) => Err(
                RunError::operation("MCP_TOOL_EXECUTION_FAILED", "MCP tool execution failed"),
            ),
            McpCallOutcome::Complete(result) => normalize_result(result),
            McpCallOutcome::InputRequired(_) => Err(interaction_unavailable(
                self.imported.import.allow_input_required,
            )),
            McpCallOutcome::Task(_) => Err(unexpected_task(self.imported.import.allow_tasks)),
        }
    }

    async fn call_with_artifacts(
        &self,
        input: Value,
        context: ActionContext,
    ) -> Result<ActionExecutionResult, RunError> {
        let result = self.call_remote(input, context, false).await?;
        match result {
            McpCallOutcome::Complete(result) if result.is_error == Some(true) => Err(
                RunError::operation("MCP_TOOL_EXECUTION_FAILED", "MCP tool execution failed"),
            ),
            McpCallOutcome::Complete(result) => normalize_result_with_artifacts(result),
            McpCallOutcome::InputRequired(_) => Err(interaction_unavailable(
                self.imported.import.allow_input_required,
            )),
            McpCallOutcome::Task(_) => Err(unexpected_task(self.imported.import.allow_tasks)),
        }
    }

    async fn call_model_tool(
        &self,
        input: Value,
        context: ActionContext,
    ) -> Result<Value, RunError> {
        match self.call_remote(input, context, true).await? {
            McpCallOutcome::Complete(result) => normalize_result(result),
            McpCallOutcome::InputRequired(_) => Err(interaction_unavailable(
                self.imported.import.allow_input_required,
            )),
            McpCallOutcome::Task(_) => Err(unexpected_task(self.imported.import.allow_tasks)),
        }
    }

    async fn call_model_tool_with_artifacts(
        &self,
        input: Value,
        context: ActionContext,
    ) -> Result<ActionExecutionResult, RunError> {
        match self.call_remote(input, context, true).await? {
            McpCallOutcome::Complete(result) => normalize_result_with_artifacts(result),
            McpCallOutcome::InputRequired(_) => Err(interaction_unavailable(
                self.imported.import.allow_input_required,
            )),
            McpCallOutcome::Task(_) => Err(unexpected_task(self.imported.import.allow_tasks)),
        }
    }
}

impl McpToolAction {
    async fn call_remote(
        &self,
        input: Value,
        context: ActionContext,
        model_tool: bool,
    ) -> Result<McpCallOutcome, RunError> {
        let cancellation = CancellationToken::new();
        let cancellation_on_stop = cancellation.clone();
        let stop_control = context.control.clone();
        let stop_watcher = tokio::spawn(async move {
            stop_control.stopped().await;
            cancellation_on_stop.cancel();
        });
        let observer = ActionProgressObserver {
            context: context.clone(),
        };
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
        let client = match &self.runtime_client_resolver {
            Some(resolver) => resolver
                .client_for(&interaction_context, &cancellation)
                .await
                .map_err(map_runtime_client_resolver_error)?,
            None => Arc::clone(&self.client),
        };
        let approval_required = match self.imported.import.approval {
            McpApprovalPolicy::Never => false,
            McpApprovalPolicy::ModelToolOnly => model_tool,
            McpApprovalPolicy::Mutating => self.imported.import.effect == EffectClass::Mutating,
            McpApprovalPolicy::Always => true,
        };
        let mut interaction_ids = Vec::new();
        if approval_required {
            let handler = self
                .interaction_handler
                .as_ref()
                .ok_or_else(|| interaction_unavailable(true))?;
            interaction_ids.extend(
                handler
                    .request_approval(
                        &interaction_context,
                        self.imported.import.effect,
                        &cancellation,
                    )
                    .await
                    .map_err(map_interaction_error)?,
            );
        }
        let mut input_responses = None;
        let mut request_state = None;
        let mut result = None;
        if self.imported.import.allow_tasks {
            if let Some(handler) = &self.remote_task_handler {
                if let Some(recovered) = handler
                    .recover(&interaction_context, &self.imported.binding)
                    .await
                    .map_err(|_| {
                        RunError::infrastructure(
                            "MCP_TASK_RUNTIME_UNAVAILABLE",
                            "durable MCP task recovery is unavailable",
                        )
                    })?
                {
                    result = Some(
                        self.await_remote_task(
                            Arc::clone(&client),
                            &mut interaction_context,
                            recovered.created,
                            Some(recovered.handle),
                            &cancellation,
                            context.clone(),
                            &mut interaction_ids,
                        )
                        .await
                        .map(McpCallOutcome::Complete),
                    );
                }
            }
        }
        for round in 0..8 {
            if result.is_some() {
                break;
            }
            interaction_context.generation = u32::try_from(round + 1).unwrap_or(u32::MAX);
            let outcome = client
                .call_tool(
                    &self.tool,
                    input.clone(),
                    input_responses.take(),
                    request_state.take(),
                    &cancellation,
                    &observer,
                )
                .await;
            match outcome {
                Ok(McpCallOutcome::InputRequired(required))
                    if self.imported.import.allow_input_required =>
                {
                    let handler = self
                        .interaction_handler
                        .as_ref()
                        .ok_or_else(|| interaction_unavailable(true))?;
                    let resolution = handler
                        .resolve_input(&interaction_context, required, &cancellation)
                        .await
                        .map_err(map_interaction_error)?;
                    input_responses = Some(resolution.input_responses);
                    request_state = resolution.request_state;
                    interaction_ids.extend(resolution.interaction_ids);
                }
                Ok(McpCallOutcome::Task(task)) if self.imported.import.allow_tasks => {
                    result = Some(
                        self.await_remote_task(
                            Arc::clone(&client),
                            &mut interaction_context,
                            task,
                            None,
                            &cancellation,
                            context.clone(),
                            &mut interaction_ids,
                        )
                        .await
                        .map(McpCallOutcome::Complete),
                    );
                    break;
                }
                outcome => {
                    result = Some(outcome);
                    break;
                }
            }
        }
        stop_watcher.abort();
        let outcome = result
            .ok_or_else(|| {
                RunError::infrastructure(
                    "MCP_INTERACTION_ROUND_LIMIT",
                    "MCP interaction round limit was exceeded",
                )
            })?
            .map_err(|error| map_client_error(error, &context));
        if !interaction_ids.is_empty() {
            if let Some(handler) = &self.interaction_handler {
                handler
                    .complete(&interaction_context, &interaction_ids, outcome.is_ok())
                    .await
                    .map_err(map_interaction_error)?;
            }
        }
        outcome
    }

    #[allow(clippy::too_many_arguments)]
    async fn await_remote_task(
        &self,
        client: Arc<McpClient>,
        interaction_context: &mut McpInteractionContext,
        created: CreateTaskResult,
        recovered_handle: Option<McpRemoteTaskHandle>,
        cancellation: &CancellationToken,
        progress_context: ActionContext,
        interaction_ids: &mut Vec<String>,
    ) -> Result<ToolCallResult, ClientError> {
        created.validate().map_err(|_| ClientError::Task)?;
        let durable_handle = match (&self.remote_task_handler, recovered_handle) {
            (_, Some(handle)) => Some(handle),
            (Some(handler), None) => Some(
                handler
                    .record_created(interaction_context, &self.imported.binding, &created)
                    .await
                    .map_err(|_| ClientError::Task)?,
            ),
            (None, None) => None,
        };
        let mut recovered_observation = match (&self.remote_task_handler, &durable_handle) {
            (Some(handler), Some(handle)) => handler
                .load_observation(handle)
                .await
                .map_err(|_| ClientError::Task)?,
            _ => None,
        };
        let task_observer = Arc::new(TaskNotificationObserver {
            task_id: created.task.task_id.clone(),
            progress: ActionProgressObserver {
                context: progress_context,
            },
            changed: tokio::sync::Notify::new(),
        });
        let subscription_cancellation = CancellationToken::new();
        let subscription = {
            let client = Arc::clone(&client);
            let task_observer = Arc::clone(&task_observer);
            let cancellation = subscription_cancellation.clone();
            tokio::spawn(async move {
                let _ = client
                    .listen_subscriptions(
                        SubscriptionFilter {
                            tools_list_changed: None,
                            resources_list_changed: None,
                            prompts_list_changed: None,
                            resource_subscriptions: Vec::new(),
                            task_ids: vec![task_observer.task_id.clone()],
                        },
                        &cancellation,
                        task_observer.as_ref(),
                    )
                    .await;
            })
        };
        let _subscription = RemoteTaskSubscription {
            cancellation: subscription_cancellation,
            handle: subscription,
        };
        let ttl_deadline = created
            .task
            .ttl_ms
            .and_then(|ttl| chrono::Duration::from_std(std::time::Duration::from_millis(ttl)).ok())
            .and_then(|ttl| {
                (created.task.created_at + ttl - chrono::Utc::now())
                    .to_std()
                    .ok()
            })
            .and_then(|remaining| std::time::Instant::now().checked_add(remaining))
            .or_else(|| created.task.ttl_ms.map(|_| std::time::Instant::now()));
        let mut ttl_terminal_attempted = false;
        let mut poll_interval = created.task.poll_interval_ms.unwrap_or(1_000).max(100);
        loop {
            if !ttl_terminal_attempted
                && ttl_deadline.is_some_and(|deadline| std::time::Instant::now() >= deadline)
            {
                ttl_terminal_attempted = true;
                let won = match (&self.remote_task_handler, &durable_handle) {
                    (Some(handler), Some(handle)) => handler
                        .record_local_terminal(handle, McpRemoteTaskLocalTerminal::Expired)
                        .await
                        .map_err(|_| ClientError::Task)?,
                    _ => true,
                };
                if !won {
                    recovered_observation = match (&self.remote_task_handler, &durable_handle) {
                        (Some(handler), Some(handle)) => handler
                            .load_observation(handle)
                            .await
                            .map_err(|_| ClientError::Task)?,
                        _ => None,
                    };
                    if recovered_observation.is_some() {
                        continue;
                    }
                }
                let best_effort = CancellationToken::new();
                let _ = client
                    .cancel_task(&created.task.task_id, &best_effort, task_observer.as_ref())
                    .await;
                return Err(ClientError::TaskExpired);
            }
            let (task, local_version, local_terminal) = if let Some(observation) =
                recovered_observation.take()
            {
                (
                    observation.result,
                    Some(observation.local_version),
                    observation.local_terminal,
                )
            } else {
                if let Some(permit) = interaction_context.operation_permit.as_ref() {
                    permit.suspend();
                }
                let cancelled = tokio::select! {
                    _ = cancellation.cancelled() => true,
                    _ = task_observer.changed.notified() => false,
                    _ = tokio::time::sleep(std::time::Duration::from_millis(poll_interval)) => false,
                };
                if let Some(permit) = interaction_context.operation_permit.as_ref() {
                    permit.resume().await.map_err(|_| ClientError::Task)?;
                }
                if cancelled {
                    if let (Some(handler), Some(handle)) =
                        (&self.remote_task_handler, &durable_handle)
                    {
                        handler
                            .record_local_terminal(handle, McpRemoteTaskLocalTerminal::Cancelled)
                            .await
                            .map_err(|_| ClientError::Task)?;
                    }
                    let best_effort = CancellationToken::new();
                    let _ = client
                        .cancel_task(&created.task.task_id, &best_effort, task_observer.as_ref())
                        .await;
                    return Err(ClientError::Transport(TransportError::Cancelled));
                }
                let poll_authority = match (&self.remote_task_handler, &durable_handle) {
                    (Some(handler), Some(handle)) => match handler.claim_poll(handle).await {
                        Ok(authority) => {
                            if authority.remote_task_id != created.task.task_id {
                                return Err(ClientError::Task);
                            }
                            Some(authority)
                        }
                        Err(McpRemoteTaskHandlerError::Busy) => continue,
                        Err(_) => return Err(ClientError::Task),
                    },
                    _ => None,
                };
                let task = client
                    .get_task(
                        poll_authority
                            .as_ref()
                            .map_or(created.task.task_id.as_str(), |authority| {
                                authority.remote_task_id.as_str()
                            }),
                        cancellation,
                        task_observer.as_ref(),
                    )
                    .await?;
                let durable_observation = if let (Some(handler), Some(authority)) =
                    (&self.remote_task_handler, poll_authority.as_ref())
                {
                    Some(
                        handler
                            .record_observation(authority, &task)
                            .await
                            .map_err(|_| ClientError::Task)?,
                    )
                } else {
                    None
                };
                match durable_observation {
                    Some(observation) => (
                        observation.result,
                        Some(observation.local_version),
                        observation.local_terminal,
                    ),
                    None => (task, None, None),
                }
            };
            if local_terminal == Some(McpRemoteTaskLocalTerminal::Expired) {
                return Err(ClientError::TaskExpired);
            }
            if local_terminal == Some(McpRemoteTaskLocalTerminal::Cancelled) {
                return Err(ClientError::TaskCancelled);
            }
            poll_interval = task.task.poll_interval_ms.unwrap_or(poll_interval).max(100);
            match task.task.status {
                TaskStatus::Working => {}
                TaskStatus::InputRequired => {
                    let requests = task.input_requests.ok_or(ClientError::Task)?;
                    if !self.imported.import.allow_input_required {
                        let best_effort = CancellationToken::new();
                        let _ = client
                            .cancel_task(
                                &created.task.task_id,
                                &best_effort,
                                task_observer.as_ref(),
                            )
                            .await;
                        return Err(ClientError::Task);
                    }
                    interaction_context.generation = match local_version {
                        Some(version) => u32::try_from(version).map_err(|_| ClientError::Task)?,
                        None => interaction_context.generation.saturating_add(1),
                    };
                    let handler = self.interaction_handler.as_ref().ok_or(ClientError::Task)?;
                    let resolution = handler
                        .resolve_input(
                            interaction_context,
                            InputRequiredResult {
                                result_type: "input_required".to_owned(),
                                input_requests: requests,
                                request_state: None,
                                metadata: None,
                            },
                            cancellation,
                        )
                        .await
                        .map_err(|_| ClientError::Task)?;
                    let update = client
                        .update_task(
                            UpdateTaskParams {
                                task_id: created.task.task_id.clone(),
                                input_responses: resolution.input_responses,
                            },
                            cancellation,
                            task_observer.as_ref(),
                        )
                        .await;
                    if let Err(error) = update {
                        let _ = handler
                            .complete(interaction_context, &resolution.interaction_ids, false)
                            .await;
                        return Err(error);
                    }
                    handler
                        .complete(interaction_context, &resolution.interaction_ids, true)
                        .await
                        .map_err(|_| ClientError::Task)?;
                    interaction_ids.extend(resolution.interaction_ids);
                }
                TaskStatus::Completed => {
                    let result = task.result.ok_or(ClientError::Task)?;
                    return client.decode_task_tool_result(&self.tool, result);
                }
                TaskStatus::Failed => return Err(ClientError::TaskFailed),
                TaskStatus::Cancelled => return Err(ClientError::TaskCancelled),
            }
        }
    }
}

fn map_runtime_client_resolver_error(error: McpRuntimeClientResolverError) -> RunError {
    match error {
        McpRuntimeClientResolverError::AuthorizationRequired => RunError::operation(
            "MCP_AUTHORIZATION_REQUIRED",
            "MCP authorization is required for the current principal",
        ),
        McpRuntimeClientResolverError::InsufficientScope => RunError::operation(
            "MCP_AUTHORIZATION_INSUFFICIENT_SCOPE",
            "MCP authorization does not grant the frozen required scopes",
        ),
        McpRuntimeClientResolverError::Declined => RunError::operation(
            "MCP_AUTHORIZATION_DECLINED",
            "MCP authorization was declined",
        ),
        McpRuntimeClientResolverError::Cancelled => RunError::operation(
            "MCP_AUTHORIZATION_CANCELLED",
            "MCP authorization was cancelled",
        ),
        McpRuntimeClientResolverError::Expired => RunError::operation(
            "MCP_AUTHORIZATION_EXPIRED",
            "MCP authorization interaction expired",
        ),
        McpRuntimeClientResolverError::Unavailable => RunError::infrastructure(
            "MCP_AUTHORIZATION_UNAVAILABLE",
            "MCP authorization state is unavailable",
        ),
    }
}

fn map_interaction_error(error: McpInteractionHandlerError) -> RunError {
    match error {
        McpInteractionHandlerError::Declined => {
            RunError::operation("MCP_INTERACTION_DECLINED", "MCP interaction was declined")
        }
        McpInteractionHandlerError::Cancelled => {
            RunError::operation("MCP_INTERACTION_CANCELLED", "MCP interaction was cancelled")
        }
        McpInteractionHandlerError::Expired => {
            RunError::operation("MCP_INTERACTION_EXPIRED", "MCP interaction expired")
        }
        McpInteractionHandlerError::Unavailable | McpInteractionHandlerError::Invalid => {
            RunError::infrastructure(
                "MCP_INTERACTION_RUNTIME_UNAVAILABLE",
                "durable MCP interaction runtime is unavailable",
            )
        }
    }
}

struct ActionProgressObserver {
    context: ActionContext,
}

impl McpNotificationObserver for ActionProgressObserver {
    fn on_notification(
        &self,
        notification: &JsonRpcNotification<Value>,
    ) -> Result<(), TransportError> {
        if notification.method != "notifications/progress" {
            return Ok(());
        }
        let Some(params) = notification.params.as_ref() else {
            return Err(TransportError::Notification);
        };
        let progress = params
            .get("progress")
            .and_then(Value::as_f64)
            .filter(|value| value.is_finite())
            .ok_or(TransportError::Notification)?;
        let total = params
            .get("total")
            .and_then(Value::as_f64)
            .filter(|value| value.is_finite());
        let context = self.context.clone();
        tokio::spawn(async move {
            let mut public = json!({"progress": progress});
            if let Some(total) = total {
                public["total"] = json!(total);
            }
            let _ = context.publish_progress(public).await;
        });
        Ok(())
    }
}

struct TaskNotificationObserver {
    task_id: String,
    progress: ActionProgressObserver,
    changed: tokio::sync::Notify,
}

impl McpNotificationObserver for TaskNotificationObserver {
    fn on_notification(
        &self,
        notification: &JsonRpcNotification<Value>,
    ) -> Result<(), TransportError> {
        if notification.method == "notifications/tasks/status" {
            let task_id = notification
                .params
                .as_ref()
                .and_then(|params| params.get("taskId"))
                .and_then(Value::as_str)
                .ok_or(TransportError::Notification)?;
            if task_id == self.task_id {
                self.changed.notify_one();
            }
            return Ok(());
        }
        self.progress.on_notification(notification)
    }
}

struct RemoteTaskSubscription {
    cancellation: CancellationToken,
    handle: tokio::task::JoinHandle<()>,
}

impl Drop for RemoteTaskSubscription {
    fn drop(&mut self) {
        self.cancellation.cancel();
        self.handle.abort();
    }
}

fn map_client_error(error: ClientError, context: &ActionContext) -> RunError {
    match error {
        ClientError::Transport(TransportError::Cancelled) => RunError::stopped(
            context
                .control
                .stop_reason()
                .unwrap_or(StopReason::Cancelled),
        ),
        ClientError::ToolArguments | ClientError::RequestState => RunError::operation(
            "MCP_TOOL_INPUT_INVALID",
            "MCP tool input violates its frozen contract",
        ),
        ClientError::Protocol(-32601) => RunError::infrastructure(
            "MCP_BINDING_DRIFT",
            "MCP server no longer recognizes the frozen tool binding",
        ),
        ClientError::Transport(_)
        | ClientError::Protocol(_)
        | ClientError::Response
        | ClientError::ToolResult
        | ClientError::ContentLimit
        | ClientError::ToolContract
        | ClientError::SchemaDraft
        | ClientError::ResourceUri
        | ClientError::ResourceContent
        | ClientError::PromptContract
        | ClientError::PromptArguments
        | ClientError::PromptResult
        | ClientError::Completion
        | ClientError::Subscription
        | ClientError::Task => {
            RunError::infrastructure("MCP_TOOL_UNAVAILABLE", "MCP tool execution is unavailable")
        }
        ClientError::TaskExpired => {
            RunError::operation("MCP_REMOTE_TASK_EXPIRED", "MCP remote task expired")
        }
        ClientError::TaskFailed => {
            RunError::operation("MCP_REMOTE_TASK_FAILED", "MCP remote task failed")
        }
        ClientError::TaskCancelled => {
            RunError::operation("MCP_REMOTE_TASK_CANCELLED", "MCP remote task was cancelled")
        }
        ClientError::Configuration
        | ClientError::Request
        | ClientError::RequestIdExhausted
        | ClientError::Discovery
        | ClientError::Catalog
        | ClientError::CatalogLimit
        | ClientError::Pagination => RunError::infrastructure(
            "MCP_PROVIDER_INVALID",
            "MCP provider configuration is invalid",
        ),
    }
}

fn interaction_unavailable(allowed: bool) -> RunError {
    if allowed {
        RunError::infrastructure(
            "MCP_INTERACTION_RUNTIME_UNAVAILABLE",
            "durable MCP interaction runtime is unavailable",
        )
    } else {
        RunError::infrastructure(
            "MCP_UNEXPECTED_INPUT_REQUIRED",
            "MCP server returned input-required outside the frozen policy",
        )
    }
}

fn unexpected_task(allowed: bool) -> RunError {
    if allowed {
        RunError::infrastructure(
            "MCP_TASK_RUNTIME_UNAVAILABLE",
            "MCP task runtime is unavailable",
        )
    } else {
        RunError::infrastructure(
            "MCP_UNEXPECTED_TASK",
            "MCP server returned a task outside the frozen policy",
        )
    }
}

fn normalize_result(result: ToolCallResult) -> Result<Value, RunError> {
    Ok(json!({
        "is_error": result.is_error.unwrap_or(false),
        "content": result.content,
        "structured_content": result.structured_content,
        "remote_metadata_safe": {}
    }))
}

fn normalize_result_with_artifacts(
    result: ToolCallResult,
) -> Result<ActionExecutionResult, RunError> {
    let mut artifact_payloads = Vec::new();
    let mut content = Vec::with_capacity(result.content.len());
    for block in result.content {
        let projected = match block {
            ContentBlock::Image {
                data, mime_type, ..
            } => externalize_content(
                &data,
                Some(mime_type),
                json!({"kind": "image"}),
                &mut artifact_payloads,
            )?,
            ContentBlock::Audio {
                data, mime_type, ..
            } => externalize_content(
                &data,
                Some(mime_type),
                json!({"kind": "audio"}),
                &mut artifact_payloads,
            )?,
            ContentBlock::EmbeddedResource {
                resource:
                    ResourceContents::Blob {
                        uri,
                        blob,
                        mime_type,
                        ..
                    },
                ..
            } => externalize_content(
                &blob,
                mime_type,
                json!({"kind": "embedded_resource", "uri": uri}),
                &mut artifact_payloads,
            )?,
            other => serde_json::to_value(other).map_err(|_| {
                RunError::infrastructure(
                    "MCP_CONTENT_INVALID",
                    "MCP tool content could not be normalized",
                )
            })?,
        };
        content.push(projected);
    }
    Ok(ActionExecutionResult::with_artifact_payloads(
        json!({
            "is_error": result.is_error.unwrap_or(false),
            "content": content,
            "structured_content": result.structured_content,
            "remote_metadata_safe": {}
        }),
        artifact_payloads,
    ))
}

fn externalize_content(
    encoded: &str,
    media_type: Option<String>,
    source: Value,
    artifact_payloads: &mut Vec<WorkerArtifactPayload>,
) -> Result<Value, RunError> {
    let bytes = BASE64_STANDARD.decode(encoded).map_err(|_| {
        RunError::operation(
            "MCP_CONTENT_INVALID",
            "MCP binary content is not valid base64",
        )
    })?;
    let payload = WorkerArtifactPayload::new(bytes, media_type).map_err(|_| {
        RunError::operation(
            "MCP_CONTENT_LIMIT_EXCEEDED",
            "MCP binary content exceeds the Artifact limit",
        )
    })?;
    let reference = serde_json::to_value(payload.artifact()).map_err(|_| {
        RunError::infrastructure(
            "MCP_CONTENT_INVALID",
            "MCP Artifact reference could not be normalized",
        )
    })?;
    artifact_payloads.push(payload);
    Ok(json!({
        "type": "artifact",
        "artifact": reference,
        "source": source
    }))
}

fn mcp_result_envelope_schema() -> Value {
    json!({
        "type":"object",
        "properties":{
            "is_error":{"type":"boolean"},
            "content":{"type":"array","maxItems":256},
            "structured_content":{},
            "remote_metadata_safe":{"type":"object","additionalProperties":false}
        },
        "required":["is_error","content","structured_content","remote_metadata_safe"],
        "additionalProperties":false
    })
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum McpProviderError {
    Server,
    Tool,
    MissingTool,
    RejectedTool,
    Policy,
    Registry,
}

impl std::fmt::Display for McpProviderError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("invalid MCP tool provider")
    }
}

impl std::error::Error for McpProviderError {}

#[cfg(test)]
mod tests {
    use std::sync::Mutex;

    use insight_engine::execution::{stop_pair, ExecutionControl};
    use insight_mcp::{
        ClientCapabilities, ClientInfo, JsonRpcNotification, McpTransport,
        NoopNotificationObserver, PrincipalScope, RequestId, ToolCatalogRejection, TransportKind,
        MCP_PROTOCOL_VERSION,
    };
    use serde_json::json;

    use super::*;

    #[derive(Default)]
    struct FixtureTransport {
        response: Mutex<Option<Value>>,
    }

    #[async_trait]
    impl McpTransport for FixtureTransport {
        fn kind(&self) -> TransportKind {
            TransportKind::StreamableHttp
        }

        async fn exchange(
            &self,
            request: &Value,
            _parameter_headers: &BTreeMap<String, String>,
            _cancellation: &CancellationToken,
            _observer: &dyn McpNotificationObserver,
        ) -> Result<Value, TransportError> {
            let mut response = self.response.lock().unwrap().take().unwrap();
            response["id"] = request["id"].clone();
            Ok(response)
        }
    }

    fn server() -> McpServerBindingIdentity {
        McpServerBindingIdentity {
            connection_id: "fixture.tools".to_owned(),
            server_id: "https://example.test/mcp".to_owned(),
            protocol_version: MCP_PROTOCOL_VERSION.to_owned(),
            transport: insight_mcp::McpTransportKind::StreamableHttp,
            principal_scope: PrincipalScope::Service,
            discovery_fingerprint: "a".repeat(64),
        }
    }

    fn client(transport: Arc<FixtureTransport>) -> Arc<McpClient> {
        Arc::new(
            McpClient::new(
                transport,
                ClientInfo {
                    name: "fixture".to_owned(),
                    version: "1.0.0".to_owned(),
                    title: None,
                    description: None,
                    website_url: None,
                    icons: Vec::new(),
                },
                ClientCapabilities::default(),
            )
            .unwrap(),
        )
    }

    fn import() -> McpToolImport {
        McpToolImport {
            remote_name: "remote.search".to_owned(),
            action_id: "mcp.fixture.remote-search".to_owned(),
            action_version: "1.0.0".to_owned(),
            model_tool_name: "fixture_search".to_owned(),
            title: None,
            description: McpDescriptionPolicy::Remote,
            effect: EffectClass::ReadOnly,
            idempotency: IdempotencyClass::Idempotent,
            cancellation: CancellationClass::Cooperative,
            required_capabilities: BTreeSet::new(),
            approval: McpApprovalPolicy::Never,
            allow_input_required: false,
            allow_tasks: false,
            terminal_only_compatible: true,
            public_policy: ToolPublicPolicy::private(),
        }
    }

    fn catalog() -> ToolCatalog {
        ToolCatalog {
            tools: vec![Tool {
                name: "remote.search".to_owned(),
                title: Some("Remote search".to_owned()),
                description: Some("Search a remote catalog.".to_owned()),
                input_schema: json!({
                    "$schema":"http://json-schema.org/draft-07/schema#",
                    "type":"object",
                    "properties":{"query":{"type":"string"}},
                    "required":["query"]
                }),
                output_schema: None,
                annotations: None,
                metadata: None,
                icons: Vec::new(),
            }],
            rejected: Vec::<ToolCatalogRejection>::new(),
            ttl_ms: 1000,
            cache_scope: insight_mcp::CacheScope::Private,
            descriptor_hash: "b".repeat(64),
        }
    }

    #[tokio::test]
    async fn provider_freezes_identity_and_preserves_model_tool_errors() {
        let transport = Arc::new(FixtureTransport::default());
        *transport.response.lock().unwrap() = Some(json!({
            "jsonrpc":"2.0",
            "id":0,
            "result":{
                "resultType":"complete",
                "content":[{"type":"text","text":"use a narrower query"}],
                "isError":true
            }
        }));
        let provider =
            McpToolProvider::freeze(server(), client(transport), catalog(), vec![import()])
                .unwrap();
        assert_eq!(provider.imports()[0].binding.remote_name, "remote.search");
        assert_eq!(
            provider.imports()[0].binding.model_tool_name,
            "fixture_search"
        );
        let mut registry = ActionRegistry::default();
        provider.register_into(&mut registry).unwrap();
        let registered = registry.resolve("mcp.fixture.remote-search").unwrap();
        assert_eq!(registered.model_tool().name, "fixture_search");
        let (_controller, signal) = stop_pair();
        let context = ActionContext::for_operation(
            "run",
            "tool",
            1,
            ExecutionControl::new(signal, std::time::Duration::from_secs(30)),
        );
        let output = registered
            .call_model_tool(json!({"query":"broad"}), context)
            .await
            .unwrap();
        assert_eq!(output["is_error"], true);
        assert_eq!(output["content"][0]["type"], "text");
    }

    #[test]
    fn provider_requires_explicit_noninteractive_policy() {
        let transport = Arc::new(FixtureTransport::default());
        let mut policy = import();
        policy.approval = McpApprovalPolicy::Always;
        assert_eq!(
            McpToolProvider::freeze(server(), client(transport), catalog(), vec![policy]).err(),
            Some(McpProviderError::Policy)
        );
        let _: Option<JsonRpcNotification<Value>> = None;
        let _: Option<RequestId> = None;
        let _ = NoopNotificationObserver;
    }

    #[test]
    fn binary_tool_content_becomes_a_typed_transient_artifact() {
        let result = ToolCallResult {
            result_type: "complete".to_owned(),
            content: vec![ContentBlock::Image {
                data: "AQID".to_owned(),
                mime_type: "image/png".to_owned(),
                annotations: None,
                metadata: None,
            }],
            structured_content: None,
            is_error: Some(false),
            metadata: None,
        };
        let execution = normalize_result_with_artifacts(result).unwrap();
        assert_eq!(execution.output()["content"][0]["type"], "artifact");
        assert_eq!(
            execution.output()["content"][0]["artifact"]["media_type"],
            "image/png"
        );
        assert!(!execution.output().to_string().contains("AQID"));
        let (_, payloads) = execution.into_parts();
        assert_eq!(payloads.len(), 1);
        assert_eq!(payloads[0].bytes(), &[1, 2, 3]);
    }
}
