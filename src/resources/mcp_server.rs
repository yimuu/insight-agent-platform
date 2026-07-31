//! Explicit platform exports for the modern MCP server profile.

use std::{
    collections::BTreeMap,
    sync::Arc,
    time::{Duration, Instant},
};

use async_trait::async_trait;
use chrono::{Duration as ChronoDuration, Utc};
use insight_durable::{
    McpInteraction, McpInteractionDisposition, McpInteractionDurableRepository,
    McpInteractionListFilter, McpInteractionPrincipal, McpInteractionRequest, McpInteractionState,
    McpProtectedSecret, McpSecretCiphertext, McpSecretProtector, McpSecretPurpose, McpSecretScope,
    McpServerTask, McpServerTaskDurableRepository, ResolveMcpInteractionCommand,
};
use insight_engine::{
    execution::{stop_pair, ExecutionControl},
    history::types::RunLifecycle,
    TransitionOutcome,
};
use insight_mcp::{
    CacheScope, CompleteResult, Completion, CompletionArgument, CompletionReference, ContentBlock,
    CreateTaskResult, DiscoverResult, ElicitAction, ElicitRequestParams, GetPromptResult,
    GetTaskResult, InputRequest, InputRequiredResult, InputResponse, JsonRpcNotification,
    ListPromptsResult, ListResourceTemplatesResult, ListResourcesResult, ListToolsResult,
    McpServerBackend, McpServerError, McpServerRequestContext, MetaMap, Prompt, PromptArgument,
    PromptMessage, ReadResourceResult, Resource, ResourceContents, ResourceTemplate, Role,
    ServerCapabilities, ServerInfo, SubscriptionFilter, Task, TaskAcknowledgement, TaskStatus,
    Tool, ToolCallResult, UpdateTaskParams, MCP_PROTOCOL_VERSION, MCP_TASKS_EXTENSION_ID,
};
use insight_resources::actions::{
    ActionContext, ActionRegistry, RegisteredAction, ToolPublicArguments,
};
use insight_runtime::{RequestMetadata, RunService};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use sha2::{Digest as _, Sha256};
use uuid::Uuid;

use crate::config::{
    McpExportExecutionConfig, McpInteractionConfig, McpPromptMessageExportConfig,
    McpPromptRoleConfig, McpServerConfig,
};

#[derive(Clone)]
struct ExportedAction {
    action: Arc<RegisteredAction>,
    tool: Tool,
    required_scope: Option<String>,
}

#[derive(Clone)]
struct ExportedAgent {
    agent_id: String,
    execution: McpExportExecutionConfig,
    input_required: McpInteractionConfig,
    tool: Tool,
    required_scope: Option<String>,
}

#[derive(Clone)]
struct ExportedResource {
    resource: Resource,
    text: String,
    required_scope: Option<String>,
}

#[derive(Clone)]
struct ExportedPrompt {
    prompt: Prompt,
    messages: Vec<McpPromptMessageExportConfig>,
    completions: BTreeMap<String, Vec<String>>,
    required_scope: Option<String>,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct ServerToolContinuation {
    version: u8,
    run_id: String,
    tool_name: String,
    arguments_hash: String,
    expires_at: chrono::DateTime<Utc>,
}

#[derive(Clone)]
pub struct PlatformMcpServerBackend {
    actions: BTreeMap<String, ExportedAction>,
    agents: BTreeMap<String, ExportedAgent>,
    resources: BTreeMap<String, ExportedResource>,
    prompts: BTreeMap<String, ExportedPrompt>,
    run_service: RunService,
    task_repository: Arc<dyn McpServerTaskDurableRepository>,
    interaction_repository: Arc<dyn McpInteractionDurableRepository>,
    secret_protector: Option<Arc<dyn McpSecretProtector>>,
    operation_timeout: Duration,
}

impl PlatformMcpServerBackend {
    pub fn new(
        config: &McpServerConfig,
        actions: &ActionRegistry,
        run_service: RunService,
        task_repository: Arc<dyn McpServerTaskDurableRepository>,
        interaction_repository: Arc<dyn McpInteractionDurableRepository>,
        secret_protector: Option<Arc<dyn McpSecretProtector>>,
        operation_timeout: Duration,
    ) -> Result<Self, PlatformMcpServerError> {
        if !config.enabled || operation_timeout.is_zero() {
            return Err(PlatformMcpServerError::Configuration);
        }
        let mut exported_actions = BTreeMap::new();
        for export in &config.exports.actions {
            let action = actions
                .resolve(&export.action)
                .map_err(|_| PlatformMcpServerError::Export)?;
            let public = action.public_policy();
            if !public.call
                || matches!(public.arguments, ToolPublicArguments::Private)
                || public.result_schema.is_none()
            {
                return Err(PlatformMcpServerError::PrivateExport);
            }
            let input_schema =
                public_input_schema(&action.descriptor().input_schema, &public.arguments)?;
            let tool = Tool {
                name: export.alias.clone(),
                title: action.model_tool().title.clone(),
                description: action.model_tool().description.clone(),
                input_schema,
                output_schema: public.result_schema.clone(),
                annotations: None,
                metadata: None,
                icons: Vec::new(),
            };
            if exported_actions
                .insert(
                    export.alias.clone(),
                    ExportedAction {
                        action,
                        tool,
                        required_scope: export.required_scope.clone(),
                    },
                )
                .is_some()
            {
                return Err(PlatformMcpServerError::Export);
            }
        }
        let mut exported_agents = BTreeMap::new();
        for export in &config.exports.agents {
            if export.input_required == McpInteractionConfig::Allowed && secret_protector.is_none()
            {
                return Err(PlatformMcpServerError::SecretProtectionRequired);
            }
            let agent = run_service
                .agents()
                .get(&export.agent)
                .ok_or(PlatformMcpServerError::Export)?;
            let published = agent.published();
            let tool = Tool {
                name: export.alias.clone(),
                title: Some(published.metadata().name.clone()),
                description: (!published.metadata().description.is_empty())
                    .then(|| published.metadata().description.clone()),
                input_schema: published.public_input_schema(),
                output_schema: Some(published.public_output_schema()),
                annotations: None,
                metadata: None,
                icons: Vec::new(),
            };
            if exported_agents
                .insert(
                    export.alias.clone(),
                    ExportedAgent {
                        agent_id: export.agent.clone(),
                        execution: export.execution,
                        input_required: export.input_required,
                        tool,
                        required_scope: export.required_scope.clone(),
                    },
                )
                .is_some()
            {
                return Err(PlatformMcpServerError::Export);
            }
        }
        let resources = config
            .exports
            .resources
            .iter()
            .map(|export| {
                Ok((
                    export.uri.clone(),
                    ExportedResource {
                        resource: Resource {
                            uri: export.uri.clone(),
                            name: export.name.clone(),
                            title: export.title.clone(),
                            description: export.description.clone(),
                            mime_type: Some(export.mime_type.clone()),
                            size: Some(
                                u64::try_from(export.text.len())
                                    .map_err(|_| PlatformMcpServerError::Export)?,
                            ),
                            icons: Vec::new(),
                            annotations: None,
                            metadata: None,
                        },
                        text: export.text.clone(),
                        required_scope: export.required_scope.clone(),
                    },
                ))
            })
            .collect::<Result<BTreeMap<_, _>, PlatformMcpServerError>>()?;
        let prompts = config
            .exports
            .prompts
            .iter()
            .map(|export| {
                (
                    export.name.clone(),
                    ExportedPrompt {
                        prompt: Prompt {
                            name: export.name.clone(),
                            title: export.title.clone(),
                            description: export.description.clone(),
                            arguments: export
                                .arguments
                                .iter()
                                .map(|argument| PromptArgument {
                                    name: argument.name.clone(),
                                    title: argument.title.clone(),
                                    description: argument.description.clone(),
                                    required: Some(argument.required),
                                })
                                .collect(),
                            icons: Vec::new(),
                            metadata: None,
                        },
                        messages: export.messages.clone(),
                        completions: export.completions.clone(),
                        required_scope: export.required_scope.clone(),
                    },
                )
            })
            .collect();
        Ok(Self {
            actions: exported_actions,
            agents: exported_agents,
            resources,
            prompts,
            run_service,
            task_repository,
            interaction_repository,
            secret_protector,
            operation_timeout,
        })
    }

    fn tools(&self, context: &McpServerRequestContext) -> Vec<Tool> {
        let mut tools = self
            .actions
            .values()
            .filter(|export| scope_allows(context, export.required_scope.as_deref()))
            .map(|export| export.tool.clone())
            .chain(
                self.agents
                    .values()
                    .filter(|export| scope_allows(context, export.required_scope.as_deref()))
                    .map(|export| export.tool.clone()),
            )
            .collect::<Vec<_>>();
        tools.sort_by(|left, right| left.name.cmp(&right.name));
        tools
    }

    async fn requested_interactions(
        &self,
        principal: &McpInteractionPrincipal,
        run_id: &str,
    ) -> Result<Vec<McpInteraction>, McpServerError> {
        let interactions = self
            .interaction_repository
            .list_mcp_interactions(
                principal,
                &McpInteractionListFilter {
                    run_id: Some(run_id.to_owned()),
                    state: Some(McpInteractionState::Requested),
                    after_interaction_id: None,
                },
                33,
            )
            .await
            .map_err(|_| McpServerError::operation_failed())?;
        if interactions.len() > 32 {
            return Err(McpServerError::operation_failed());
        }
        Ok(interactions
            .into_iter()
            .filter(|interaction| interaction.deadline() > Utc::now())
            .collect())
    }

    async fn input_request_for_interaction(
        &self,
        interaction: &McpInteraction,
    ) -> Result<InputRequest, McpServerError> {
        let params = match interaction.request() {
            McpInteractionRequest::Form {
                message,
                requested_schema,
            } => ElicitRequestParams::Form {
                message: message.clone(),
                requested_schema: requested_schema.clone(),
            },
            McpInteractionRequest::Url { message, .. } => {
                let protector = self
                    .secret_protector
                    .as_ref()
                    .ok_or_else(McpServerError::operation_failed)?;
                let secret = self
                    .interaction_repository
                    .load_mcp_interaction_secret(interaction.interaction_id())
                    .await
                    .map_err(|_| McpServerError::operation_failed())?
                    .ok_or_else(McpServerError::operation_failed)?;
                let scope = McpSecretScope::new(
                    interaction.principal(),
                    interaction.server_id(),
                    interaction.interaction_id().as_str(),
                    McpSecretPurpose::ElicitationRequest,
                )
                .map_err(|_| McpServerError::operation_failed())?;
                let protected = McpProtectedSecret::new(secret.request_secret, secret.request_hash)
                    .map_err(|_| McpServerError::operation_failed())?;
                let plaintext = protector
                    .open(&scope, &protected)
                    .map_err(|_| McpServerError::operation_failed())?;
                let private: Value = serde_json::from_slice(&plaintext)
                    .map_err(|_| McpServerError::operation_failed())?;
                let url = private
                    .get("url")
                    .and_then(Value::as_str)
                    .filter(|url| url.len() <= 8 * 1024)
                    .ok_or_else(McpServerError::operation_failed)?;
                ElicitRequestParams::Url {
                    message: message.clone(),
                    url: url.to_owned(),
                }
            }
            McpInteractionRequest::Approval { .. }
            | McpInteractionRequest::Authorization { .. } => {
                return Err(McpServerError::missing_required_client_capability())
            }
        };
        Ok(InputRequest::ElicitationCreate(params))
    }

    async fn input_requests(
        &self,
        context: &McpServerRequestContext,
        interactions: &[McpInteraction],
    ) -> Result<BTreeMap<String, InputRequest>, McpServerError> {
        let mut requests = BTreeMap::new();
        for interaction in interactions {
            if !client_supports_interaction(context, interaction.request()) {
                return Err(McpServerError::missing_required_client_capability());
            }
            requests.insert(
                interaction.interaction_id().as_str().to_owned(),
                self.input_request_for_interaction(interaction).await?,
            );
        }
        Ok(requests)
    }

    fn continuation_scope(
        principal: &McpInteractionPrincipal,
        tool_name: &str,
        arguments_hash: &str,
    ) -> Result<McpSecretScope, McpServerError> {
        let authority_id = digest_hex(&json!({
            "tenant_id": principal.tenant_id(),
            "user_id": principal.user_id(),
            "tool_name": tool_name,
            "arguments_hash": arguments_hash,
        }))?;
        McpSecretScope::new(
            principal,
            "platform-server",
            authority_id,
            McpSecretPurpose::ServerContinuation,
        )
        .map_err(|_| McpServerError::operation_failed())
    }

    fn seal_continuation(
        &self,
        principal: &McpInteractionPrincipal,
        run_id: &str,
        tool_name: &str,
        arguments_hash: &str,
    ) -> Result<String, McpServerError> {
        let protector = self
            .secret_protector
            .as_ref()
            .ok_or_else(McpServerError::operation_failed)?;
        let state = ServerToolContinuation {
            version: 1,
            run_id: run_id.to_owned(),
            tool_name: tool_name.to_owned(),
            arguments_hash: arguments_hash.to_owned(),
            expires_at: Utc::now() + ChronoDuration::hours(24),
        };
        let plaintext =
            serde_jcs::to_vec(&state).map_err(|_| McpServerError::operation_failed())?;
        let scope = Self::continuation_scope(principal, tool_name, arguments_hash)?;
        let (ciphertext, content_hash) = protector
            .seal(&scope, &plaintext)
            .map_err(|_| McpServerError::operation_failed())?
            .into_parts();
        let encoded = format!(
            "mcp-state-v1.{content_hash}.{}",
            ciphertext.expose_ciphertext()
        );
        if encoded.len() > 16 * 1024 {
            return Err(McpServerError::operation_failed());
        }
        Ok(encoded)
    }

    fn open_continuation(
        &self,
        principal: &McpInteractionPrincipal,
        tool_name: &str,
        arguments_hash: &str,
        encoded: &str,
    ) -> Result<ServerToolContinuation, McpServerError> {
        let encoded = encoded
            .strip_prefix("mcp-state-v1.")
            .ok_or_else(McpServerError::invalid_params)?;
        let (content_hash, ciphertext) = encoded
            .split_once('.')
            .ok_or_else(McpServerError::invalid_params)?;
        let protected = McpProtectedSecret::new(
            McpSecretCiphertext::new(ciphertext).map_err(|_| McpServerError::invalid_params())?,
            content_hash,
        )
        .map_err(|_| McpServerError::invalid_params())?;
        let scope = Self::continuation_scope(principal, tool_name, arguments_hash)?;
        let plaintext = self
            .secret_protector
            .as_ref()
            .ok_or_else(McpServerError::operation_failed)?
            .open(&scope, &protected)
            .map_err(|_| McpServerError::invalid_params())?;
        let state: ServerToolContinuation =
            serde_json::from_slice(&plaintext).map_err(|_| McpServerError::invalid_params())?;
        if state.version != 1
            || state.tool_name != tool_name
            || state.arguments_hash != arguments_hash
            || state.expires_at <= Utc::now()
        {
            return Err(McpServerError::invalid_params());
        }
        Ok(state)
    }

    async fn resolve_run_inputs(
        &self,
        context: &McpServerRequestContext,
        principal: &McpInteractionPrincipal,
        run_id: &str,
        input_responses: BTreeMap<String, InputResponse>,
    ) -> Result<(), McpServerError> {
        let interactions = self.requested_interactions(principal, run_id).await?;
        if interactions.is_empty()
            || interactions.len() != input_responses.len()
            || interactions.iter().any(|interaction| {
                !input_responses.contains_key(interaction.interaction_id().as_str())
            })
        {
            return Err(McpServerError::invalid_params());
        }

        for interaction in interactions {
            let response = input_responses
                .get(interaction.interaction_id().as_str())
                .ok_or_else(McpServerError::invalid_params)?;
            let (disposition, response_value) =
                interaction_response(interaction.request(), response)?;
            let (response_secret, response_hash) = if let Some(response_value) = &response_value {
                let protector = self
                    .secret_protector
                    .as_ref()
                    .ok_or_else(McpServerError::operation_failed)?;
                let scope = McpSecretScope::new(
                    principal,
                    interaction.server_id(),
                    interaction.interaction_id().as_str(),
                    McpSecretPurpose::ElicitationResponse,
                )
                .map_err(|_| McpServerError::operation_failed())?;
                let plaintext = serde_jcs::to_vec(response_value)
                    .map_err(|_| McpServerError::invalid_params())?;
                let (secret, hash) = protector
                    .seal(&scope, &plaintext)
                    .map_err(|_| McpServerError::operation_failed())?
                    .into_parts();
                (Some(secret), Some(hash))
            } else {
                (None, None)
            };
            let mutation_id = digest_hex(&json!({
                "request_id": request_id(context),
                "interaction_id": interaction.interaction_id().as_str(),
                "response": response,
            }))?;
            let command = ResolveMcpInteractionCommand::new(
                &interaction,
                principal.clone(),
                format!("mcp-update-{mutation_id}"),
                interaction.version(),
                disposition,
                response_value.as_ref(),
                response_secret,
                response_hash,
                Utc::now(),
            )
            .map_err(|_| McpServerError::invalid_params())?;
            match self
                .interaction_repository
                .resolve_mcp_interaction(command)
                .await
                .map_err(|_| McpServerError::operation_failed())?
            {
                TransitionOutcome::Committed { .. } | TransitionOutcome::ExactReplay { .. } => {}
                TransitionOutcome::StateConflict | TransitionOutcome::StaleLease => {
                    return Err(McpServerError::invalid_params())
                }
            }
        }
        Ok(())
    }

    async fn call_action(
        &self,
        context: &McpServerRequestContext,
        export: &ExportedAction,
        arguments: Value,
    ) -> Result<Value, McpServerError> {
        if !scope_allows(context, export.required_scope.as_deref()) {
            return Err(McpServerError::method_not_found());
        }
        let (_, stop) = stop_pair();
        let request_id = request_id(context);
        let action_context = ActionContext::for_operation(
            format!("mcp_{request_id}"),
            format!("mcp_action_{}", export.tool.name),
            1,
            ExecutionControl::new(stop, self.operation_timeout),
        );
        match tokio::time::timeout(
            self.operation_timeout,
            export.action.call(arguments, action_context),
        )
        .await
        {
            Ok(Ok(output)) => {
                export
                    .action
                    .validate_public_result(&output)
                    .map_err(|_| McpServerError::operation_failed())?;
                tool_success(output)
            }
            Ok(Err(_)) => tool_failure(),
            Err(_) => Err(McpServerError::operation_failed()),
        }
    }

    async fn call_agent(
        &self,
        context: &McpServerRequestContext,
        export: &ExportedAgent,
        arguments: Value,
        input_responses: Option<BTreeMap<String, InputResponse>>,
        request_state: Option<String>,
    ) -> Result<Value, McpServerError> {
        if !scope_allows(context, export.required_scope.as_deref()) {
            return Err(McpServerError::method_not_found());
        }
        let supports_tasks = client_supports_tasks(context);
        let use_task = match export.execution {
            McpExportExecutionConfig::Synchronous => false,
            McpExportExecutionConfig::TaskPreferred => supports_tasks,
            McpExportExecutionConfig::TaskRequired if supports_tasks => true,
            McpExportExecutionConfig::TaskRequired => {
                return Err(McpServerError::missing_required_client_capability())
            }
        };
        if use_task {
            if input_responses.is_some() || request_state.is_some() {
                return Err(McpServerError::invalid_params());
            }
            return self.create_agent_task(context, export, arguments).await;
        }
        let principal = request_principal(context)?;
        let arguments_hash = digest_hex(&arguments)?;
        let run_id = match (request_state, input_responses) {
            (None, None) => {
                self.run_service
                    .create_detached_for_tenant(
                        principal.tenant_id(),
                        &export.agent_id,
                        arguments,
                        RequestMetadata {
                            request_id: Some(format!("mcp_{}", request_id(context))),
                        },
                    )
                    .await
                    .map_err(|_| McpServerError::operation_failed())?
                    .run_id
            }
            (Some(request_state), Some(input_responses))
                if export.input_required == McpInteractionConfig::Allowed =>
            {
                let continuation = self.open_continuation(
                    &principal,
                    &export.tool.name,
                    &arguments_hash,
                    &request_state,
                )?;
                self.run_service
                    .get_run_for_principal(
                        principal.tenant_id(),
                        Some(principal.user_id()),
                        &continuation.run_id,
                    )
                    .await
                    .map_err(|_| McpServerError::invalid_params())?;
                self.resolve_run_inputs(context, &principal, &continuation.run_id, input_responses)
                    .await?;
                continuation.run_id
            }
            _ => return Err(McpServerError::invalid_params()),
        };
        let deadline = Instant::now() + self.operation_timeout;
        loop {
            let run = self
                .run_service
                .get_run_for_principal(principal.tenant_id(), Some(principal.user_id()), &run_id)
                .await
                .map_err(|_| McpServerError::operation_failed())?;
            match run.lifecycle {
                RunLifecycle::Completed { output } => return tool_success(output.data),
                RunLifecycle::Failed { .. }
                | RunLifecycle::Cancelled { .. }
                | RunLifecycle::Interrupted { .. } => return tool_failure(),
                RunLifecycle::Created | RunLifecycle::Running => {}
            }
            let interactions = self.requested_interactions(&principal, &run_id).await?;
            if !interactions.is_empty() {
                if export.input_required != McpInteractionConfig::Allowed
                    || interactions.iter().any(|interaction| {
                        !client_supports_interaction(context, interaction.request())
                    })
                {
                    let _ = self
                        .run_service
                        .cancel_for_principal(
                            principal.tenant_id(),
                            Some(principal.user_id()),
                            &run_id,
                        )
                        .await;
                    return Err(McpServerError::missing_required_client_capability());
                }
                let input_requests = self.input_requests(context, &interactions).await?;
                let request_state = self.seal_continuation(
                    &principal,
                    &run_id,
                    &export.tool.name,
                    &arguments_hash,
                )?;
                return serde_json::to_value(InputRequiredResult {
                    result_type: "input_required".to_owned(),
                    input_requests,
                    request_state: Some(request_state),
                    metadata: None,
                })
                .map_err(|_| McpServerError::internal());
            }
            if Instant::now() >= deadline {
                let _ = self
                    .run_service
                    .cancel_for_principal(principal.tenant_id(), Some(principal.user_id()), &run_id)
                    .await;
                return Err(McpServerError::operation_failed());
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    }

    async fn create_agent_task(
        &self,
        context: &McpServerRequestContext,
        export: &ExportedAgent,
        arguments: Value,
    ) -> Result<Value, McpServerError> {
        const TASK_TTL_MS: u64 = 24 * 60 * 60 * 1_000;
        let principal = request_principal(context)?;
        let admitted = self
            .run_service
            .create_detached_for_tenant(
                principal.tenant_id(),
                &export.agent_id,
                arguments,
                RequestMetadata {
                    request_id: Some(format!("mcp_{}", request_id(context))),
                },
            )
            .await
            .map_err(|_| McpServerError::operation_failed())?;
        let created_at = Utc::now();
        let task_id = format!("task_{}", Uuid::new_v4().simple());
        let expires_at = created_at
            + ChronoDuration::milliseconds(
                i64::try_from(TASK_TTL_MS).map_err(|_| McpServerError::internal())?,
            );
        let task = McpServerTask::new(
            task_id.clone(),
            principal,
            admitted.run_id,
            export.tool.name.clone(),
            created_at,
            expires_at,
        )
        .map_err(|_| McpServerError::internal())?;
        if !self
            .task_repository
            .create_mcp_server_task(task)
            .await
            .map_err(|_| McpServerError::operation_failed())?
        {
            return Err(McpServerError::operation_failed());
        }
        serde_json::to_value(CreateTaskResult {
            result_type: "task".to_owned(),
            task: Task {
                task_id,
                status: TaskStatus::Working,
                status_message: Some("Agent run is in progress".to_owned()),
                created_at,
                last_updated_at: created_at,
                ttl_ms: Some(TASK_TTL_MS),
                poll_interval_ms: Some(500),
            },
            metadata: None,
        })
        .map_err(|_| McpServerError::internal())
    }

    async fn owned_task(
        &self,
        context: &McpServerRequestContext,
        task_id: &str,
    ) -> Result<(McpInteractionPrincipal, McpServerTask), McpServerError> {
        let principal = request_principal(context)?;
        let task = self
            .task_repository
            .load_mcp_server_task(&principal, task_id)
            .await
            .map_err(|_| McpServerError::operation_failed())?
            .ok_or_else(McpServerError::invalid_params)?;
        if task.expires_at() <= Utc::now() {
            self.run_service
                .cancel_for_principal(
                    principal.tenant_id(),
                    Some(principal.user_id()),
                    task.run_id(),
                )
                .await
                .map_err(|_| McpServerError::operation_failed())?;
            return Err(McpServerError::invalid_params());
        }
        Ok((principal, task))
    }

    async fn task_result(
        &self,
        context: &McpServerRequestContext,
        task_id: &str,
    ) -> Result<GetTaskResult, McpServerError> {
        const TASK_TTL_MS: u64 = 24 * 60 * 60 * 1_000;
        let (principal, task) = self.owned_task(context, task_id).await?;
        let export = self
            .agents
            .get(task.agent_id())
            .filter(|export| scope_allows(context, export.required_scope.as_deref()))
            .ok_or_else(McpServerError::method_not_found)?;
        let run = self
            .run_service
            .get_run_for_principal(
                principal.tenant_id(),
                Some(principal.user_id()),
                task.run_id(),
            )
            .await
            .map_err(|_| McpServerError::operation_failed())?;
        let requested = if matches!(run.lifecycle, RunLifecycle::Created | RunLifecycle::Running) {
            self.requested_interactions(&principal, task.run_id())
                .await?
        } else {
            Vec::new()
        };
        let (status, status_message, input_requests, result, error) = match run.lifecycle {
            RunLifecycle::Created | RunLifecycle::Running if !requested.is_empty() => {
                if export.input_required != McpInteractionConfig::Allowed {
                    let _ = self
                        .run_service
                        .cancel_for_principal(
                            principal.tenant_id(),
                            Some(principal.user_id()),
                            task.run_id(),
                        )
                        .await;
                    return Err(McpServerError::missing_required_client_capability());
                }
                (
                    TaskStatus::InputRequired,
                    Some("Agent run requires input".to_owned()),
                    Some(self.input_requests(context, &requested).await?),
                    None,
                    None,
                )
            }
            RunLifecycle::Created | RunLifecycle::Running => (
                TaskStatus::Working,
                Some("Agent run is in progress".to_owned()),
                None,
                None,
                None,
            ),
            RunLifecycle::Completed { output } => (
                TaskStatus::Completed,
                Some("Agent run completed".to_owned()),
                None,
                Some(tool_success(output.data)?),
                None,
            ),
            RunLifecycle::Cancelled { .. } => (
                TaskStatus::Cancelled,
                Some("Agent run was cancelled".to_owned()),
                None,
                None,
                None,
            ),
            RunLifecycle::Failed { .. } | RunLifecycle::Interrupted { .. } => (
                TaskStatus::Failed,
                Some("Agent run failed".to_owned()),
                None,
                None,
                Some(json!({
                    "code": -32010,
                    "message": "Exported operation failed"
                })),
            ),
        };
        Ok(GetTaskResult {
            result_type: "complete".to_owned(),
            task: Task {
                task_id: task.task_id().to_owned(),
                status,
                status_message,
                created_at: task.created_at(),
                last_updated_at: run.updated_at.max(task.created_at()),
                ttl_ms: Some(TASK_TTL_MS),
                poll_interval_ms: (!status.is_terminal()).then_some(500),
            },
            input_requests,
            result,
            error,
            metadata: None,
        })
    }
}

#[async_trait]
impl McpServerBackend for PlatformMcpServerBackend {
    async fn discover(
        &self,
        context: &McpServerRequestContext,
    ) -> Result<DiscoverResult, McpServerError> {
        let task_capability = self
            .agents
            .values()
            .any(|export| {
                export.execution != McpExportExecutionConfig::Synchronous
                    && scope_allows(context, export.required_scope.as_deref())
            })
            .then(|| {
                MetaMap::new(BTreeMap::from([(
                    MCP_TASKS_EXTENSION_ID.to_owned(),
                    json!({}),
                )]))
                .map_err(|_| McpServerError::internal())
            })
            .transpose()?;
        Ok(DiscoverResult {
            result_type: "complete".to_owned(),
            supported_versions: vec![MCP_PROTOCOL_VERSION.to_owned()],
            capabilities: ServerCapabilities {
                tools: (!self.tools(context).is_empty()).then(|| json!({"listChanged": false})),
                resources: self
                    .resources
                    .values()
                    .any(|export| scope_allows(context, export.required_scope.as_deref()))
                    .then(|| json!({"subscribe": true, "listChanged": false})),
                prompts: self
                    .prompts
                    .values()
                    .any(|export| scope_allows(context, export.required_scope.as_deref()))
                    .then(|| json!({"listChanged": false})),
                completions: self
                    .prompts
                    .values()
                    .any(|export| {
                        !export.completions.is_empty()
                            && scope_allows(context, export.required_scope.as_deref())
                    })
                    .then(|| json!({})),
                extensions: task_capability,
                ..ServerCapabilities::default()
            },
            server_info: ServerInfo {
                name: "insight-agent-platform".to_owned(),
                version: env!("CARGO_PKG_VERSION").to_owned(),
                title: Some("Insight Agent Platform".to_owned()),
                description: None,
                website_url: None,
                icons: Vec::new(),
            },
            ttl_ms: 30_000,
            cache_scope: CacheScope::Private,
            instructions: None,
            metadata: None,
        })
    }

    async fn list_tools(
        &self,
        context: &McpServerRequestContext,
        cursor: Option<String>,
    ) -> Result<ListToolsResult, McpServerError> {
        reject_cursor(cursor)?;
        Ok(ListToolsResult {
            result_type: "complete".to_owned(),
            tools: self.tools(context),
            next_cursor: None,
            ttl_ms: 30_000,
            cache_scope: CacheScope::Private,
            metadata: None,
        })
    }

    async fn call_tool(
        &self,
        context: &McpServerRequestContext,
        name: &str,
        arguments: Value,
        input_responses: Option<BTreeMap<String, InputResponse>>,
        request_state: Option<String>,
    ) -> Result<Value, McpServerError> {
        if let Some(export) = self.actions.get(name) {
            if input_responses.is_some() || request_state.is_some() {
                return Err(McpServerError::invalid_params());
            }
            return self.call_action(context, export, arguments).await;
        }
        if let Some(export) = self.agents.get(name) {
            return self
                .call_agent(context, export, arguments, input_responses, request_state)
                .await;
        }
        Err(McpServerError::method_not_found())
    }

    async fn list_resources(
        &self,
        context: &McpServerRequestContext,
        cursor: Option<String>,
    ) -> Result<ListResourcesResult, McpServerError> {
        reject_cursor(cursor)?;
        Ok(ListResourcesResult {
            result_type: "complete".to_owned(),
            resources: self
                .resources
                .values()
                .filter(|export| scope_allows(context, export.required_scope.as_deref()))
                .map(|export| export.resource.clone())
                .collect(),
            next_cursor: None,
            ttl_ms: 30_000,
            cache_scope: CacheScope::Private,
            metadata: None,
        })
    }

    async fn list_resource_templates(
        &self,
        _context: &McpServerRequestContext,
        cursor: Option<String>,
    ) -> Result<ListResourceTemplatesResult, McpServerError> {
        reject_cursor(cursor)?;
        Ok(ListResourceTemplatesResult {
            result_type: "complete".to_owned(),
            resource_templates: Vec::<ResourceTemplate>::new(),
            next_cursor: None,
            ttl_ms: 30_000,
            cache_scope: CacheScope::Private,
            metadata: None,
        })
    }

    async fn read_resource(
        &self,
        context: &McpServerRequestContext,
        uri: &str,
        input_responses: Option<BTreeMap<String, InputResponse>>,
        request_state: Option<String>,
    ) -> Result<Value, McpServerError> {
        if input_responses.is_some() || request_state.is_some() {
            return Err(McpServerError::invalid_params());
        }
        let export = self
            .resources
            .get(uri)
            .filter(|export| scope_allows(context, export.required_scope.as_deref()))
            .ok_or_else(McpServerError::method_not_found)?;
        serde_json::to_value(ReadResourceResult {
            result_type: "complete".to_owned(),
            contents: vec![ResourceContents::Text {
                uri: export.resource.uri.clone(),
                text: export.text.clone(),
                mime_type: export.resource.mime_type.clone(),
                metadata: None,
            }],
            ttl_ms: 30_000,
            cache_scope: CacheScope::Private,
            metadata: None,
        })
        .map_err(|_| McpServerError::internal())
    }

    async fn list_prompts(
        &self,
        context: &McpServerRequestContext,
        cursor: Option<String>,
    ) -> Result<ListPromptsResult, McpServerError> {
        reject_cursor(cursor)?;
        Ok(ListPromptsResult {
            result_type: "complete".to_owned(),
            prompts: self
                .prompts
                .values()
                .filter(|export| scope_allows(context, export.required_scope.as_deref()))
                .map(|export| export.prompt.clone())
                .collect(),
            next_cursor: None,
            ttl_ms: 30_000,
            cache_scope: CacheScope::Private,
            metadata: None,
        })
    }

    async fn get_prompt(
        &self,
        context: &McpServerRequestContext,
        name: &str,
        arguments: BTreeMap<String, String>,
        input_responses: Option<BTreeMap<String, InputResponse>>,
        request_state: Option<String>,
    ) -> Result<Value, McpServerError> {
        if input_responses.is_some() || request_state.is_some() {
            return Err(McpServerError::invalid_params());
        }
        let export = self
            .prompts
            .get(name)
            .filter(|export| scope_allows(context, export.required_scope.as_deref()))
            .ok_or_else(McpServerError::method_not_found)?;
        let declared = export
            .prompt
            .arguments
            .iter()
            .map(|argument| argument.name.as_str())
            .collect::<std::collections::BTreeSet<_>>();
        if arguments
            .keys()
            .any(|name| !declared.contains(name.as_str()))
            || export.prompt.arguments.iter().any(|argument| {
                argument.required == Some(true) && !arguments.contains_key(&argument.name)
            })
            || arguments
                .values()
                .any(|value| value.len() > 16 * 1024 || value.chars().any(char::is_control))
        {
            return Err(McpServerError::invalid_params());
        }
        let messages = export
            .messages
            .iter()
            .map(|message| PromptMessage {
                role: match message.role {
                    McpPromptRoleConfig::Assistant => Role::Assistant,
                    McpPromptRoleConfig::User => Role::User,
                },
                content: ContentBlock::Text {
                    text: render_prompt_message(&message.text, &arguments),
                    annotations: None,
                    metadata: None,
                },
            })
            .collect();
        serde_json::to_value(GetPromptResult {
            result_type: "complete".to_owned(),
            description: export.prompt.description.clone(),
            messages,
            metadata: None,
        })
        .map_err(|_| McpServerError::internal())
    }

    async fn complete(
        &self,
        context: &McpServerRequestContext,
        reference: CompletionReference,
        argument: CompletionArgument,
        arguments: BTreeMap<String, String>,
    ) -> Result<CompleteResult, McpServerError> {
        let values = match reference {
            CompletionReference::Prompt { name, .. } => {
                let export = self
                    .prompts
                    .get(&name)
                    .filter(|export| scope_allows(context, export.required_scope.as_deref()))
                    .ok_or_else(McpServerError::method_not_found)?;
                let declared = export
                    .prompt
                    .arguments
                    .iter()
                    .map(|argument| argument.name.as_str())
                    .collect::<std::collections::BTreeSet<_>>();
                if !declared.contains(argument.name.as_str())
                    || arguments
                        .keys()
                        .any(|name| !declared.contains(name.as_str()))
                {
                    return Err(McpServerError::invalid_params());
                }
                export
                    .completions
                    .get(&argument.name)
                    .into_iter()
                    .flatten()
                    .filter(|value| value.starts_with(&argument.value))
                    .take(100)
                    .cloned()
                    .collect::<Vec<_>>()
            }
            CompletionReference::Resource { .. } => Vec::new(),
        };
        Ok(CompleteResult {
            result_type: "complete".to_owned(),
            completion: Completion {
                total: Some(u64::try_from(values.len()).unwrap_or(100)),
                has_more: Some(false),
                values,
            },
            metadata: None,
        })
    }

    async fn listen_subscriptions(
        &self,
        context: &McpServerRequestContext,
        filter: &SubscriptionFilter,
    ) -> Result<(), McpServerError> {
        for task_id in &filter.task_ids {
            self.owned_task(context, task_id).await?;
        }
        if filter.resource_subscriptions.iter().any(|uri| {
            !self
                .resources
                .get(uri)
                .is_some_and(|export| scope_allows(context, export.required_scope.as_deref()))
        }) || (filter.tools_list_changed == Some(true) && self.tools(context).is_empty())
            || (filter.resources_list_changed == Some(true)
                && !self
                    .resources
                    .values()
                    .any(|export| scope_allows(context, export.required_scope.as_deref())))
            || (filter.prompts_list_changed == Some(true)
                && !self
                    .prompts
                    .values()
                    .any(|export| scope_allows(context, export.required_scope.as_deref())))
        {
            return Err(McpServerError::invalid_params());
        }
        (filter.tools_list_changed == Some(true)
            || filter.resources_list_changed == Some(true)
            || filter.prompts_list_changed == Some(true)
            || !filter.resource_subscriptions.is_empty()
            || !filter.task_ids.is_empty())
        .then_some(())
        .ok_or_else(McpServerError::invalid_params)
    }

    async fn subscription_notifications(
        &self,
        context: &McpServerRequestContext,
        filter: &SubscriptionFilter,
    ) -> Result<tokio::sync::mpsc::Receiver<JsonRpcNotification<Value>>, McpServerError> {
        self.listen_subscriptions(context, filter).await?;
        let (sender, receiver) = tokio::sync::mpsc::channel(16);
        if filter.task_ids.is_empty() {
            tokio::spawn(async move {
                sender.closed().await;
            });
            return Ok(receiver);
        }
        for task_id in &filter.task_ids {
            self.owned_task(context, task_id).await?;
        }
        let backend = self.clone();
        let context = context.clone();
        let task_ids = filter.task_ids.clone();
        tokio::spawn(async move {
            let mut observed = BTreeMap::<String, (TaskStatus, chrono::DateTime<Utc>)>::new();
            let mut interval = tokio::time::interval(Duration::from_millis(500));
            interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
            loop {
                tokio::select! {
                    _ = sender.closed() => break,
                    _ = interval.tick() => {}
                }
                let mut all_terminal = true;
                for task_id in &task_ids {
                    let Ok(result) = backend.task_result(&context, task_id).await else {
                        all_terminal = false;
                        continue;
                    };
                    all_terminal &= result.task.status.is_terminal();
                    let observed_state = (result.task.status, result.task.last_updated_at);
                    if observed.get(task_id) == Some(&observed_state) {
                        continue;
                    }
                    observed.insert(task_id.clone(), observed_state);
                    let params = serde_json::to_value(&result.task).ok();
                    let Some(params) = params else {
                        continue;
                    };
                    if sender
                        .send(JsonRpcNotification {
                            jsonrpc: "2.0".to_owned(),
                            method: "notifications/tasks/status".to_owned(),
                            params: Some(params),
                        })
                        .await
                        .is_err()
                    {
                        return;
                    }
                }
                if all_terminal {
                    break;
                }
            }
        });
        Ok(receiver)
    }

    async fn get_task(
        &self,
        context: &McpServerRequestContext,
        task_id: &str,
    ) -> Result<GetTaskResult, McpServerError> {
        self.task_result(context, task_id).await
    }

    async fn update_task(
        &self,
        context: &McpServerRequestContext,
        update: UpdateTaskParams,
    ) -> Result<TaskAcknowledgement, McpServerError> {
        let (principal, task) = self.owned_task(context, &update.task_id).await?;
        let export = self
            .agents
            .get(task.agent_id())
            .filter(|export| scope_allows(context, export.required_scope.as_deref()))
            .filter(|export| export.input_required == McpInteractionConfig::Allowed)
            .ok_or_else(McpServerError::method_not_found)?;
        if !client_supports_tasks(context)
            || export.execution == McpExportExecutionConfig::Synchronous
        {
            return Err(McpServerError::missing_required_client_capability());
        }
        self.resolve_run_inputs(context, &principal, task.run_id(), update.input_responses)
            .await?;
        Ok(TaskAcknowledgement::complete())
    }

    async fn cancel_task(
        &self,
        context: &McpServerRequestContext,
        task_id: &str,
    ) -> Result<TaskAcknowledgement, McpServerError> {
        let (principal, task) = self.owned_task(context, task_id).await?;
        self.run_service
            .cancel_for_principal(
                principal.tenant_id(),
                Some(principal.user_id()),
                task.run_id(),
            )
            .await
            .map_err(|_| McpServerError::operation_failed())?;
        Ok(TaskAcknowledgement::complete())
    }
}

fn public_input_schema(
    schema: &Value,
    arguments: &ToolPublicArguments,
) -> Result<Value, PlatformMcpServerError> {
    match arguments {
        ToolPublicArguments::All => Ok(schema.clone()),
        ToolPublicArguments::Private => Err(PlatformMcpServerError::PrivateExport),
        ToolPublicArguments::Fields(fields) => {
            let properties = schema
                .get("properties")
                .and_then(Value::as_object)
                .ok_or(PlatformMcpServerError::Export)?;
            let selected = fields
                .iter()
                .map(|field| {
                    properties
                        .get(field)
                        .cloned()
                        .map(|schema| (field.clone(), schema))
                        .ok_or(PlatformMcpServerError::Export)
                })
                .collect::<Result<serde_json::Map<_, _>, _>>()?;
            let required = schema
                .get("required")
                .and_then(Value::as_array)
                .into_iter()
                .flatten()
                .filter(|field| field.as_str().is_some_and(|field| fields.contains(field)))
                .cloned()
                .collect::<Vec<_>>();
            Ok(json!({
                "$schema": "https://json-schema.org/draft/2020-12/schema",
                "type": "object",
                "properties": selected,
                "required": required,
                "additionalProperties": false
            }))
        }
    }
}

fn request_principal(
    context: &McpServerRequestContext,
) -> Result<McpInteractionPrincipal, McpServerError> {
    let subject = context
        .authorization_subject
        .as_deref()
        .ok_or_else(McpServerError::unauthorized)?;
    let (tenant, user) = match subject.split_once('/') {
        Some((tenant, user)) if !user.contains('/') => (tenant, user),
        Some(_) => return Err(McpServerError::unauthorized()),
        None => (subject, "service"),
    };
    if tenant.is_empty() || user.is_empty() {
        return Err(McpServerError::unauthorized());
    }
    McpInteractionPrincipal::new(tenant, user).map_err(|_| McpServerError::unauthorized())
}

fn scope_allows(context: &McpServerRequestContext, required_scope: Option<&str>) -> bool {
    required_scope.is_none_or(|required| {
        context
            .authorization_scopes
            .as_ref()
            .is_some_and(|scopes| scopes.contains(required))
    })
}

fn render_prompt_message(template: &str, arguments: &BTreeMap<String, String>) -> String {
    let mut rendered = template.to_owned();
    for (name, value) in arguments {
        rendered = rendered.replace(&format!("{{{{{name}}}}}"), value);
    }
    // Optional arguments that were not supplied render as an empty string.
    while let Some(start) = rendered.find("{{") {
        let Some(end) = rendered[start + 2..].find("}}") else {
            break;
        };
        rendered.replace_range(start..start + 2 + end + 2, "");
    }
    rendered
}

fn client_supports_tasks(context: &McpServerRequestContext) -> bool {
    context
        .metadata
        .client_capabilities
        .extensions
        .as_ref()
        .and_then(|extensions| extensions.get(MCP_TASKS_EXTENSION_ID))
        .is_some()
}

fn client_supports_interaction(
    context: &McpServerRequestContext,
    request: &McpInteractionRequest,
) -> bool {
    let Some(elicitation) = context
        .metadata
        .client_capabilities
        .elicitation
        .as_ref()
        .and_then(Value::as_object)
    else {
        return false;
    };
    match request {
        McpInteractionRequest::Form { .. } => elicitation.contains_key("form"),
        McpInteractionRequest::Url { .. } => elicitation.contains_key("url"),
        McpInteractionRequest::Approval { .. } | McpInteractionRequest::Authorization { .. } => {
            false
        }
    }
}

fn interaction_response(
    request: &McpInteractionRequest,
    response: &InputResponse,
) -> Result<(McpInteractionDisposition, Option<Value>), McpServerError> {
    let InputResponse::Elicitation(response) = response;
    match (&response.action, request) {
        (ElicitAction::Accept, McpInteractionRequest::Form { .. }) => {
            let content = response
                .content
                .as_ref()
                .cloned()
                .map(serde_json::Map::from_iter)
                .map(Value::Object)
                .ok_or_else(McpServerError::invalid_params)?;
            request
                .validate_response(&content)
                .map_err(|_| McpServerError::invalid_params())?;
            Ok((McpInteractionDisposition::Accept, Some(content)))
        }
        (ElicitAction::Accept, McpInteractionRequest::Url { .. }) if response.content.is_none() => {
            Ok((
                McpInteractionDisposition::Accept,
                Some(Value::Object(serde_json::Map::new())),
            ))
        }
        (ElicitAction::Decline, _) if response.content.is_none() => {
            Ok((McpInteractionDisposition::Decline, None))
        }
        (ElicitAction::Cancel, _) if response.content.is_none() => {
            Ok((McpInteractionDisposition::Cancel, None))
        }
        _ => Err(McpServerError::invalid_params()),
    }
}

fn digest_hex(value: &Value) -> Result<String, McpServerError> {
    let canonical = serde_jcs::to_vec(value).map_err(|_| McpServerError::invalid_params())?;
    let digest = Sha256::digest(canonical);
    Ok(digest.iter().map(|byte| format!("{byte:02x}")).collect())
}

fn request_id(context: &McpServerRequestContext) -> String {
    serde_json::to_string(&context.request_id)
        .unwrap_or_else(|_| "request".to_owned())
        .replace('"', "")
}

fn tool_success(output: Value) -> Result<Value, McpServerError> {
    let text = serde_json::to_string(&output).map_err(|_| McpServerError::internal())?;
    serde_json::to_value(ToolCallResult {
        result_type: "complete".to_owned(),
        content: vec![ContentBlock::Text {
            text,
            annotations: None,
            metadata: None,
        }],
        structured_content: Some(output),
        is_error: Some(false),
        metadata: None,
    })
    .map_err(|_| McpServerError::internal())
}

fn tool_failure() -> Result<Value, McpServerError> {
    serde_json::to_value(ToolCallResult {
        result_type: "complete".to_owned(),
        content: vec![ContentBlock::Text {
            text: "Exported operation failed".to_owned(),
            annotations: None,
            metadata: None,
        }],
        structured_content: None,
        is_error: Some(true),
        metadata: None,
    })
    .map_err(|_| McpServerError::internal())
}

fn reject_cursor(cursor: Option<String>) -> Result<(), McpServerError> {
    if cursor.is_some() {
        Err(McpServerError::invalid_params())
    } else {
        Ok(())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PlatformMcpServerError {
    Configuration,
    Export,
    PrivateExport,
    SecretProtectionRequired,
}

impl std::fmt::Display for PlatformMcpServerError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(match self {
            Self::Configuration => "invalid platform MCP server configuration",
            Self::Export => "configured MCP export is unavailable",
            Self::PrivateExport => "configured MCP export lacks an explicit public projection",
            Self::SecretProtectionRequired => {
                "MCP Agent input-required export needs secret encryption"
            }
        })
    }
}

impl std::error::Error for PlatformMcpServerError {}

#[cfg(test)]
mod tests {
    use super::*;

    fn form_request() -> McpInteractionRequest {
        McpInteractionRequest::Form {
            message: "Choose a route".to_owned(),
            requested_schema: json!({
                "$schema": "https://json-schema.org/draft/2020-12/schema",
                "type": "object",
                "properties": {
                    "route": {
                        "type": "string",
                        "enum": ["safe", "fast"]
                    }
                },
                "required": ["route"],
                "additionalProperties": false
            }),
        }
    }

    #[test]
    fn task_input_response_preserves_elicitation_outcomes_and_validates_form_content() {
        let accepted = InputResponse::Elicitation(insight_mcp::ElicitResult {
            action: ElicitAction::Accept,
            content: Some(BTreeMap::from([(
                "route".to_owned(),
                Value::String("safe".to_owned()),
            )])),
        });
        let (disposition, value) = interaction_response(&form_request(), &accepted).unwrap();
        assert_eq!(disposition, McpInteractionDisposition::Accept);
        assert_eq!(value, Some(json!({"route": "safe"})));

        let invalid = InputResponse::Elicitation(insight_mcp::ElicitResult {
            action: ElicitAction::Accept,
            content: Some(BTreeMap::from([(
                "route".to_owned(),
                Value::String("unknown".to_owned()),
            )])),
        });
        assert!(interaction_response(&form_request(), &invalid).is_err());

        let declined = InputResponse::Elicitation(insight_mcp::ElicitResult {
            action: ElicitAction::Decline,
            content: None,
        });
        assert_eq!(
            interaction_response(&form_request(), &declined).unwrap(),
            (McpInteractionDisposition::Decline, None)
        );
    }

    #[test]
    fn continuation_digest_is_canonical_and_sensitive_to_arguments() {
        assert_eq!(
            digest_hex(&json!({"a": 1, "b": 2})).unwrap(),
            digest_hex(&json!({"b": 2, "a": 1})).unwrap()
        );
        assert_ne!(
            digest_hex(&json!({"a": 1})).unwrap(),
            digest_hex(&json!({"a": 2})).unwrap()
        );
    }
}
