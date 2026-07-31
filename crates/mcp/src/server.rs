//! Transport-neutral modern MCP server dispatcher.

use std::collections::{BTreeMap, BTreeSet};

use async_trait::async_trait;
use serde_json::{json, Map, Value};
use tokio::sync::mpsc;

use crate::{
    decode_header_value, valid_task_id, CompleteResult, CompletionArgument, CompletionReference,
    CoreMethod, DiscoverResult, GetPromptResult, GetTaskResult, InputRequiredResult, InputResponse,
    JsonRpcError, JsonRpcErrorResponse, JsonRpcNotification, JsonRpcResponse, ListPromptsResult,
    ListResourceTemplatesResult, ListResourcesResult, ListToolsResult, McpCodec, ModernRequest,
    ReadResourceResult, RequestId, RequestMetadata, SubscriptionFilter, TaskAcknowledgement,
    ToolCallResult, UpdateTaskParams, JSON_RPC_VERSION, MCP_PROTOCOL_VERSION,
    MCP_TASKS_EXTENSION_ID,
};

const MAX_CURSOR_BYTES: usize = 8 * 1024;
const MAX_NAME_BYTES: usize = 8 * 1024;
const MAX_REQUEST_STATE_BYTES: usize = 16 * 1024;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct McpRequestHeaders {
    pub protocol_version: String,
    pub method: String,
    pub name: Option<String>,
}

impl McpRequestHeaders {
    pub fn validate(&self, request: &ModernRequest) -> Result<(), McpServerError> {
        if self.protocol_version != MCP_PROTOCOL_VERSION || self.method != request.method.as_str() {
            return Err(McpServerError::invalid_request());
        }
        let expected_name = match request.method {
            CoreMethod::ToolsCall | CoreMethod::PromptsGet => {
                request.params.get("name").and_then(Value::as_str)
            }
            CoreMethod::ResourcesRead => request.params.get("uri").and_then(Value::as_str),
            CoreMethod::TasksGet | CoreMethod::TasksUpdate | CoreMethod::TasksCancel => {
                request.params.get("taskId").and_then(Value::as_str)
            }
            _ => None,
        };
        match (&self.name, expected_name) {
            (None, None) => Ok(()),
            (Some(header), Some(expected))
                if decode_header_value(header).ok().as_deref() == Some(expected) =>
            {
                Ok(())
            }
            _ => Err(McpServerError::invalid_request()),
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum McpServerReply {
    Json(Value),
    SubscriptionAcknowledgement(JsonRpcNotification<Value>),
}

#[derive(Debug, Clone)]
pub struct McpServerDispatcher<B> {
    backend: B,
    codec: McpCodec,
}

impl<B> McpServerDispatcher<B>
where
    B: McpServerBackend,
{
    pub fn new(backend: B) -> Self {
        Self {
            backend,
            codec: McpCodec::modern(),
        }
    }

    pub fn with_codec(backend: B, codec: McpCodec) -> Self {
        Self { backend, codec }
    }

    pub async fn dispatch(&self, body: &[u8], headers: &McpRequestHeaders) -> McpServerReply {
        self.dispatch_with_authorization_subject(body, headers, None)
            .await
    }

    pub async fn dispatch_with_authorization_subject(
        &self,
        body: &[u8],
        headers: &McpRequestHeaders,
        authorization_subject: Option<String>,
    ) -> McpServerReply {
        self.dispatch_with_authorization(body, headers, authorization_subject, None)
            .await
    }

    pub async fn dispatch_with_authorization(
        &self,
        body: &[u8],
        headers: &McpRequestHeaders,
        authorization_subject: Option<String>,
        authorization_scopes: Option<BTreeSet<String>>,
    ) -> McpServerReply {
        let started = std::time::Instant::now();
        let primitive = crate::observability::primitive(&headers.method);
        let reply = self
            .dispatch_with_authorization_inner(
                body,
                headers,
                authorization_subject,
                authorization_scopes,
            )
            .await;
        let outcome = match &reply {
            McpServerReply::Json(value) if value.get("error").is_some() => "protocol",
            McpServerReply::Json(_) | McpServerReply::SubscriptionAcknowledgement(_) => "success",
        };
        crate::observability::record_operation(
            primitive,
            "streamable_http_server",
            outcome,
            started.elapsed(),
        );
        reply
    }

    async fn dispatch_with_authorization_inner(
        &self,
        body: &[u8],
        headers: &McpRequestHeaders,
        authorization_subject: Option<String>,
        authorization_scopes: Option<BTreeSet<String>>,
    ) -> McpServerReply {
        let request = match self.codec.decode_request(body) {
            Ok(request) => request,
            Err(error) => {
                return McpServerReply::Json(error_response(
                    request_id_from_untrusted(body),
                    error.json_rpc_code(),
                    "MCP request rejected",
                ))
            }
        };
        if let Err(error) = headers.validate(&request) {
            return McpServerReply::Json(error_response(
                Some(request.id),
                error.code,
                error.message,
            ));
        }
        let id = request.id.clone();
        match self
            .dispatch_request(request, authorization_subject, authorization_scopes)
            .await
        {
            Ok(McpServerReply::Json(result)) => {
                let response = JsonRpcResponse {
                    jsonrpc: JSON_RPC_VERSION.to_owned(),
                    id,
                    result,
                };
                McpServerReply::Json(
                    serde_json::to_value(response)
                        .unwrap_or_else(|_| error_response(None, -32603, "MCP server failed")),
                )
            }
            Ok(reply @ McpServerReply::SubscriptionAcknowledgement(_)) => reply,
            Err(error) => McpServerReply::Json(error_response(Some(id), error.code, error.message)),
        }
    }

    pub async fn open_subscription_with_authorization_subject(
        &self,
        body: &[u8],
        headers: &McpRequestHeaders,
        authorization_subject: Option<String>,
    ) -> Result<mpsc::Receiver<JsonRpcNotification<Value>>, McpServerError> {
        self.open_subscription_with_authorization(body, headers, authorization_subject, None)
            .await
    }

    pub async fn open_subscription_with_authorization(
        &self,
        body: &[u8],
        headers: &McpRequestHeaders,
        authorization_subject: Option<String>,
        authorization_scopes: Option<BTreeSet<String>>,
    ) -> Result<mpsc::Receiver<JsonRpcNotification<Value>>, McpServerError> {
        let started = std::time::Instant::now();
        let result = async {
            let request = self
                .codec
                .decode_request(body)
                .map_err(|_| McpServerError::invalid_request())?;
            headers.validate(&request)?;
            if request.method != CoreMethod::SubscriptionsListen {
                return Err(McpServerError::invalid_request());
            }
            let context = McpServerRequestContext {
                request_id: request.id,
                metadata: request.metadata,
                authorization_subject,
                authorization_scopes,
            };
            let params = closed_params(&request.params, &["notifications"])?;
            let filter: SubscriptionFilter = from_value(
                params
                    .get("notifications")
                    .cloned()
                    .ok_or_else(McpServerError::invalid_params)?,
            )?;
            if !filter.task_ids.is_empty() {
                require_tasks_capability(&context)?;
                if filter
                    .task_ids
                    .iter()
                    .any(|task_id| !valid_task_id(task_id))
                {
                    return Err(McpServerError::invalid_params());
                }
            }
            self.backend
                .subscription_notifications(&context, &filter)
                .await
        }
        .await;
        crate::observability::record_operation(
            "subscriptions_listen",
            "streamable_http_server",
            if result.is_ok() {
                "success"
            } else {
                "protocol"
            },
            started.elapsed(),
        );
        result
    }

    async fn dispatch_request(
        &self,
        request: ModernRequest,
        authorization_subject: Option<String>,
        authorization_scopes: Option<BTreeSet<String>>,
    ) -> Result<McpServerReply, McpServerError> {
        let context = McpServerRequestContext {
            request_id: request.id.clone(),
            metadata: request.metadata,
            authorization_subject,
            authorization_scopes,
        };
        let result = match request.method {
            CoreMethod::ServerDiscover => {
                closed_params(&request.params, &[])?;
                to_value(self.backend.discover(&context).await?)?
            }
            CoreMethod::ToolsList => {
                let params = closed_params(&request.params, &["cursor"])?;
                to_value(self.backend.list_tools(&context, cursor(&params)?).await?)?
            }
            CoreMethod::ToolsCall => {
                let params = closed_params(
                    &request.params,
                    &["name", "arguments", "inputResponses", "requestState"],
                )?;
                let name = required_string(&params, "name", MAX_NAME_BYTES)?;
                let arguments = params
                    .get("arguments")
                    .cloned()
                    .unwrap_or_else(|| json!({}));
                if !arguments.is_object() {
                    return Err(McpServerError::invalid_params());
                }
                let result = self
                    .backend
                    .call_tool(
                        &context,
                        name,
                        arguments,
                        input_responses(&params)?,
                        request_state(&params)?,
                    )
                    .await?;
                if result.get("resultType").and_then(Value::as_str) == Some("task") {
                    require_tasks_capability(&context)?;
                    let task = from_value::<crate::CreateTaskResult>(result.clone())?;
                    task.validate().map_err(|_| McpServerError::internal())?;
                } else {
                    validate_interactive_result::<ToolCallResult>(&result)?;
                }
                result
            }
            CoreMethod::ResourcesList => {
                let params = closed_params(&request.params, &["cursor"])?;
                to_value(
                    self.backend
                        .list_resources(&context, cursor(&params)?)
                        .await?,
                )?
            }
            CoreMethod::ResourceTemplatesList => {
                let params = closed_params(&request.params, &["cursor"])?;
                to_value(
                    self.backend
                        .list_resource_templates(&context, cursor(&params)?)
                        .await?,
                )?
            }
            CoreMethod::ResourcesRead => {
                let params =
                    closed_params(&request.params, &["uri", "inputResponses", "requestState"])?;
                let uri = required_string(&params, "uri", MAX_NAME_BYTES)?;
                let result = self
                    .backend
                    .read_resource(
                        &context,
                        uri,
                        input_responses(&params)?,
                        request_state(&params)?,
                    )
                    .await?;
                validate_interactive_result::<ReadResourceResult>(&result)?;
                result
            }
            CoreMethod::PromptsList => {
                let params = closed_params(&request.params, &["cursor"])?;
                to_value(
                    self.backend
                        .list_prompts(&context, cursor(&params)?)
                        .await?,
                )?
            }
            CoreMethod::PromptsGet => {
                let params = closed_params(
                    &request.params,
                    &["name", "arguments", "inputResponses", "requestState"],
                )?;
                let name = required_string(&params, "name", MAX_NAME_BYTES)?;
                let arguments = string_map(params.get("arguments"))?;
                let result = self
                    .backend
                    .get_prompt(
                        &context,
                        name,
                        arguments,
                        input_responses(&params)?,
                        request_state(&params)?,
                    )
                    .await?;
                validate_interactive_result::<GetPromptResult>(&result)?;
                result
            }
            CoreMethod::CompletionComplete => {
                let params = closed_params(&request.params, &["ref", "argument", "context"])?;
                let reference = from_value(
                    params
                        .get("ref")
                        .cloned()
                        .ok_or_else(McpServerError::invalid_params)?,
                )?;
                let argument = from_value(
                    params
                        .get("argument")
                        .cloned()
                        .ok_or_else(McpServerError::invalid_params)?,
                )?;
                let context_arguments = params
                    .get("context")
                    .and_then(|value| value.get("arguments"));
                let result = self
                    .backend
                    .complete(
                        &context,
                        reference,
                        argument,
                        string_map(context_arguments)?,
                    )
                    .await?;
                to_value(result)?
            }
            CoreMethod::SubscriptionsListen => {
                let params = closed_params(&request.params, &["notifications"])?;
                let filter: SubscriptionFilter = from_value(
                    params
                        .get("notifications")
                        .cloned()
                        .ok_or_else(McpServerError::invalid_params)?,
                )?;
                if !filter.task_ids.is_empty() {
                    require_tasks_capability(&context)?;
                    if filter
                        .task_ids
                        .iter()
                        .any(|task_id| !valid_task_id(task_id))
                    {
                        return Err(McpServerError::invalid_params());
                    }
                }
                self.backend.listen_subscriptions(&context, &filter).await?;
                let subscription_id = serde_json::to_value(&context.request_id)
                    .map_err(|_| McpServerError::internal())?;
                return Ok(McpServerReply::SubscriptionAcknowledgement(
                    JsonRpcNotification {
                        jsonrpc: JSON_RPC_VERSION.to_owned(),
                        method: "notifications/subscriptions/acknowledged".to_owned(),
                        params: Some(json!({
                            "notifications": filter,
                            "_meta": {
                                "io.modelcontextprotocol/subscriptionId": subscription_id
                            }
                        })),
                    },
                ));
            }
            CoreMethod::TasksGet => {
                require_tasks_capability(&context)?;
                let params = closed_params(&request.params, &["taskId"])?;
                let task_id = required_string(&params, "taskId", MAX_NAME_BYTES)?;
                if !valid_task_id(task_id) {
                    return Err(McpServerError::invalid_params());
                }
                let result = self.backend.get_task(&context, task_id).await?;
                result.validate().map_err(|_| McpServerError::internal())?;
                to_value(result)?
            }
            CoreMethod::TasksUpdate => {
                require_tasks_capability(&context)?;
                let params = closed_params(&request.params, &["taskId", "inputResponses"])?;
                let update: UpdateTaskParams = from_value(Value::Object(params))?;
                update
                    .validate()
                    .map_err(|_| McpServerError::invalid_params())?;
                let result = self.backend.update_task(&context, update).await?;
                result.validate().map_err(|_| McpServerError::internal())?;
                to_value(result)?
            }
            CoreMethod::TasksCancel => {
                require_tasks_capability(&context)?;
                let params = closed_params(&request.params, &["taskId"])?;
                let task_id = required_string(&params, "taskId", MAX_NAME_BYTES)?;
                if !valid_task_id(task_id) {
                    return Err(McpServerError::invalid_params());
                }
                let result = self.backend.cancel_task(&context, task_id).await?;
                result.validate().map_err(|_| McpServerError::internal())?;
                to_value(result)?
            }
        };
        Ok(McpServerReply::Json(result))
    }
}

#[derive(Debug, Clone)]
pub struct McpServerRequestContext {
    pub request_id: RequestId,
    pub metadata: RequestMetadata,
    pub authorization_subject: Option<String>,
    /// `None` represents a trusted non-OAuth authorizer. OAuth resource
    /// servers provide the exact validated access-token scope set.
    pub authorization_scopes: Option<BTreeSet<String>>,
}

#[async_trait]
pub trait McpServerBackend: Send + Sync {
    async fn discover(
        &self,
        context: &McpServerRequestContext,
    ) -> Result<DiscoverResult, McpServerError>;
    async fn list_tools(
        &self,
        context: &McpServerRequestContext,
        cursor: Option<String>,
    ) -> Result<ListToolsResult, McpServerError>;
    async fn call_tool(
        &self,
        context: &McpServerRequestContext,
        name: &str,
        arguments: Value,
        input_responses: Option<BTreeMap<String, InputResponse>>,
        request_state: Option<String>,
    ) -> Result<Value, McpServerError>;
    async fn list_resources(
        &self,
        context: &McpServerRequestContext,
        cursor: Option<String>,
    ) -> Result<ListResourcesResult, McpServerError>;
    async fn list_resource_templates(
        &self,
        context: &McpServerRequestContext,
        cursor: Option<String>,
    ) -> Result<ListResourceTemplatesResult, McpServerError>;
    async fn read_resource(
        &self,
        context: &McpServerRequestContext,
        uri: &str,
        input_responses: Option<BTreeMap<String, InputResponse>>,
        request_state: Option<String>,
    ) -> Result<Value, McpServerError>;
    async fn list_prompts(
        &self,
        context: &McpServerRequestContext,
        cursor: Option<String>,
    ) -> Result<ListPromptsResult, McpServerError>;
    async fn get_prompt(
        &self,
        context: &McpServerRequestContext,
        name: &str,
        arguments: BTreeMap<String, String>,
        input_responses: Option<BTreeMap<String, InputResponse>>,
        request_state: Option<String>,
    ) -> Result<Value, McpServerError>;
    async fn complete(
        &self,
        context: &McpServerRequestContext,
        reference: CompletionReference,
        argument: CompletionArgument,
        arguments: BTreeMap<String, String>,
    ) -> Result<CompleteResult, McpServerError>;
    async fn listen_subscriptions(
        &self,
        context: &McpServerRequestContext,
        filter: &SubscriptionFilter,
    ) -> Result<(), McpServerError>;
    async fn subscription_notifications(
        &self,
        _context: &McpServerRequestContext,
        _filter: &SubscriptionFilter,
    ) -> Result<mpsc::Receiver<JsonRpcNotification<Value>>, McpServerError> {
        let (sender, receiver) = mpsc::channel(1);
        tokio::spawn(async move {
            sender.closed().await;
        });
        Ok(receiver)
    }
    async fn get_task(
        &self,
        _context: &McpServerRequestContext,
        _task_id: &str,
    ) -> Result<GetTaskResult, McpServerError> {
        Err(McpServerError::method_not_found())
    }
    async fn update_task(
        &self,
        _context: &McpServerRequestContext,
        _update: UpdateTaskParams,
    ) -> Result<TaskAcknowledgement, McpServerError> {
        Err(McpServerError::method_not_found())
    }
    async fn cancel_task(
        &self,
        _context: &McpServerRequestContext,
        _task_id: &str,
    ) -> Result<TaskAcknowledgement, McpServerError> {
        Err(McpServerError::method_not_found())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct McpServerError {
    code: i64,
    message: &'static str,
}

impl McpServerError {
    pub const fn invalid_request() -> Self {
        Self {
            code: -32600,
            message: "MCP request rejected",
        }
    }

    pub const fn method_not_found() -> Self {
        Self {
            code: -32601,
            message: "MCP method is unavailable",
        }
    }

    pub const fn invalid_params() -> Self {
        Self {
            code: -32602,
            message: "MCP request parameters are invalid",
        }
    }

    pub const fn unauthorized() -> Self {
        Self {
            code: -32001,
            message: "MCP authorization is required",
        }
    }

    pub const fn operation_failed() -> Self {
        Self {
            code: -32010,
            message: "MCP operation failed",
        }
    }

    pub const fn missing_required_client_capability() -> Self {
        Self {
            code: -32003,
            message: "Missing required client capability",
        }
    }

    pub const fn internal() -> Self {
        Self {
            code: -32603,
            message: "MCP server failed",
        }
    }

    pub const fn code(self) -> i64 {
        self.code
    }

    pub const fn message(self) -> &'static str {
        self.message
    }
}

impl std::fmt::Display for McpServerError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(self.message)
    }
}

impl std::error::Error for McpServerError {}

fn closed_params(params: &Value, allowed: &[&str]) -> Result<Map<String, Value>, McpServerError> {
    let mut object = params
        .as_object()
        .cloned()
        .ok_or_else(McpServerError::invalid_params)?;
    object.remove("_meta");
    if object.keys().any(|key| !allowed.contains(&key.as_str())) {
        return Err(McpServerError::invalid_params());
    }
    Ok(object)
}

fn cursor(params: &Map<String, Value>) -> Result<Option<String>, McpServerError> {
    optional_string(params, "cursor", MAX_CURSOR_BYTES)
}

fn request_state(params: &Map<String, Value>) -> Result<Option<String>, McpServerError> {
    optional_string(params, "requestState", MAX_REQUEST_STATE_BYTES)
}

fn required_string<'a>(
    params: &'a Map<String, Value>,
    name: &str,
    max_bytes: usize,
) -> Result<&'a str, McpServerError> {
    params
        .get(name)
        .and_then(Value::as_str)
        .filter(|value| !value.is_empty() && value.len() <= max_bytes)
        .ok_or_else(McpServerError::invalid_params)
}

fn optional_string(
    params: &Map<String, Value>,
    name: &str,
    max_bytes: usize,
) -> Result<Option<String>, McpServerError> {
    params
        .get(name)
        .map(|_| required_string(params, name, max_bytes).map(str::to_owned))
        .transpose()
}

fn input_responses(
    params: &Map<String, Value>,
) -> Result<Option<BTreeMap<String, InputResponse>>, McpServerError> {
    params
        .get("inputResponses")
        .cloned()
        .map(from_value)
        .transpose()
}

fn string_map(value: Option<&Value>) -> Result<BTreeMap<String, String>, McpServerError> {
    value
        .cloned()
        .map(from_value)
        .transpose()
        .map(Option::unwrap_or_default)
}

fn validate_interactive_result<T>(value: &Value) -> Result<(), McpServerError>
where
    T: serde::de::DeserializeOwned,
{
    match value.get("resultType").and_then(Value::as_str) {
        Some("complete") => {
            from_value::<T>(value.clone())?;
            Ok(())
        }
        Some("input_required") => {
            let result = from_value::<InputRequiredResult>(value.clone())?;
            result.validate().map_err(|_| McpServerError::internal())
        }
        _ => Err(McpServerError::internal()),
    }
}

fn require_tasks_capability(context: &McpServerRequestContext) -> Result<(), McpServerError> {
    let declared = context
        .metadata
        .client_capabilities
        .extensions
        .as_ref()
        .and_then(|extensions| extensions.get(MCP_TASKS_EXTENSION_ID))
        .is_some();
    if declared {
        Ok(())
    } else {
        Err(McpServerError::missing_required_client_capability())
    }
}

fn from_value<T: serde::de::DeserializeOwned>(value: Value) -> Result<T, McpServerError> {
    serde_json::from_value(value).map_err(|_| McpServerError::invalid_params())
}

fn to_value<T: serde::Serialize>(value: T) -> Result<Value, McpServerError> {
    serde_json::to_value(value).map_err(|_| McpServerError::internal())
}

fn request_id_from_untrusted(body: &[u8]) -> Option<RequestId> {
    serde_json::from_slice::<Value>(body)
        .ok()
        .and_then(|value| value.get("id").cloned())
        .and_then(|value| serde_json::from_value(value).ok())
}

fn error_response(id: Option<RequestId>, code: i64, message: &'static str) -> Value {
    serde_json::to_value(JsonRpcErrorResponse {
        jsonrpc: JSON_RPC_VERSION.to_owned(),
        id,
        error: JsonRpcError {
            code,
            message: message.to_owned(),
            data: (code == -32003).then(|| {
                json!({
                    "requiredCapabilities": [MCP_TASKS_EXTENSION_ID]
                })
            }),
        },
    })
    .unwrap_or_else(|_| {
        json!({"jsonrpc": JSON_RPC_VERSION, "id": null, "error": {
            "code": -32603, "message": "MCP server failed"
        }})
    })
}
