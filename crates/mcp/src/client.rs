use std::{
    collections::{BTreeMap, BTreeSet},
    fmt::Write as _,
    sync::{
        atomic::{AtomicI64, Ordering},
        Arc,
    },
};

use base64::{engine::general_purpose::STANDARD as BASE64_STANDARD, Engine as _};
use jsonschema::{Draft, Validator};
use reqwest::Url;
use serde::de::DeserializeOwned;
use serde::Serialize;
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use tokio_util::sync::CancellationToken;

use crate::{
    valid_task_id, CacheScope, ClientCapabilities, ClientInfo, CompleteResult, CompletionArgument,
    CompletionReference, CoreMethod, CreateTaskResult, DiscoverResult, GetPromptResult,
    GetTaskResult, InputRequiredResult, InputResponse, JsonRpcErrorResponse, JsonRpcRequest,
    ListPromptsResult, ListResourceTemplatesResult, ListResourcesResult, ListToolsResult, McpCodec,
    McpNotificationObserver, McpTransport, Prompt, ProtocolLimits, ReadResourceResult, RequestId,
    RequestMetadata, Resource, ResourceContents, ResourceTemplate, SubscriptionFilter,
    TaskAcknowledgement, Tool, ToolCallResult, ToolHeaderPlan, TransportError, TransportKind,
    UpdateTaskParams, MCP_PROTOCOL_VERSION, MCP_TASKS_EXTENSION_ID,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct McpClientLimits {
    pub max_list_pages: usize,
    pub max_tools: usize,
    pub max_tool_name_bytes: usize,
    pub max_content_items: usize,
    pub max_content_bytes: usize,
    pub max_request_state_bytes: usize,
    pub max_resources: usize,
    pub max_prompts: usize,
    pub max_uri_bytes: usize,
    pub max_completion_values: usize,
    pub max_completion_value_bytes: usize,
}

impl Default for McpClientLimits {
    fn default() -> Self {
        Self {
            max_list_pages: 128,
            max_tools: 4_096,
            max_tool_name_bytes: 128,
            max_content_items: 256,
            max_content_bytes: 4 * 1024 * 1024,
            max_request_state_bytes: 16 * 1024,
            max_resources: 4_096,
            max_prompts: 4_096,
            max_uri_bytes: 8 * 1024,
            max_completion_values: 100,
            max_completion_value_bytes: 4 * 1024,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ToolCatalogRejection {
    pub tool_name: String,
    pub code: &'static str,
}

#[derive(Debug, Clone, PartialEq)]
pub struct ToolCatalog {
    pub tools: Vec<Tool>,
    pub rejected: Vec<ToolCatalogRejection>,
    pub ttl_ms: u64,
    pub cache_scope: CacheScope,
    pub descriptor_hash: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CatalogRejection {
    pub identity: String,
    pub code: &'static str,
}

#[derive(Debug, Clone, PartialEq)]
pub struct McpCatalog<T> {
    pub items: Vec<T>,
    pub rejected: Vec<CatalogRejection>,
    pub ttl_ms: u64,
    pub cache_scope: CacheScope,
    pub descriptor_hash: String,
}

pub type ResourceCatalog = McpCatalog<Resource>;
pub type ResourceTemplateCatalog = McpCatalog<ResourceTemplate>;
pub type PromptCatalog = McpCatalog<Prompt>;

#[derive(Serialize)]
struct CanonicalCatalog<'a, T> {
    protocol_version: &'static str,
    kind: &'static str,
    items: &'a [T],
    ttl_ms: u64,
    cache_scope: CacheScope,
}

impl<T: Serialize> McpCatalog<T> {
    pub fn empty(kind: &'static str) -> Result<Self, ClientError> {
        let canonical = serde_jcs::to_vec(&CanonicalCatalog::<T> {
            protocol_version: MCP_PROTOCOL_VERSION,
            kind,
            items: &[],
            ttl_ms: 0,
            cache_scope: CacheScope::Private,
        })
        .map_err(|_| ClientError::Catalog)?;
        Ok(Self {
            items: Vec::new(),
            rejected: Vec::new(),
            ttl_ms: 0,
            cache_scope: CacheScope::Private,
            descriptor_hash: hex_sha256(&canonical),
        })
    }

    fn seal(
        kind: &'static str,
        mut items: Vec<T>,
        mut rejected: Vec<CatalogRejection>,
        ttl_ms: u64,
        cache_scope: CacheScope,
        identity: impl Fn(&T) -> &str,
    ) -> Result<Self, ClientError> {
        items.sort_by(|left, right| identity(left).as_bytes().cmp(identity(right).as_bytes()));
        rejected.sort_by(|left, right| {
            left.identity
                .as_bytes()
                .cmp(right.identity.as_bytes())
                .then_with(|| left.code.cmp(right.code))
        });
        let canonical = serde_jcs::to_vec(&CanonicalCatalog {
            protocol_version: MCP_PROTOCOL_VERSION,
            kind,
            items: &items,
            ttl_ms,
            cache_scope,
        })
        .map_err(|_| ClientError::Catalog)?;
        Ok(Self {
            items,
            rejected,
            ttl_ms,
            cache_scope,
            descriptor_hash: hex_sha256(&canonical),
        })
    }
}

#[derive(Serialize)]
struct CanonicalToolCatalog<'a> {
    protocol_version: &'static str,
    tools: &'a [Tool],
    ttl_ms: u64,
    cache_scope: CacheScope,
}

impl ToolCatalog {
    fn seal(
        mut tools: Vec<Tool>,
        mut rejected: Vec<ToolCatalogRejection>,
        ttl_ms: u64,
        cache_scope: CacheScope,
    ) -> Result<Self, ClientError> {
        tools.sort_by(|left, right| left.name.as_bytes().cmp(right.name.as_bytes()));
        rejected.sort_by(|left, right| {
            left.tool_name
                .as_bytes()
                .cmp(right.tool_name.as_bytes())
                .then_with(|| left.code.cmp(right.code))
        });
        let canonical = serde_jcs::to_vec(&CanonicalToolCatalog {
            protocol_version: MCP_PROTOCOL_VERSION,
            tools: &tools,
            ttl_ms,
            cache_scope,
        })
        .map_err(|_| ClientError::Catalog)?;
        Ok(Self {
            tools,
            rejected,
            ttl_ms,
            cache_scope,
            descriptor_hash: hex_sha256(&canonical),
        })
    }
}

trait CachedPage<T> {
    fn into_parts(self) -> (String, Vec<T>, Option<String>, u64, CacheScope);
}

struct CachedListPolicy<T> {
    kind: &'static str,
    max_items: usize,
    identity: fn(&T) -> &str,
    validate: fn(&McpClient, &T) -> Result<(), &'static str>,
}

impl CachedPage<Resource> for ListResourcesResult {
    fn into_parts(self) -> (String, Vec<Resource>, Option<String>, u64, CacheScope) {
        (
            self.result_type,
            self.resources,
            self.next_cursor,
            self.ttl_ms,
            self.cache_scope,
        )
    }
}

impl CachedPage<ResourceTemplate> for ListResourceTemplatesResult {
    fn into_parts(
        self,
    ) -> (
        String,
        Vec<ResourceTemplate>,
        Option<String>,
        u64,
        CacheScope,
    ) {
        (
            self.result_type,
            self.resource_templates,
            self.next_cursor,
            self.ttl_ms,
            self.cache_scope,
        )
    }
}

impl CachedPage<Prompt> for ListPromptsResult {
    fn into_parts(self) -> (String, Vec<Prompt>, Option<String>, u64, CacheScope) {
        (
            self.result_type,
            self.prompts,
            self.next_cursor,
            self.ttl_ms,
            self.cache_scope,
        )
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum McpCallOutcome {
    Complete(ToolCallResult),
    InputRequired(InputRequiredResult),
    Task(CreateTaskResult),
}

#[derive(Debug, Clone, PartialEq)]
pub enum McpReadResourceOutcome {
    Complete(ReadResourceResult),
    InputRequired(InputRequiredResult),
}

#[derive(Debug, Clone, PartialEq)]
pub enum McpGetPromptOutcome {
    Complete(GetPromptResult),
    InputRequired(InputRequiredResult),
}

#[derive(Clone)]
pub struct McpClient {
    transport: Arc<dyn McpTransport>,
    client_info: ClientInfo,
    capabilities: ClientCapabilities,
    limits: McpClientLimits,
    next_request_id: Arc<AtomicI64>,
    codec: McpCodec,
    protocol_version: String,
}

impl McpClient {
    pub fn new(
        transport: Arc<dyn McpTransport>,
        client_info: ClientInfo,
        capabilities: ClientCapabilities,
    ) -> Result<Self, ClientError> {
        if client_info.name.is_empty()
            || client_info.name.len() > 256
            || client_info.version.is_empty()
            || client_info.version.len() > 128
        {
            return Err(ClientError::Configuration);
        }
        Ok(Self {
            transport,
            client_info,
            capabilities,
            limits: McpClientLimits::default(),
            next_request_id: Arc::new(AtomicI64::new(1)),
            codec: McpCodec::modern(),
            protocol_version: MCP_PROTOCOL_VERSION.to_owned(),
        })
    }

    /// Constructs the internal client facade used by the isolated
    /// `2025-11-25` compatibility transport. The compatibility transport owns
    /// the legacy handshake/session and removes this per-request metadata
    /// before anything reaches the legacy peer.
    pub fn new_legacy(
        transport: Arc<dyn McpTransport>,
        client_info: ClientInfo,
        capabilities: ClientCapabilities,
    ) -> Result<Self, ClientError> {
        let mut client = Self::new(transport, client_info, capabilities)?;
        client.protocol_version = crate::MCP_LEGACY_PROTOCOL_VERSION.to_owned();
        Ok(client)
    }

    pub fn protocol_version(&self) -> &str {
        &self.protocol_version
    }

    pub fn with_limits(mut self, limits: McpClientLimits) -> Result<Self, ClientError> {
        if limits.max_list_pages == 0
            || limits.max_tools == 0
            || limits.max_tool_name_bytes == 0
            || limits.max_content_items == 0
            || limits.max_content_bytes == 0
            || limits.max_request_state_bytes == 0
            || limits.max_resources == 0
            || limits.max_prompts == 0
            || limits.max_uri_bytes == 0
            || limits.max_completion_values == 0
            || limits.max_completion_values > 100
            || limits.max_completion_value_bytes == 0
        {
            return Err(ClientError::Configuration);
        }
        self.limits = limits;
        Ok(self)
    }

    pub fn with_protocol_limits(mut self, limits: ProtocolLimits) -> Result<Self, ClientError> {
        self.codec = McpCodec::new(limits).map_err(|_| ClientError::Configuration)?;
        Ok(self)
    }

    pub fn transport_kind(&self) -> TransportKind {
        self.transport.kind()
    }

    pub async fn discover(
        &self,
        cancellation: &CancellationToken,
        observer: &dyn McpNotificationObserver,
    ) -> Result<DiscoverResult, ClientError> {
        let request = self.request(CoreMethod::ServerDiscover, json!({}))?;
        let result: DiscoverResult = self
            .exchange(&request, &BTreeMap::new(), cancellation, observer)
            .await?;
        if result.result_type != "complete"
            || !result
                .supported_versions
                .iter()
                .any(|version| version == &self.protocol_version)
            || result.server_info.name.is_empty()
            || result.server_info.name.len() > 256
            || result.server_info.version.is_empty()
            || result.server_info.version.len() > 128
            || result.supported_versions.is_empty()
            || result.supported_versions.len() > 32
            || result
                .instructions
                .as_ref()
                .is_some_and(|value| value.len() > 64 * 1024)
        {
            return Err(ClientError::Discovery);
        }
        Ok(result)
    }

    pub async fn list_tools(
        &self,
        cancellation: &CancellationToken,
        observer: &dyn McpNotificationObserver,
    ) -> Result<ToolCatalog, ClientError> {
        let mut cursor: Option<String> = None;
        let mut seen_cursors = BTreeSet::new();
        let mut seen_names = BTreeSet::new();
        let mut accepted = Vec::new();
        let mut rejected = Vec::new();
        let mut cache_scope = None;
        let mut ttl_ms = None;

        for _ in 0..self.limits.max_list_pages {
            let mut params = serde_json::Map::new();
            if let Some(value) = cursor.as_ref() {
                params.insert("cursor".to_owned(), Value::String(value.clone()));
            }
            let request = self.request(CoreMethod::ToolsList, Value::Object(params))?;
            let page: ListToolsResult = self
                .exchange(&request, &BTreeMap::new(), cancellation, observer)
                .await?;
            if page.result_type != "complete"
                || cache_scope.is_some_and(|scope| scope != page.cache_scope)
            {
                return Err(ClientError::Catalog);
            }
            cache_scope = Some(page.cache_scope);
            ttl_ms = Some(ttl_ms.map_or(page.ttl_ms, |current: u64| current.min(page.ttl_ms)));

            for tool in page.tools {
                if accepted.len() >= self.limits.max_tools {
                    return Err(ClientError::CatalogLimit);
                }
                let name = tool.name.clone();
                if !seen_names.insert(name.clone()) {
                    return Err(ClientError::Catalog);
                }
                match self.validate_tool(&tool) {
                    Ok(()) => accepted.push(tool),
                    Err(code) => rejected.push(ToolCatalogRejection {
                        tool_name: name,
                        code,
                    }),
                }
            }
            let Some(next) = page.next_cursor else {
                return ToolCatalog::seal(
                    accepted,
                    rejected,
                    ttl_ms.unwrap_or(0),
                    cache_scope.unwrap_or(CacheScope::Private),
                );
            };
            if next.is_empty() || next.len() > 8 * 1024 || !seen_cursors.insert(next.clone()) {
                return Err(ClientError::Pagination);
            }
            cursor = Some(next);
        }
        Err(ClientError::Pagination)
    }

    pub async fn list_resources(
        &self,
        cancellation: &CancellationToken,
        observer: &dyn McpNotificationObserver,
    ) -> Result<ResourceCatalog, ClientError> {
        self.list_cached::<ListResourcesResult, Resource>(
            CoreMethod::ResourcesList,
            CachedListPolicy {
                kind: "resources",
                max_items: self.limits.max_resources,
                identity: resource_identity,
                validate: Self::validate_resource,
            },
            cancellation,
            observer,
        )
        .await
    }

    pub async fn list_resource_templates(
        &self,
        cancellation: &CancellationToken,
        observer: &dyn McpNotificationObserver,
    ) -> Result<ResourceTemplateCatalog, ClientError> {
        self.list_cached::<ListResourceTemplatesResult, ResourceTemplate>(
            CoreMethod::ResourceTemplatesList,
            CachedListPolicy {
                kind: "resource_templates",
                max_items: self.limits.max_resources,
                identity: resource_template_identity,
                validate: Self::validate_resource_template,
            },
            cancellation,
            observer,
        )
        .await
    }

    pub async fn list_prompts(
        &self,
        cancellation: &CancellationToken,
        observer: &dyn McpNotificationObserver,
    ) -> Result<PromptCatalog, ClientError> {
        self.list_cached::<ListPromptsResult, Prompt>(
            CoreMethod::PromptsList,
            CachedListPolicy {
                kind: "prompts",
                max_items: self.limits.max_prompts,
                identity: prompt_identity,
                validate: Self::validate_prompt,
            },
            cancellation,
            observer,
        )
        .await
    }

    pub async fn read_resource(
        &self,
        uri: &str,
        input_responses: Option<BTreeMap<String, InputResponse>>,
        request_state: Option<String>,
        cancellation: &CancellationToken,
        observer: &dyn McpNotificationObserver,
    ) -> Result<McpReadResourceOutcome, ClientError> {
        validate_resource_uri(uri, self.limits.max_uri_bytes)?;
        let params = interactive_params(
            serde_json::Map::from_iter([("uri".to_owned(), Value::String(uri.to_owned()))]),
            input_responses,
            request_state,
            self.limits.max_request_state_bytes,
        )?;
        let request = self.request(CoreMethod::ResourcesRead, Value::Object(params))?;
        let result: Value = self
            .exchange(&request, &BTreeMap::new(), cancellation, observer)
            .await?;
        match result.get("resultType").and_then(Value::as_str) {
            Some("input_required") => {
                let result: InputRequiredResult =
                    serde_json::from_value(result).map_err(|_| ClientError::Response)?;
                result.validate().map_err(|_| ClientError::Response)?;
                Ok(McpReadResourceOutcome::InputRequired(result))
            }
            Some("complete") => {
                let result: ReadResourceResult =
                    serde_json::from_value(result).map_err(|_| ClientError::Response)?;
                self.validate_read_resource_result(&result)?;
                Ok(McpReadResourceOutcome::Complete(result))
            }
            _ => Err(ClientError::Response),
        }
    }

    pub async fn get_prompt(
        &self,
        prompt: &Prompt,
        arguments: BTreeMap<String, String>,
        input_responses: Option<BTreeMap<String, InputResponse>>,
        request_state: Option<String>,
        cancellation: &CancellationToken,
        observer: &dyn McpNotificationObserver,
    ) -> Result<McpGetPromptOutcome, ClientError> {
        self.validate_prompt(prompt)
            .map_err(|_| ClientError::PromptContract)?;
        validate_prompt_arguments(prompt, &arguments)?;
        let params = interactive_params(
            serde_json::Map::from_iter([
                ("name".to_owned(), Value::String(prompt.name.clone())),
                (
                    "arguments".to_owned(),
                    serde_json::to_value(arguments).map_err(|_| ClientError::Request)?,
                ),
            ]),
            input_responses,
            request_state,
            self.limits.max_request_state_bytes,
        )?;
        let request = self.request(CoreMethod::PromptsGet, Value::Object(params))?;
        let result: Value = self
            .exchange(&request, &BTreeMap::new(), cancellation, observer)
            .await?;
        match result.get("resultType").and_then(Value::as_str) {
            Some("input_required") => {
                let result: InputRequiredResult =
                    serde_json::from_value(result).map_err(|_| ClientError::Response)?;
                result.validate().map_err(|_| ClientError::Response)?;
                Ok(McpGetPromptOutcome::InputRequired(result))
            }
            Some("complete") => {
                let result: GetPromptResult =
                    serde_json::from_value(result).map_err(|_| ClientError::Response)?;
                self.validate_prompt_result(&result)?;
                Ok(McpGetPromptOutcome::Complete(result))
            }
            _ => Err(ClientError::Response),
        }
    }

    pub async fn complete(
        &self,
        reference: CompletionReference,
        argument: CompletionArgument,
        context: BTreeMap<String, String>,
        cancellation: &CancellationToken,
        observer: &dyn McpNotificationObserver,
    ) -> Result<CompleteResult, ClientError> {
        validate_completion_request(&reference, &argument, &context, self.limits.max_uri_bytes)?;
        let mut params = serde_json::Map::from_iter([
            (
                "ref".to_owned(),
                serde_json::to_value(reference).map_err(|_| ClientError::Request)?,
            ),
            (
                "argument".to_owned(),
                serde_json::to_value(argument).map_err(|_| ClientError::Request)?,
            ),
        ]);
        if !context.is_empty() {
            params.insert("context".to_owned(), json!({"arguments": context}));
        }
        let request = self.request(CoreMethod::CompletionComplete, Value::Object(params))?;
        let result: CompleteResult = self
            .exchange(&request, &BTreeMap::new(), cancellation, observer)
            .await?;
        if result.result_type != "complete"
            || result.completion.values.len() > self.limits.max_completion_values
            || result.completion.values.iter().any(|value| {
                value.len() > self.limits.max_completion_value_bytes
                    || value.chars().any(char::is_control)
            })
            || result
                .completion
                .total
                .is_some_and(|total| total < result.completion.values.len() as u64)
        {
            return Err(ClientError::Completion);
        }
        Ok(result)
    }

    pub async fn listen_subscriptions(
        &self,
        filter: SubscriptionFilter,
        cancellation: &CancellationToken,
        observer: &dyn McpNotificationObserver,
    ) -> Result<(), ClientError> {
        let mut resource_uris = BTreeSet::new();
        if (filter.tools_list_changed != Some(true)
            && filter.resources_list_changed != Some(true)
            && filter.prompts_list_changed != Some(true)
            && filter.resource_subscriptions.is_empty()
            && filter.task_ids.is_empty())
            || filter.resource_subscriptions.len() > self.limits.max_resources
            || filter.task_ids.len() > self.limits.max_tools
            || filter.resource_subscriptions.iter().any(|uri| {
                validate_resource_uri(uri, self.limits.max_uri_bytes).is_err()
                    || !resource_uris.insert(uri.clone())
            })
            || filter
                .task_ids
                .iter()
                .any(|task_id| !valid_task_id(task_id))
            || (!filter.task_ids.is_empty() && !self.supports_tasks())
        {
            return Err(ClientError::Subscription);
        }
        let request = self.request(
            CoreMethod::SubscriptionsListen,
            json!({"notifications": filter}),
        )?;
        self.transport
            .listen(&request, cancellation, observer)
            .await
            .map_err(ClientError::Transport)
    }

    pub async fn call_tool(
        &self,
        tool: &Tool,
        arguments: Value,
        input_responses: Option<BTreeMap<String, InputResponse>>,
        request_state: Option<String>,
        cancellation: &CancellationToken,
        observer: &dyn McpNotificationObserver,
    ) -> Result<McpCallOutcome, ClientError> {
        self.validate_tool(tool)
            .map_err(|_| ClientError::ToolContract)?;
        if !arguments.is_object() || !compile_schema(&tool.input_schema)?.is_valid(&arguments) {
            return Err(ClientError::ToolArguments);
        }
        if request_state.as_ref().is_some_and(|value| {
            value.is_empty() || value.len() > self.limits.max_request_state_bytes
        }) {
            return Err(ClientError::RequestState);
        }
        let parameter_headers = if self.transport.kind() == TransportKind::StreamableHttp {
            ToolHeaderPlan::from_schema(&tool.input_schema)
                .map_err(|_| ClientError::ToolContract)?
                .extract(&arguments)
                .map_err(|_| ClientError::ToolArguments)?
        } else {
            BTreeMap::new()
        };
        let mut params = serde_json::Map::from_iter([
            ("name".to_owned(), Value::String(tool.name.clone())),
            ("arguments".to_owned(), arguments),
        ]);
        if let Some(input_responses) = input_responses {
            params.insert(
                "inputResponses".to_owned(),
                serde_json::to_value(input_responses).map_err(|_| ClientError::Request)?,
            );
        }
        if let Some(request_state) = request_state {
            params.insert("requestState".to_owned(), Value::String(request_state));
        }
        let request = self.request(CoreMethod::ToolsCall, Value::Object(params))?;
        let result: Value = self
            .exchange(&request, &parameter_headers, cancellation, observer)
            .await?;
        match result.get("resultType").and_then(Value::as_str) {
            Some("task") if self.supports_tasks() => {
                let result: CreateTaskResult =
                    serde_json::from_value(result).map_err(|_| ClientError::Response)?;
                result.validate().map_err(|_| ClientError::Response)?;
                Ok(McpCallOutcome::Task(result))
            }
            Some("input_required") => {
                let result: InputRequiredResult =
                    serde_json::from_value(result).map_err(|_| ClientError::Response)?;
                result.validate().map_err(|_| ClientError::Response)?;
                Ok(McpCallOutcome::InputRequired(result))
            }
            Some("complete") => {
                let result: ToolCallResult =
                    serde_json::from_value(result).map_err(|_| ClientError::Response)?;
                self.validate_tool_result(tool, &result)?;
                Ok(McpCallOutcome::Complete(result))
            }
            _ => Err(ClientError::Response),
        }
    }

    pub async fn get_task(
        &self,
        task_id: &str,
        cancellation: &CancellationToken,
        observer: &dyn McpNotificationObserver,
    ) -> Result<GetTaskResult, ClientError> {
        if !self.supports_tasks() || !valid_task_id(task_id) {
            return Err(ClientError::Task);
        }
        let request = self.request(CoreMethod::TasksGet, json!({"taskId": task_id}))?;
        let result: GetTaskResult = self
            .exchange(&request, &BTreeMap::new(), cancellation, observer)
            .await?;
        result.validate().map_err(|_| ClientError::Task)?;
        if result.task.task_id != task_id {
            return Err(ClientError::Task);
        }
        Ok(result)
    }

    pub async fn update_task(
        &self,
        update: UpdateTaskParams,
        cancellation: &CancellationToken,
        observer: &dyn McpNotificationObserver,
    ) -> Result<(), ClientError> {
        if !self.supports_tasks() || update.validate().is_err() {
            return Err(ClientError::Task);
        }
        let request = self.request(
            CoreMethod::TasksUpdate,
            serde_json::to_value(update).map_err(|_| ClientError::Request)?,
        )?;
        let result: TaskAcknowledgement = self
            .exchange(&request, &BTreeMap::new(), cancellation, observer)
            .await?;
        result.validate().map_err(|_| ClientError::Task)
    }

    pub async fn cancel_task(
        &self,
        task_id: &str,
        cancellation: &CancellationToken,
        observer: &dyn McpNotificationObserver,
    ) -> Result<(), ClientError> {
        if !self.supports_tasks() || !valid_task_id(task_id) {
            return Err(ClientError::Task);
        }
        let request = self.request(CoreMethod::TasksCancel, json!({"taskId": task_id}))?;
        let result: TaskAcknowledgement = self
            .exchange(&request, &BTreeMap::new(), cancellation, observer)
            .await?;
        result.validate().map_err(|_| ClientError::Task)
    }

    pub fn decode_task_tool_result(
        &self,
        tool: &Tool,
        value: Value,
    ) -> Result<ToolCallResult, ClientError> {
        let result = serde_json::from_value(value).map_err(|_| ClientError::Task)?;
        self.validate_tool_result(tool, &result)?;
        Ok(result)
    }

    fn supports_tasks(&self) -> bool {
        self.capabilities
            .extensions
            .as_ref()
            .and_then(|extensions| extensions.get(MCP_TASKS_EXTENSION_ID))
            .is_some()
    }

    async fn list_cached<P, T>(
        &self,
        method: CoreMethod,
        policy: CachedListPolicy<T>,
        cancellation: &CancellationToken,
        observer: &dyn McpNotificationObserver,
    ) -> Result<McpCatalog<T>, ClientError>
    where
        P: DeserializeOwned + CachedPage<T>,
        T: Serialize,
    {
        let mut cursor: Option<String> = None;
        let mut seen_cursors = BTreeSet::new();
        let mut seen_identities = BTreeSet::new();
        let mut accepted = Vec::new();
        let mut rejected = Vec::new();
        let mut cache_scope = None;
        let mut ttl_ms = None;

        for _ in 0..self.limits.max_list_pages {
            let mut params = serde_json::Map::new();
            if let Some(value) = cursor.as_ref() {
                params.insert("cursor".to_owned(), Value::String(value.clone()));
            }
            let request = self.request(method, Value::Object(params))?;
            let page: P = self
                .exchange(&request, &BTreeMap::new(), cancellation, observer)
                .await?;
            let (result_type, items, next_cursor, page_ttl, page_scope) = page.into_parts();
            if result_type != "complete" || cache_scope.is_some_and(|scope| scope != page_scope) {
                return Err(ClientError::Catalog);
            }
            cache_scope = Some(page_scope);
            ttl_ms = Some(ttl_ms.map_or(page_ttl, |current: u64| current.min(page_ttl)));
            for item in items {
                if accepted.len().saturating_add(rejected.len()) >= policy.max_items {
                    return Err(ClientError::CatalogLimit);
                }
                let item_identity = (policy.identity)(&item).to_owned();
                if !seen_identities.insert(item_identity.clone()) {
                    return Err(ClientError::Catalog);
                }
                match (policy.validate)(self, &item) {
                    Ok(()) => accepted.push(item),
                    Err(code) => rejected.push(CatalogRejection {
                        identity: item_identity,
                        code,
                    }),
                }
            }
            let Some(next) = next_cursor else {
                return McpCatalog::seal(
                    policy.kind,
                    accepted,
                    rejected,
                    ttl_ms.unwrap_or(0),
                    cache_scope.unwrap_or(CacheScope::Private),
                    policy.identity,
                );
            };
            if next.is_empty() || next.len() > 8 * 1024 || !seen_cursors.insert(next.clone()) {
                return Err(ClientError::Pagination);
            }
            cursor = Some(next);
        }
        Err(ClientError::Pagination)
    }

    fn request(&self, method: CoreMethod, params: Value) -> Result<Value, ClientError> {
        let mut params = params.as_object().cloned().ok_or(ClientError::Request)?;
        let id = self.next_request_id.fetch_add(1, Ordering::Relaxed);
        if id <= 0 {
            return Err(ClientError::RequestIdExhausted);
        }
        let mut metadata = RequestMetadata::for_protocol(
            &self.protocol_version,
            self.client_info.clone(),
            self.capabilities.clone(),
        );
        if matches!(
            method,
            CoreMethod::ToolsCall | CoreMethod::ResourcesRead | CoreMethod::PromptsGet
        ) {
            metadata.progress_token = Some(json!(id));
        }
        params.insert(
            "_meta".to_owned(),
            serde_json::to_value(metadata).map_err(|_| ClientError::Request)?,
        );
        serde_json::to_value(JsonRpcRequest::new(
            RequestId::Integer(id),
            method,
            Value::Object(params),
        ))
        .map_err(|_| ClientError::Request)
    }

    async fn exchange<R: DeserializeOwned>(
        &self,
        request: &Value,
        headers: &BTreeMap<String, String>,
        cancellation: &CancellationToken,
        observer: &dyn McpNotificationObserver,
    ) -> Result<R, ClientError> {
        let primitive = crate::observability::primitive(
            request
                .get("method")
                .and_then(Value::as_str)
                .unwrap_or_default(),
        );
        let transport = match self.transport.kind() {
            TransportKind::StreamableHttp => "streamable_http",
            TransportKind::Stdio => "stdio",
        };
        let started = std::time::Instant::now();
        let result = async {
            let expected_id: RequestId =
                serde_json::from_value(request.get("id").cloned().ok_or(ClientError::Request)?)
                    .map_err(|_| ClientError::Request)?;
            let response = self
                .transport
                .exchange(request, headers, cancellation, observer)
                .await?;
            let bytes = self
                .codec
                .encode(&response)
                .map_err(|_| ClientError::Response)?;
            match self
                .codec
                .decode_response::<R>(&bytes, &expected_id)
                .map_err(|_| ClientError::Response)?
            {
                Ok(result) => Ok(result),
                Err(JsonRpcErrorResponse { error, .. }) => Err(ClientError::Protocol(error.code)),
            }
        }
        .await;
        let outcome = match &result {
            Ok(_) => "success",
            Err(ClientError::Transport(_)) => "transport",
            Err(ClientError::Protocol(_)) => "protocol",
            Err(_) => "validation",
        };
        crate::observability::record_operation(primitive, transport, outcome, started.elapsed());
        result
    }

    fn validate_tool(&self, tool: &Tool) -> Result<(), &'static str> {
        if !valid_tool_name(&tool.name, self.limits.max_tool_name_bytes) {
            return Err("MCP_TOOL_NAME_INVALID");
        }
        if tool
            .title
            .as_ref()
            .is_some_and(|value| value.is_empty() || value.len() > 512)
            || tool.description.as_ref().is_some_and(|value| {
                value.is_empty() || value.len() > 16 * 1024 || value.chars().any(char::is_control)
            })
            || !tool.input_schema.is_object()
            || tool.input_schema.get("type").and_then(Value::as_str) != Some("object")
            || compile_schema(&tool.input_schema).is_err()
            || tool
                .output_schema
                .as_ref()
                .is_some_and(|schema| !schema.is_object() || compile_schema(schema).is_err())
        {
            return Err("MCP_TOOL_SCHEMA_INVALID");
        }
        if self.transport.kind() == TransportKind::StreamableHttp
            && ToolHeaderPlan::from_schema(&tool.input_schema).is_err()
        {
            return Err("MCP_TOOL_HEADER_INVALID");
        }
        Ok(())
    }

    fn validate_resource(&self, resource: &Resource) -> Result<(), &'static str> {
        if validate_resource_uri(&resource.uri, self.limits.max_uri_bytes).is_err()
            || !valid_display_name(&resource.name, 512)
            || resource
                .title
                .as_ref()
                .is_some_and(|value| !valid_display_name(value, 512))
            || resource
                .description
                .as_ref()
                .is_some_and(|value| !valid_display_text(value, 16 * 1024))
            || resource
                .mime_type
                .as_ref()
                .is_some_and(|value| !valid_mime_type(value))
            || !bounded_json(&resource.annotations, 32 * 1024)
            || !bounded_json(&resource.icons, 64 * 1024)
        {
            return Err("MCP_RESOURCE_INVALID");
        }
        Ok(())
    }

    fn validate_resource_template(&self, template: &ResourceTemplate) -> Result<(), &'static str> {
        if !valid_uri_template(&template.uri_template, self.limits.max_uri_bytes)
            || !valid_display_name(&template.name, 512)
            || template
                .title
                .as_ref()
                .is_some_and(|value| !valid_display_name(value, 512))
            || template
                .description
                .as_ref()
                .is_some_and(|value| !valid_display_text(value, 16 * 1024))
            || template
                .mime_type
                .as_ref()
                .is_some_and(|value| !valid_mime_type(value))
            || !bounded_json(&template.annotations, 32 * 1024)
            || !bounded_json(&template.icons, 64 * 1024)
        {
            return Err("MCP_RESOURCE_TEMPLATE_INVALID");
        }
        Ok(())
    }

    fn validate_prompt(&self, prompt: &Prompt) -> Result<(), &'static str> {
        let mut argument_names = BTreeSet::new();
        if !valid_tool_name(&prompt.name, self.limits.max_tool_name_bytes)
            || prompt
                .title
                .as_ref()
                .is_some_and(|value| !valid_display_name(value, 512))
            || prompt
                .description
                .as_ref()
                .is_some_and(|value| !valid_display_text(value, 16 * 1024))
            || prompt.arguments.len() > 128
            || prompt.arguments.iter().any(|argument| {
                !valid_tool_name(&argument.name, self.limits.max_tool_name_bytes)
                    || !argument_names.insert(argument.name.clone())
                    || argument
                        .title
                        .as_ref()
                        .is_some_and(|value| !valid_display_name(value, 512))
                    || argument
                        .description
                        .as_ref()
                        .is_some_and(|value| !valid_display_text(value, 4 * 1024))
            })
            || !bounded_json(&prompt.icons, 64 * 1024)
        {
            return Err("MCP_PROMPT_INVALID");
        }
        Ok(())
    }

    fn validate_read_resource_result(
        &self,
        result: &ReadResourceResult,
    ) -> Result<(), ClientError> {
        if result.result_type != "complete"
            || result.contents.len() > self.limits.max_content_items
            || serde_json::to_vec(&result.contents)
                .map(|bytes| bytes.len() > self.limits.max_content_bytes)
                .unwrap_or(true)
        {
            return Err(ClientError::ContentLimit);
        }
        for content in &result.contents {
            match content {
                ResourceContents::Text {
                    uri,
                    text,
                    mime_type,
                    ..
                } => {
                    validate_resource_uri(uri, self.limits.max_uri_bytes)?;
                    if text.len() > self.limits.max_content_bytes
                        || mime_type
                            .as_ref()
                            .is_some_and(|value| !valid_mime_type(value))
                    {
                        return Err(ClientError::ResourceContent);
                    }
                }
                ResourceContents::Blob {
                    uri,
                    blob,
                    mime_type,
                    ..
                } => {
                    validate_resource_uri(uri, self.limits.max_uri_bytes)?;
                    if blob.len() > self.limits.max_content_bytes.saturating_mul(2)
                        || mime_type
                            .as_ref()
                            .is_some_and(|value| !valid_mime_type(value))
                        || BASE64_STANDARD
                            .decode(blob)
                            .map(|bytes| bytes.len() > self.limits.max_content_bytes)
                            .unwrap_or(true)
                    {
                        return Err(ClientError::ResourceContent);
                    }
                }
            }
        }
        Ok(())
    }

    fn validate_prompt_result(&self, result: &GetPromptResult) -> Result<(), ClientError> {
        if result.result_type != "complete"
            || result.messages.len() > self.limits.max_content_items
            || result
                .description
                .as_ref()
                .is_some_and(|value| !valid_display_text(value, 16 * 1024))
            || serde_json::to_vec(&result.messages)
                .map(|bytes| bytes.len() > self.limits.max_content_bytes)
                .unwrap_or(true)
        {
            return Err(ClientError::PromptResult);
        }
        Ok(())
    }

    fn validate_tool_result(
        &self,
        tool: &Tool,
        result: &ToolCallResult,
    ) -> Result<(), ClientError> {
        if result.content.len() > self.limits.max_content_items
            || serde_json::to_vec(&result.content)
                .map(|bytes| bytes.len() > self.limits.max_content_bytes)
                .unwrap_or(true)
        {
            return Err(ClientError::ContentLimit);
        }
        if result.is_error != Some(true) {
            if let Some(schema) = &tool.output_schema {
                let structured = result
                    .structured_content
                    .as_ref()
                    .ok_or(ClientError::ToolResult)?;
                if !compile_schema(schema)?.is_valid(structured) {
                    return Err(ClientError::ToolResult);
                }
            }
        }
        Ok(())
    }
}

fn interactive_params(
    mut params: serde_json::Map<String, Value>,
    input_responses: Option<BTreeMap<String, InputResponse>>,
    request_state: Option<String>,
    max_request_state_bytes: usize,
) -> Result<serde_json::Map<String, Value>, ClientError> {
    if request_state.as_ref().is_some_and(|value| {
        value.is_empty()
            || value.len() > max_request_state_bytes
            || value.chars().any(char::is_control)
    }) {
        return Err(ClientError::RequestState);
    }
    if let Some(input_responses) = input_responses {
        if input_responses.is_empty()
            || input_responses.len() > 32
            || input_responses
                .keys()
                .any(|key| key.is_empty() || key.len() > 128 || key.chars().any(char::is_control))
        {
            return Err(ClientError::Request);
        }
        params.insert(
            "inputResponses".to_owned(),
            serde_json::to_value(input_responses).map_err(|_| ClientError::Request)?,
        );
    }
    if let Some(request_state) = request_state {
        params.insert("requestState".to_owned(), Value::String(request_state));
    }
    Ok(params)
}

fn resource_identity(resource: &Resource) -> &str {
    &resource.uri
}

fn resource_template_identity(template: &ResourceTemplate) -> &str {
    &template.uri_template
}

fn prompt_identity(prompt: &Prompt) -> &str {
    &prompt.name
}

fn validate_resource_uri(uri: &str, max_bytes: usize) -> Result<(), ClientError> {
    if uri.is_empty()
        || uri.len() > max_bytes
        || uri.chars().any(char::is_control)
        || Url::parse(uri).is_err()
    {
        return Err(ClientError::ResourceUri);
    }
    Ok(())
}

fn valid_uri_template(value: &str, max_bytes: usize) -> bool {
    if value.is_empty()
        || value.len() > max_bytes
        || value.chars().any(char::is_control)
        || value.contains(char::is_whitespace)
    {
        return false;
    }
    let Some(colon) = value.find(':') else {
        return false;
    };
    colon > 0
        && value[..colon].bytes().enumerate().all(|(index, byte)| {
            byte.is_ascii_alphabetic()
                || (index > 0 && (byte.is_ascii_digit() || matches!(byte, b'+' | b'-' | b'.')))
        })
}

fn valid_display_name(value: &str, max_bytes: usize) -> bool {
    !value.is_empty()
        && value.len() <= max_bytes
        && !value
            .chars()
            .any(|character| character.is_control() && !matches!(character, '\n' | '\r' | '\t'))
}

fn valid_display_text(value: &str, max_bytes: usize) -> bool {
    valid_display_name(value, max_bytes)
}

fn valid_mime_type(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 256
        && value.is_ascii()
        && !value.chars().any(char::is_whitespace)
        && value.split_once('/').is_some_and(|(kind, subtype)| {
            !kind.is_empty()
                && !subtype.is_empty()
                && kind
                    .bytes()
                    .chain(subtype.bytes())
                    .all(|byte| byte.is_ascii_alphanumeric() || b"!#$&^_.+-".contains(&byte))
        })
}

fn bounded_json(value: &impl Serialize, max_bytes: usize) -> bool {
    serde_json::to_vec(value)
        .map(|bytes| bytes.len() <= max_bytes)
        .unwrap_or(false)
}

fn validate_prompt_arguments(
    prompt: &Prompt,
    arguments: &BTreeMap<String, String>,
) -> Result<(), ClientError> {
    let declared = prompt
        .arguments
        .iter()
        .map(|argument| (argument.name.as_str(), argument.required == Some(true)))
        .collect::<BTreeMap<_, _>>();
    if arguments.len() > declared.len()
        || arguments.iter().any(|(name, value)| {
            !declared.contains_key(name.as_str())
                || value.len() > 16 * 1024
                || value.chars().any(char::is_control)
        })
        || declared
            .iter()
            .any(|(name, required)| *required && !arguments.contains_key(*name))
    {
        return Err(ClientError::PromptArguments);
    }
    Ok(())
}

fn validate_completion_request(
    reference: &CompletionReference,
    argument: &CompletionArgument,
    context: &BTreeMap<String, String>,
    max_uri_bytes: usize,
) -> Result<(), ClientError> {
    let reference_valid = match reference {
        CompletionReference::Prompt { name, title } => {
            valid_tool_name(name, 128)
                && title
                    .as_ref()
                    .is_none_or(|value| valid_display_name(value, 512))
        }
        CompletionReference::Resource { uri } => valid_uri_template(uri, max_uri_bytes),
    };
    if !reference_valid
        || !valid_tool_name(&argument.name, 128)
        || argument.value.len() > 16 * 1024
        || argument.value.chars().any(char::is_control)
        || context.len() > 128
        || context.iter().any(|(name, value)| {
            !valid_tool_name(name, 128)
                || value.len() > 16 * 1024
                || value.chars().any(char::is_control)
        })
    {
        return Err(ClientError::Completion);
    }
    Ok(())
}

fn compile_schema(schema: &Value) -> Result<Validator, ClientError> {
    validate_schema_references(schema)?;
    let draft = match schema.get("$schema").and_then(Value::as_str) {
        None
        | Some("https://json-schema.org/draft/2020-12/schema")
        | Some("https://json-schema.org/draft/2020-12/schema#") => Draft::Draft202012,
        Some("http://json-schema.org/draft-07/schema#")
        | Some("https://json-schema.org/draft-07/schema#") => Draft::Draft7,
        Some(_) => return Err(ClientError::SchemaDraft),
    };
    jsonschema::options()
        .with_draft(draft)
        .build(schema)
        .map_err(|_| ClientError::ToolContract)
}

fn validate_schema_references(schema: &Value) -> Result<(), ClientError> {
    match schema {
        Value::Array(values) => {
            for value in values {
                validate_schema_references(value)?;
            }
        }
        Value::Object(values) => {
            for (key, value) in values {
                if matches!(key.as_str(), "$dynamicRef" | "$dynamicAnchor") {
                    return Err(ClientError::ToolContract);
                }
                if key == "$ref" {
                    let reference = value.as_str().ok_or(ClientError::ToolContract)?;
                    if !reference.starts_with('#') {
                        return Err(ClientError::ToolContract);
                    }
                }
                validate_schema_references(value)?;
            }
        }
        _ => {}
    }
    Ok(())
}

fn valid_tool_name(value: &str, max_bytes: usize) -> bool {
    !value.is_empty()
        && value.len() <= max_bytes
        && value.is_ascii()
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'-' | b'.'))
}

fn hex_sha256(bytes: &[u8]) -> String {
    let digest = Sha256::digest(bytes);
    let mut encoded = String::with_capacity(digest.len() * 2);
    for byte in digest {
        write!(&mut encoded, "{byte:02x}").expect("writing to String cannot fail");
    }
    encoded
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ClientError {
    Configuration,
    Transport(TransportError),
    Request,
    RequestIdExhausted,
    Response,
    Protocol(i64),
    Discovery,
    Catalog,
    CatalogLimit,
    Pagination,
    SchemaDraft,
    ToolContract,
    ToolArguments,
    RequestState,
    ToolResult,
    ContentLimit,
    ResourceUri,
    ResourceContent,
    PromptContract,
    PromptArguments,
    PromptResult,
    Completion,
    Subscription,
    Task,
    TaskExpired,
    TaskFailed,
    TaskCancelled,
}

impl From<TransportError> for ClientError {
    fn from(error: TransportError) -> Self {
        Self::Transport(error)
    }
}

impl std::fmt::Display for ClientError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(match self {
            Self::Configuration => "invalid MCP client configuration",
            Self::Transport(_) => "MCP transport failed",
            Self::Request => "MCP request construction failed",
            Self::RequestIdExhausted => "MCP request ID space is exhausted",
            Self::Response => "invalid MCP response",
            Self::Protocol(_) => "MCP server returned a protocol error",
            Self::Discovery => "invalid MCP discovery result",
            Self::Catalog => "invalid MCP tool catalog",
            Self::CatalogLimit => "MCP tool catalog exceeds configured limits",
            Self::Pagination => "invalid MCP pagination sequence",
            Self::SchemaDraft => "unsupported MCP tool schema draft",
            Self::ToolContract => "invalid MCP tool contract",
            Self::ToolArguments => "MCP tool arguments violate the frozen contract",
            Self::RequestState => "invalid MCP request state",
            Self::ToolResult => "MCP tool result violates the frozen contract",
            Self::ContentLimit => "MCP tool content exceeds configured limits",
            Self::ResourceUri => "invalid MCP resource URI",
            Self::ResourceContent => "invalid MCP resource content",
            Self::PromptContract => "invalid MCP prompt contract",
            Self::PromptArguments => "invalid MCP prompt arguments",
            Self::PromptResult => "invalid MCP prompt result",
            Self::Completion => "invalid MCP completion request or result",
            Self::Subscription => "invalid MCP subscription request or stream",
            Self::Task => "invalid MCP task request or result",
            Self::TaskExpired => "MCP task expired",
            Self::TaskFailed => "MCP task failed",
            Self::TaskCancelled => "MCP task was cancelled",
        })
    }
}

impl std::error::Error for ClientError {}

#[cfg(test)]
mod tests {
    use async_trait::async_trait;
    use std::sync::Mutex;

    use super::*;
    use crate::{
        ElicitAction, ElicitResult, JsonRpcNotification, MetaMap, NoopNotificationObserver,
    };

    #[derive(Default)]
    struct FixtureTransport {
        requests: Mutex<Vec<Value>>,
        responses: Mutex<Vec<Value>>,
        kind: Option<TransportKind>,
    }

    #[async_trait]
    impl McpTransport for FixtureTransport {
        fn kind(&self) -> TransportKind {
            self.kind.unwrap_or(TransportKind::StreamableHttp)
        }

        async fn exchange(
            &self,
            request: &Value,
            _parameter_headers: &BTreeMap<String, String>,
            _cancellation: &CancellationToken,
            _observer: &dyn McpNotificationObserver,
        ) -> Result<Value, TransportError> {
            self.requests.lock().unwrap().push(request.clone());
            let mut response = self.responses.lock().unwrap().remove(0);
            response["id"] = request["id"].clone();
            Ok(response)
        }
    }

    fn client(transport: Arc<FixtureTransport>) -> McpClient {
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
        .unwrap()
    }

    fn task_client(transport: Arc<FixtureTransport>) -> McpClient {
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
            ClientCapabilities {
                extensions: Some(
                    MetaMap::new(BTreeMap::from([(
                        MCP_TASKS_EXTENSION_ID.to_owned(),
                        json!({}),
                    )]))
                    .unwrap(),
                ),
                ..ClientCapabilities::default()
            },
        )
        .unwrap()
    }

    fn task_tool() -> Tool {
        Tool {
            name: "slow.tool".to_owned(),
            title: None,
            description: None,
            input_schema: json!({
                "$schema": "https://json-schema.org/draft/2020-12/schema",
                "type": "object",
                "additionalProperties": false
            }),
            output_schema: Some(json!({
                "$schema": "https://json-schema.org/draft/2020-12/schema",
                "type": "object",
                "properties": {"answer": {"type": "string"}},
                "required": ["answer"],
                "additionalProperties": false
            })),
            annotations: None,
            metadata: None,
            icons: Vec::new(),
        }
    }

    #[tokio::test]
    async fn tasks_are_capability_gated_correlated_and_use_current_methods() {
        let task_result = json!({"jsonrpc":"2.0","id":0,"result":{
            "resultType":"task",
            "taskId":"opaque-task",
            "status":"working",
            "createdAt":"2026-07-30T10:00:00Z",
            "lastUpdatedAt":"2026-07-30T10:00:00Z",
            "ttlMs":60000,
            "pollIntervalMs":250
        }});
        let unsupported_transport = Arc::new(FixtureTransport::default());
        unsupported_transport
            .responses
            .lock()
            .unwrap()
            .push(task_result.clone());
        assert!(matches!(
            client(unsupported_transport)
                .call_tool(
                    &task_tool(),
                    json!({}),
                    None,
                    None,
                    &CancellationToken::new(),
                    &NoopNotificationObserver
                )
                .await,
            Err(ClientError::Response)
        ));

        let transport = Arc::new(FixtureTransport::default());
        transport.responses.lock().unwrap().extend([
            task_result,
            json!({"jsonrpc":"2.0","id":0,"result":{
                "resultType":"complete",
                "taskId":"wrong-task",
                "status":"working",
                "createdAt":"2026-07-30T10:00:00Z",
                "lastUpdatedAt":"2026-07-30T10:00:01Z",
                "ttlMs":60000
            }}),
            json!({"jsonrpc":"2.0","id":0,"result":{
                "resultType":"complete",
                "taskId":"opaque-task",
                "status":"completed",
                "createdAt":"2026-07-30T10:00:00Z",
                "lastUpdatedAt":"2026-07-30T10:00:02Z",
                "ttlMs":60000,
                "result":{
                    "resultType":"complete",
                    "content":[{"type":"text","text":"done"}],
                    "structuredContent":{"answer":"done"},
                    "isError":false
                }
            }}),
            json!({"jsonrpc":"2.0","id":0,"result":{"resultType":"complete"}}),
            json!({"jsonrpc":"2.0","id":0,"result":{"resultType":"complete"}}),
        ]);
        let client = task_client(transport.clone());
        let cancellation = CancellationToken::new();
        assert!(matches!(
            client
                .call_tool(
                    &task_tool(),
                    json!({}),
                    None,
                    None,
                    &cancellation,
                    &NoopNotificationObserver
                )
                .await
                .unwrap(),
            McpCallOutcome::Task(_)
        ));
        assert!(matches!(
            client
                .get_task("opaque-task", &cancellation, &NoopNotificationObserver)
                .await,
            Err(ClientError::Task)
        ));
        let completed = client
            .get_task("opaque-task", &cancellation, &NoopNotificationObserver)
            .await
            .unwrap();
        client
            .decode_task_tool_result(&task_tool(), completed.result.unwrap())
            .unwrap();
        client
            .update_task(
                UpdateTaskParams {
                    task_id: "opaque-task".to_owned(),
                    input_responses: BTreeMap::from([(
                        "choice".to_owned(),
                        InputResponse::Elicitation(ElicitResult {
                            action: ElicitAction::Accept,
                            content: Some(BTreeMap::from([("choice".to_owned(), json!("yes"))])),
                        }),
                    )]),
                },
                &cancellation,
                &NoopNotificationObserver,
            )
            .await
            .unwrap();
        client
            .cancel_task("opaque-task", &cancellation, &NoopNotificationObserver)
            .await
            .unwrap();
        let requests = transport.requests.lock().unwrap();
        assert_eq!(requests[0]["method"], "tools/call");
        assert_eq!(requests[1]["method"], "tasks/get");
        assert_eq!(requests[2]["method"], "tasks/get");
        assert_eq!(requests[3]["method"], "tasks/update");
        assert_eq!(requests[4]["method"], "tasks/cancel");
        assert!(requests.iter().all(|request| {
            request["params"]["_meta"]["io.modelcontextprotocol/clientCapabilities"]["extensions"]
                [MCP_TASKS_EXTENSION_ID]
                .is_object()
        }));
    }

    #[tokio::test]
    async fn discovery_and_paginated_tool_catalog_are_modern_and_frozen() {
        let transport = Arc::new(FixtureTransport::default());
        transport.responses.lock().unwrap().extend([
            json!({"jsonrpc":"2.0","id":0,"result":{
                "resultType":"complete",
                "supportedVersions":["2026-07-28"],
                "capabilities":{"tools":{}},
                "serverInfo":{"name":"fixture","version":"1.0.0"},
                "ttlMs":1000,"cacheScope":"private"
            }}),
            json!({"jsonrpc":"2.0","id":0,"result":{
                "resultType":"complete",
                "tools":[{"name":"search.one","inputSchema":{"type":"object"}}],
                "nextCursor":"next","ttlMs":1000,"cacheScope":"private"
            }}),
            json!({"jsonrpc":"2.0","id":0,"result":{
                "resultType":"complete",
                "tools":[
                    {"name":"search.two","inputSchema":{"type":"object"}},
                    {"name":"bad","inputSchema":{"type":"object","properties":{
                        "n":{"type":"number","x-mcp-header":"Route"}
                    }}}
                ],
                "ttlMs":500,"cacheScope":"private"
            }}),
        ]);
        let client = client(transport.clone());
        let cancellation = CancellationToken::new();
        client
            .discover(&cancellation, &NoopNotificationObserver)
            .await
            .unwrap();
        let catalog = client
            .list_tools(&cancellation, &NoopNotificationObserver)
            .await
            .unwrap();
        assert_eq!(catalog.tools.len(), 2);
        assert_eq!(catalog.tools[0].name, "search.one");
        assert_eq!(catalog.tools[1].name, "search.two");
        assert_eq!(catalog.rejected[0].code, "MCP_TOOL_HEADER_INVALID");
        assert_eq!(catalog.ttl_ms, 500);
        assert_eq!(catalog.descriptor_hash.len(), 64);
        let requests = transport.requests.lock().unwrap();
        assert_eq!(requests[0]["method"], "server/discover");
        assert_eq!(
            requests[0]["params"]["_meta"]["io.modelcontextprotocol/protocolVersion"],
            MCP_PROTOCOL_VERSION
        );
        assert_ne!(requests[0]["id"], requests[1]["id"]);
        assert_ne!(requests[1]["id"], requests[2]["id"]);
    }

    #[test]
    fn catalog_hash_is_order_independent_and_remote_refs_are_rejected() {
        let first = Tool {
            name: "zeta".to_owned(),
            title: None,
            description: None,
            input_schema: json!({"type":"object"}),
            output_schema: None,
            annotations: None,
            metadata: None,
            icons: Vec::new(),
        };
        let second = Tool {
            name: "alpha".to_owned(),
            title: None,
            description: None,
            input_schema: json!({"type":"object"}),
            output_schema: None,
            annotations: None,
            metadata: None,
            icons: Vec::new(),
        };
        let forward = ToolCatalog::seal(
            vec![first.clone(), second.clone()],
            Vec::new(),
            100,
            CacheScope::Private,
        )
        .unwrap();
        let reverse =
            ToolCatalog::seal(vec![second, first], Vec::new(), 100, CacheScope::Private).unwrap();
        assert_eq!(forward.descriptor_hash, reverse.descriptor_hash);
        assert_eq!(forward.tools[0].name, "alpha");

        let client = client(Arc::new(FixtureTransport::default()));
        let remote_ref = Tool {
            name: "remote_ref".to_owned(),
            title: None,
            description: None,
            input_schema: json!({
                "type":"object",
                "properties":{"value":{"$ref":"https://attacker.invalid/schema.json"}}
            }),
            output_schema: None,
            annotations: None,
            metadata: None,
            icons: Vec::new(),
        };
        assert_eq!(
            client.validate_tool(&remote_ref),
            Err("MCP_TOOL_SCHEMA_INVALID")
        );
    }

    #[tokio::test]
    async fn tool_call_distinguishes_complete_error_and_input_required() {
        let transport = Arc::new(FixtureTransport::default());
        transport.responses.lock().unwrap().extend([
            json!({"jsonrpc":"2.0","id":0,"result":{
                "resultType":"complete",
                "content":[{"type":"text","text":"try another value"}],
                "isError":true
            }}),
            json!({"jsonrpc":"2.0","id":0,"result":{
                "resultType":"input_required",
                "requestState":"opaque",
                "inputRequests":{"approval":{
                    "method":"elicitation/create",
                    "params":{"mode":"url","message":"Authorize","url":"https://example.test"}
                }}
            }}),
        ]);
        let client = client(transport);
        let tool = Tool {
            name: "search".to_owned(),
            title: None,
            description: None,
            input_schema: json!({
                "type":"object",
                "properties":{"query":{"type":"string"}},
                "required":["query"]
            }),
            output_schema: Some(json!({
                "type":"object",
                "properties":{"answer":{"type":"string"}},
                "required":["answer"]
            })),
            annotations: None,
            metadata: None,
            icons: Vec::new(),
        };
        let cancellation = CancellationToken::new();
        let first = client
            .call_tool(
                &tool,
                json!({"query":"x"}),
                None,
                None,
                &cancellation,
                &NoopNotificationObserver,
            )
            .await
            .unwrap();
        assert!(matches!(
            first,
            McpCallOutcome::Complete(ToolCallResult {
                is_error: Some(true),
                ..
            })
        ));
        let second = client
            .call_tool(
                &tool,
                json!({"query":"x"}),
                None,
                None,
                &cancellation,
                &NoopNotificationObserver,
            )
            .await
            .unwrap();
        assert!(matches!(second, McpCallOutcome::InputRequired(_)));
    }

    #[tokio::test]
    async fn resources_prompts_and_completion_are_bounded_and_typed() {
        let transport = Arc::new(FixtureTransport::default());
        transport.responses.lock().unwrap().extend([
            json!({"jsonrpc":"2.0","id":0,"result":{
                "resultType":"complete",
                "resources":[{"uri":"repo://project/readme","name":"README","mimeType":"text/plain"}],
                "ttlMs":500,"cacheScope":"private"
            }}),
            json!({"jsonrpc":"2.0","id":0,"result":{
                "resultType":"complete",
                "resourceTemplates":[{"uriTemplate":"repo://project/{path}","name":"Project file"}],
                "ttlMs":500,"cacheScope":"private"
            }}),
            json!({"jsonrpc":"2.0","id":0,"result":{
                "resultType":"complete",
                "prompts":[{
                    "name":"review_change",
                    "arguments":[{"name":"change","required":true}]
                }],
                "ttlMs":500,"cacheScope":"private"
            }}),
            json!({"jsonrpc":"2.0","id":0,"result":{
                "resultType":"complete",
                "contents":[{"uri":"repo://project/readme","text":"untrusted body","mimeType":"text/plain"}],
                "ttlMs":100,"cacheScope":"private"
            }}),
            json!({"jsonrpc":"2.0","id":0,"result":{
                "resultType":"complete",
                "description":"Untrusted prompt",
                "messages":[{"role":"user","content":{"type":"text","text":"Review change 42"}}]
            }}),
            json!({"jsonrpc":"2.0","id":0,"result":{
                "resultType":"complete",
                "completion":{"values":["42","420"],"total":2,"hasMore":false}
            }}),
        ]);
        let client = client(transport.clone());
        let cancellation = CancellationToken::new();
        let resources = client
            .list_resources(&cancellation, &NoopNotificationObserver)
            .await
            .unwrap();
        assert_eq!(resources.items[0].uri, "repo://project/readme");
        let templates = client
            .list_resource_templates(&cancellation, &NoopNotificationObserver)
            .await
            .unwrap();
        assert_eq!(templates.items[0].uri_template, "repo://project/{path}");
        let prompts = client
            .list_prompts(&cancellation, &NoopNotificationObserver)
            .await
            .unwrap();
        let resource = client
            .read_resource(
                "repo://project/readme",
                None,
                None,
                &cancellation,
                &NoopNotificationObserver,
            )
            .await
            .unwrap();
        assert!(matches!(resource, McpReadResourceOutcome::Complete(_)));
        let prompt = client
            .get_prompt(
                &prompts.items[0],
                BTreeMap::from([("change".to_owned(), "42".to_owned())]),
                None,
                None,
                &cancellation,
                &NoopNotificationObserver,
            )
            .await
            .unwrap();
        assert!(matches!(prompt, McpGetPromptOutcome::Complete(_)));
        let completion = client
            .complete(
                CompletionReference::Prompt {
                    name: "review_change".to_owned(),
                    title: None,
                },
                CompletionArgument {
                    name: "change".to_owned(),
                    value: "4".to_owned(),
                },
                BTreeMap::new(),
                &cancellation,
                &NoopNotificationObserver,
            )
            .await
            .unwrap();
        assert_eq!(completion.completion.values, ["42", "420"]);
        let methods = transport
            .requests
            .lock()
            .unwrap()
            .iter()
            .map(|request| request["method"].as_str().unwrap().to_owned())
            .collect::<Vec<_>>();
        assert_eq!(
            methods,
            [
                "resources/list",
                "resources/templates/list",
                "prompts/list",
                "resources/read",
                "prompts/get",
                "completion/complete"
            ]
        );
    }

    #[test]
    fn observer_contract_is_object_safe() {
        fn accepts(_observer: &dyn McpNotificationObserver) {}
        accepts(&NoopNotificationObserver);
        let _: Option<JsonRpcNotification<Value>> = None;
    }
}
