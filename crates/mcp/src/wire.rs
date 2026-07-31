use std::collections::BTreeMap;

use serde::{de::Error as _, Deserialize, Deserializer, Serialize};
use serde_json::Value;

pub const JSON_RPC_VERSION: &str = "2.0";
pub const MCP_PROTOCOL_VERSION: &str = "2026-07-28";
pub const MCP_LEGACY_PROTOCOL_VERSION: &str = "2025-11-25";
const MAX_META_ENTRIES: usize = 64;
const MAX_META_BYTES: usize = 32 * 1024;
const MAX_META_DEPTH: usize = 16;

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(untagged)]
pub enum RequestId {
    String(String),
    Integer(i64),
}

#[derive(Debug, Clone, PartialEq, Serialize)]
#[serde(transparent)]
pub struct MetaMap(BTreeMap<String, Value>);

impl MetaMap {
    pub fn new(values: BTreeMap<String, Value>) -> Result<Self, &'static str> {
        validate_meta(&values)?;
        Ok(Self(values))
    }

    pub fn empty() -> Self {
        Self(BTreeMap::new())
    }

    pub fn get(&self, key: &str) -> Option<&Value> {
        self.0.get(key)
    }

    pub fn iter(&self) -> impl Iterator<Item = (&str, &Value)> {
        self.0.iter().map(|(key, value)| (key.as_str(), value))
    }

    pub fn into_inner(self) -> BTreeMap<String, Value> {
        self.0
    }
}

impl<'de> Deserialize<'de> for MetaMap {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let values = BTreeMap::<String, Value>::deserialize(deserializer)?;
        Self::new(values).map_err(D::Error::custom)
    }
}

impl Default for MetaMap {
    fn default() -> Self {
        Self::empty()
    }
}

fn validate_meta(values: &BTreeMap<String, Value>) -> Result<(), &'static str> {
    if values.len() > MAX_META_ENTRIES
        || values
            .keys()
            .any(|key| key.is_empty() || key.len() > 256 || key.chars().any(char::is_control))
    {
        return Err("MCP metadata entry limit exceeded");
    }
    let value = serde_json::to_value(values).map_err(|_| "MCP metadata is not serializable")?;
    if serde_json::to_vec(&value)
        .map_err(|_| "MCP metadata is not serializable")?
        .len()
        > MAX_META_BYTES
        || json_depth(&value) > MAX_META_DEPTH
    {
        return Err("MCP metadata size or depth limit exceeded");
    }
    Ok(())
}

fn json_depth(value: &Value) -> usize {
    match value {
        Value::Array(values) => 1 + values.iter().map(json_depth).max().unwrap_or(0),
        Value::Object(values) => 1 + values.values().map(json_depth).max().unwrap_or(0),
        _ => 1,
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ClientInfo {
    pub name: String,
    pub version: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub title: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    #[serde(
        rename = "websiteUrl",
        default,
        skip_serializing_if = "Option::is_none"
    )]
    pub website_url: Option<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub icons: Vec<Value>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Default)]
pub struct ClientCapabilities {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub elicitation: Option<Value>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub experimental: Option<MetaMap>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub extensions: Option<MetaMap>,
    #[serde(flatten)]
    pub additional: BTreeMap<String, Value>,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct RequestMetadata {
    #[serde(rename = "io.modelcontextprotocol/protocolVersion")]
    pub protocol_version: String,
    #[serde(rename = "io.modelcontextprotocol/clientCapabilities")]
    pub client_capabilities: ClientCapabilities,
    #[serde(rename = "io.modelcontextprotocol/clientInfo")]
    pub client_info: ClientInfo,
    #[serde(
        rename = "io.modelcontextprotocol/logLevel",
        default,
        skip_serializing_if = "Option::is_none"
    )]
    pub log_level: Option<String>,
    #[serde(
        rename = "progressToken",
        default,
        skip_serializing_if = "Option::is_none"
    )]
    pub progress_token: Option<Value>,
    #[serde(flatten)]
    pub extensions: BTreeMap<String, Value>,
}

#[derive(Deserialize)]
struct RequestMetadataWire {
    #[serde(rename = "io.modelcontextprotocol/protocolVersion")]
    protocol_version: String,
    #[serde(rename = "io.modelcontextprotocol/clientCapabilities")]
    client_capabilities: ClientCapabilities,
    #[serde(rename = "io.modelcontextprotocol/clientInfo")]
    client_info: ClientInfo,
    #[serde(rename = "io.modelcontextprotocol/logLevel", default)]
    log_level: Option<String>,
    #[serde(rename = "progressToken", default)]
    progress_token: Option<Value>,
    #[serde(flatten)]
    extensions: BTreeMap<String, Value>,
}

impl<'de> Deserialize<'de> for RequestMetadata {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let wire = RequestMetadataWire::deserialize(deserializer)?;
        validate_meta(&wire.extensions).map_err(D::Error::custom)?;
        if wire.protocol_version.is_empty()
            || wire.protocol_version.len() > 64
            || wire.client_info.name.is_empty()
            || wire.client_info.name.len() > 256
            || wire.client_info.version.is_empty()
            || wire.client_info.version.len() > 128
        {
            return Err(D::Error::custom("invalid MCP request metadata"));
        }
        Ok(Self {
            protocol_version: wire.protocol_version,
            client_capabilities: wire.client_capabilities,
            client_info: wire.client_info,
            log_level: wire.log_level,
            progress_token: wire.progress_token,
            extensions: wire.extensions,
        })
    }
}

impl RequestMetadata {
    pub fn modern(client_info: ClientInfo, client_capabilities: ClientCapabilities) -> Self {
        Self::for_protocol(MCP_PROTOCOL_VERSION, client_info, client_capabilities)
    }

    pub fn for_protocol(
        protocol_version: &str,
        client_info: ClientInfo,
        client_capabilities: ClientCapabilities,
    ) -> Self {
        Self {
            protocol_version: protocol_version.to_owned(),
            client_capabilities,
            client_info,
            log_level: None,
            progress_token: None,
            extensions: BTreeMap::new(),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CoreMethod {
    ServerDiscover,
    ToolsList,
    ToolsCall,
    ResourcesList,
    ResourceTemplatesList,
    ResourcesRead,
    PromptsList,
    PromptsGet,
    CompletionComplete,
    SubscriptionsListen,
    TasksGet,
    TasksUpdate,
    TasksCancel,
}

impl CoreMethod {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::ServerDiscover => "server/discover",
            Self::ToolsList => "tools/list",
            Self::ToolsCall => "tools/call",
            Self::ResourcesList => "resources/list",
            Self::ResourceTemplatesList => "resources/templates/list",
            Self::ResourcesRead => "resources/read",
            Self::PromptsList => "prompts/list",
            Self::PromptsGet => "prompts/get",
            Self::CompletionComplete => "completion/complete",
            Self::SubscriptionsListen => "subscriptions/listen",
            Self::TasksGet => "tasks/get",
            Self::TasksUpdate => "tasks/update",
            Self::TasksCancel => "tasks/cancel",
        }
    }

    pub fn parse(value: &str) -> Option<Self> {
        Some(match value {
            "server/discover" => Self::ServerDiscover,
            "tools/list" => Self::ToolsList,
            "tools/call" => Self::ToolsCall,
            "resources/list" => Self::ResourcesList,
            "resources/templates/list" => Self::ResourceTemplatesList,
            "resources/read" => Self::ResourcesRead,
            "prompts/list" => Self::PromptsList,
            "prompts/get" => Self::PromptsGet,
            "completion/complete" => Self::CompletionComplete,
            "subscriptions/listen" => Self::SubscriptionsListen,
            "tasks/get" => Self::TasksGet,
            "tasks/update" => Self::TasksUpdate,
            "tasks/cancel" => Self::TasksCancel,
            _ => return None,
        })
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct JsonRpcRequest<P> {
    pub jsonrpc: String,
    pub id: RequestId,
    pub method: String,
    pub params: P,
}

impl<P> JsonRpcRequest<P> {
    pub fn new(id: RequestId, method: CoreMethod, params: P) -> Self {
        Self {
            jsonrpc: JSON_RPC_VERSION.to_owned(),
            id,
            method: method.as_str().to_owned(),
            params,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct JsonRpcNotification<P> {
    pub jsonrpc: String,
    pub method: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub params: Option<P>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct JsonRpcResponse<R> {
    pub jsonrpc: String,
    pub id: RequestId,
    pub result: R,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct JsonRpcErrorResponse {
    pub jsonrpc: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub id: Option<RequestId>,
    pub error: JsonRpcError,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct JsonRpcError {
    pub code: i64,
    pub message: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub data: Option<Value>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct ModernRequest {
    pub id: RequestId,
    pub method: CoreMethod,
    pub metadata: RequestMetadata,
    pub params: Value,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct EmptyParams {
    #[serde(rename = "_meta")]
    pub metadata: RequestMetadata,
}

pub type DiscoverParams = EmptyParams;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ServerInfo {
    pub name: String,
    pub version: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub title: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    #[serde(
        rename = "websiteUrl",
        default,
        skip_serializing_if = "Option::is_none"
    )]
    pub website_url: Option<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub icons: Vec<Value>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Default)]
pub struct ServerCapabilities {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tools: Option<Value>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub resources: Option<Value>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub prompts: Option<Value>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub completions: Option<Value>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub experimental: Option<MetaMap>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub extensions: Option<MetaMap>,
    #[serde(flatten)]
    pub additional: BTreeMap<String, Value>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum CacheScope {
    Private,
    Public,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DiscoverResult {
    #[serde(rename = "resultType")]
    pub result_type: String,
    #[serde(rename = "supportedVersions")]
    pub supported_versions: Vec<String>,
    pub capabilities: ServerCapabilities,
    #[serde(rename = "serverInfo")]
    pub server_info: ServerInfo,
    #[serde(rename = "ttlMs")]
    pub ttl_ms: u64,
    #[serde(rename = "cacheScope")]
    pub cache_scope: CacheScope,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub instructions: Option<String>,
    #[serde(rename = "_meta", default, skip_serializing_if = "Option::is_none")]
    pub metadata: Option<MetaMap>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Tool {
    pub name: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub title: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    #[serde(rename = "inputSchema")]
    pub input_schema: Value,
    #[serde(
        rename = "outputSchema",
        default,
        skip_serializing_if = "Option::is_none"
    )]
    pub output_schema: Option<Value>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub annotations: Option<ToolAnnotations>,
    #[serde(rename = "_meta", default, skip_serializing_if = "Option::is_none")]
    pub metadata: Option<MetaMap>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub icons: Vec<Value>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Default)]
#[serde(deny_unknown_fields)]
pub struct ToolAnnotations {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub title: Option<String>,
    #[serde(
        rename = "readOnlyHint",
        default,
        skip_serializing_if = "Option::is_none"
    )]
    pub read_only_hint: Option<bool>,
    #[serde(
        rename = "destructiveHint",
        default,
        skip_serializing_if = "Option::is_none"
    )]
    pub destructive_hint: Option<bool>,
    #[serde(
        rename = "idempotentHint",
        default,
        skip_serializing_if = "Option::is_none"
    )]
    pub idempotent_hint: Option<bool>,
    #[serde(
        rename = "openWorldHint",
        default,
        skip_serializing_if = "Option::is_none"
    )]
    pub open_world_hint: Option<bool>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ListToolsResult {
    #[serde(rename = "resultType")]
    pub result_type: String,
    pub tools: Vec<Tool>,
    #[serde(
        rename = "nextCursor",
        default,
        skip_serializing_if = "Option::is_none"
    )]
    pub next_cursor: Option<String>,
    #[serde(rename = "ttlMs")]
    pub ttl_ms: u64,
    #[serde(rename = "cacheScope")]
    pub cache_scope: CacheScope,
    #[serde(rename = "_meta", default, skip_serializing_if = "Option::is_none")]
    pub metadata: Option<MetaMap>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Resource {
    pub uri: String,
    pub name: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub title: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    #[serde(rename = "mimeType", default, skip_serializing_if = "Option::is_none")]
    pub mime_type: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub size: Option<u64>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub icons: Vec<Value>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub annotations: Option<Value>,
    #[serde(rename = "_meta", default, skip_serializing_if = "Option::is_none")]
    pub metadata: Option<MetaMap>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ResourceTemplate {
    #[serde(rename = "uriTemplate")]
    pub uri_template: String,
    pub name: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub title: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    #[serde(rename = "mimeType", default, skip_serializing_if = "Option::is_none")]
    pub mime_type: Option<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub icons: Vec<Value>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub annotations: Option<Value>,
    #[serde(rename = "_meta", default, skip_serializing_if = "Option::is_none")]
    pub metadata: Option<MetaMap>,
}

macro_rules! cached_list_result {
    ($name:ident, $field:ident, $wire:literal, $item:ty) => {
        #[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
        #[serde(deny_unknown_fields)]
        pub struct $name {
            #[serde(rename = "resultType")]
            pub result_type: String,
            #[serde(rename = $wire)]
            pub $field: Vec<$item>,
            #[serde(
                rename = "nextCursor",
                default,
                skip_serializing_if = "Option::is_none"
            )]
            pub next_cursor: Option<String>,
            #[serde(rename = "ttlMs")]
            pub ttl_ms: u64,
            #[serde(rename = "cacheScope")]
            pub cache_scope: CacheScope,
            #[serde(rename = "_meta", default, skip_serializing_if = "Option::is_none")]
            pub metadata: Option<MetaMap>,
        }
    };
}

cached_list_result!(ListResourcesResult, resources, "resources", Resource);
cached_list_result!(
    ListResourceTemplatesResult,
    resource_templates,
    "resourceTemplates",
    ResourceTemplate
);

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(untagged)]
pub enum ResourceContents {
    Text {
        uri: String,
        text: String,
        #[serde(rename = "mimeType", default, skip_serializing_if = "Option::is_none")]
        mime_type: Option<String>,
        #[serde(rename = "_meta", default, skip_serializing_if = "Option::is_none")]
        metadata: Option<MetaMap>,
    },
    Blob {
        uri: String,
        blob: String,
        #[serde(rename = "mimeType", default, skip_serializing_if = "Option::is_none")]
        mime_type: Option<String>,
        #[serde(rename = "_meta", default, skip_serializing_if = "Option::is_none")]
        metadata: Option<MetaMap>,
    },
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ReadResourceResult {
    #[serde(rename = "resultType")]
    pub result_type: String,
    pub contents: Vec<ResourceContents>,
    #[serde(rename = "ttlMs")]
    pub ttl_ms: u64,
    #[serde(rename = "cacheScope")]
    pub cache_scope: CacheScope,
    #[serde(rename = "_meta", default, skip_serializing_if = "Option::is_none")]
    pub metadata: Option<MetaMap>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Role {
    Assistant,
    User,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ContentBlock {
    Text {
        text: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        annotations: Option<Value>,
        #[serde(rename = "_meta", default, skip_serializing_if = "Option::is_none")]
        metadata: Option<MetaMap>,
    },
    Image {
        data: String,
        #[serde(rename = "mimeType")]
        mime_type: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        annotations: Option<Value>,
        #[serde(rename = "_meta", default, skip_serializing_if = "Option::is_none")]
        metadata: Option<MetaMap>,
    },
    Audio {
        data: String,
        #[serde(rename = "mimeType")]
        mime_type: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        annotations: Option<Value>,
        #[serde(rename = "_meta", default, skip_serializing_if = "Option::is_none")]
        metadata: Option<MetaMap>,
    },
    ResourceLink {
        uri: String,
        name: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        title: Option<String>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        description: Option<String>,
        #[serde(rename = "mimeType", default, skip_serializing_if = "Option::is_none")]
        mime_type: Option<String>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        size: Option<u64>,
        #[serde(default, skip_serializing_if = "Vec::is_empty")]
        icons: Vec<Value>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        annotations: Option<Value>,
        #[serde(rename = "_meta", default, skip_serializing_if = "Option::is_none")]
        metadata: Option<MetaMap>,
    },
    #[serde(rename = "resource")]
    EmbeddedResource {
        resource: EmbeddedResource,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        annotations: Option<Value>,
        #[serde(rename = "_meta", default, skip_serializing_if = "Option::is_none")]
        metadata: Option<MetaMap>,
    },
}

pub type EmbeddedResource = ResourceContents;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ToolCallResult {
    #[serde(rename = "resultType")]
    pub result_type: String,
    pub content: Vec<ContentBlock>,
    #[serde(
        rename = "structuredContent",
        default,
        skip_serializing_if = "Option::is_none"
    )]
    pub structured_content: Option<Value>,
    #[serde(rename = "isError", default, skip_serializing_if = "Option::is_none")]
    pub is_error: Option<bool>,
    #[serde(rename = "_meta", default, skip_serializing_if = "Option::is_none")]
    pub metadata: Option<MetaMap>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PromptArgument {
    pub name: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub title: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub required: Option<bool>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Prompt {
    pub name: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub title: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub arguments: Vec<PromptArgument>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub icons: Vec<Value>,
    #[serde(rename = "_meta", default, skip_serializing_if = "Option::is_none")]
    pub metadata: Option<MetaMap>,
}

cached_list_result!(ListPromptsResult, prompts, "prompts", Prompt);

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PromptMessage {
    pub role: Role,
    pub content: ContentBlock,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct GetPromptResult {
    #[serde(rename = "resultType")]
    pub result_type: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    pub messages: Vec<PromptMessage>,
    #[serde(rename = "_meta", default, skip_serializing_if = "Option::is_none")]
    pub metadata: Option<MetaMap>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Completion {
    pub values: Vec<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub total: Option<u64>,
    #[serde(rename = "hasMore", default, skip_serializing_if = "Option::is_none")]
    pub has_more: Option<bool>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CompleteResult {
    #[serde(rename = "resultType")]
    pub result_type: String,
    pub completion: Completion,
    #[serde(rename = "_meta", default, skip_serializing_if = "Option::is_none")]
    pub metadata: Option<MetaMap>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type")]
pub enum CompletionReference {
    #[serde(rename = "ref/prompt")]
    Prompt {
        name: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        title: Option<String>,
    },
    #[serde(rename = "ref/resource")]
    Resource { uri: String },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CompletionArgument {
    pub name: String,
    pub value: String,
}

/// Shared pagination fields for method-specific list contracts.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PaginatedResult<T> {
    #[serde(rename = "resultType")]
    pub result_type: String,
    pub items: Vec<T>,
    #[serde(
        rename = "nextCursor",
        default,
        skip_serializing_if = "Option::is_none"
    )]
    pub next_cursor: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", tag = "mode")]
pub enum ElicitRequestParams {
    Form {
        message: String,
        #[serde(rename = "requestedSchema")]
        requested_schema: Value,
    },
    Url {
        message: String,
        url: String,
    },
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "method", content = "params")]
pub enum InputRequest {
    #[serde(rename = "elicitation/create")]
    ElicitationCreate(ElicitRequestParams),
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum ElicitAction {
    Accept,
    Decline,
    Cancel,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ElicitResult {
    pub action: ElicitAction,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub content: Option<BTreeMap<String, Value>>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(untagged)]
pub enum InputResponse {
    Elicitation(ElicitResult),
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct InputRequiredResult {
    #[serde(rename = "resultType")]
    pub result_type: String,
    #[serde(
        rename = "inputRequests",
        default,
        skip_serializing_if = "BTreeMap::is_empty"
    )]
    pub input_requests: BTreeMap<String, InputRequest>,
    #[serde(
        rename = "requestState",
        default,
        skip_serializing_if = "Option::is_none"
    )]
    pub request_state: Option<String>,
    #[serde(rename = "_meta", default, skip_serializing_if = "Option::is_none")]
    pub metadata: Option<MetaMap>,
}

impl InputRequiredResult {
    pub fn validate(&self) -> Result<(), &'static str> {
        if self.result_type != "input_required"
            || (self.input_requests.is_empty() && self.request_state.is_none())
            || self.input_requests.len() > 32
            || self
                .input_requests
                .keys()
                .any(|key| key.is_empty() || key.len() > 128)
            || self
                .request_state
                .as_ref()
                .is_some_and(|value| value.is_empty() || value.len() > 16 * 1024)
        {
            Err("invalid MCP input-required result")
        } else {
            Ok(())
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Default)]
#[serde(deny_unknown_fields)]
pub struct SubscriptionFilter {
    #[serde(
        rename = "toolsListChanged",
        default,
        skip_serializing_if = "Option::is_none"
    )]
    pub tools_list_changed: Option<bool>,
    #[serde(
        rename = "resourcesListChanged",
        default,
        skip_serializing_if = "Option::is_none"
    )]
    pub resources_list_changed: Option<bool>,
    #[serde(
        rename = "promptsListChanged",
        default,
        skip_serializing_if = "Option::is_none"
    )]
    pub prompts_list_changed: Option<bool>,
    #[serde(
        rename = "resourceSubscriptions",
        default,
        skip_serializing_if = "Vec::is_empty"
    )]
    pub resource_subscriptions: Vec<String>,
    #[serde(rename = "taskIds", default, skip_serializing_if = "Vec::is_empty")]
    pub task_ids: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SubscriptionsListenParams {
    #[serde(rename = "_meta")]
    pub metadata: RequestMetadata,
    pub notifications: SubscriptionFilter,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SubscriptionsAcknowledgedParams {
    pub notifications: SubscriptionFilter,
    #[serde(rename = "_meta", default, skip_serializing_if = "Option::is_none")]
    pub metadata: Option<MetaMap>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ResourceUpdatedNotificationParams {
    pub uri: String,
    #[serde(rename = "_meta", default, skip_serializing_if = "Option::is_none")]
    pub metadata: Option<MetaMap>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ProgressNotificationParams {
    #[serde(rename = "progressToken")]
    pub progress_token: Value,
    pub progress: f64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub total: Option<f64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub message: Option<String>,
    #[serde(rename = "_meta", default, skip_serializing_if = "Option::is_none")]
    pub metadata: Option<MetaMap>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CancelledNotificationParams {
    #[serde(rename = "requestId")]
    pub request_id: RequestId,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reason: Option<String>,
    #[serde(rename = "_meta", default, skip_serializing_if = "Option::is_none")]
    pub metadata: Option<MetaMap>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn all_content_variants_round_trip() {
        let variants = [
            json!({"type":"text","text":"hello"}),
            json!({"type":"image","data":"AA==","mimeType":"image/png"}),
            json!({"type":"audio","data":"AA==","mimeType":"audio/wav"}),
            json!({"type":"resource_link","uri":"file:///a","name":"a"}),
            json!({"type":"resource","resource":{"uri":"file:///a","text":"body"}}),
        ];
        for fixture in variants {
            let decoded: ContentBlock = serde_json::from_value(fixture.clone()).unwrap();
            assert_eq!(serde_json::to_value(decoded).unwrap(), fixture);
        }
    }

    #[test]
    fn input_required_is_closed_and_excludes_deprecated_input_kinds() {
        let value = json!({
            "resultType": "input_required",
            "requestState": "opaque",
            "inputRequests": {
                "approval": {
                    "method": "elicitation/create",
                    "params": {
                        "mode": "form",
                        "message": "Approve?",
                        "requestedSchema": {"type":"object","properties":{}}
                    }
                }
            }
        });
        let decoded: InputRequiredResult = serde_json::from_value(value).unwrap();
        decoded.validate().unwrap();
        assert!(serde_json::from_value::<InputRequest>(json!({
            "method":"sampling/createMessage",
            "params":{}
        }))
        .is_err());
    }

    #[test]
    fn metadata_is_bounded() {
        let too_many = (0..=MAX_META_ENTRIES)
            .map(|index| (format!("com.example/{index}"), Value::Null))
            .collect();
        assert!(MetaMap::new(too_many).is_err());
        assert!(MetaMap::new(BTreeMap::from([(
            "com.example/value".to_owned(),
            Value::String("x".repeat(MAX_META_BYTES)),
        )]))
        .is_err());
    }
}
