//! Protocol-owned Model Context Protocol contracts.
//!
//! This crate deliberately contains no platform policy, database, Agent DSL,
//! or runtime authority. It owns bounded wire decoding, protocol negotiation,
//! content types, and immutable binding evidence shared by MCP adapters.

mod binding;
mod client;
mod codec;
mod headers;
mod legacy;
mod negotiation;
mod oauth;
mod observability;
mod server;
mod tasks;
mod transport;
mod wire;

pub use binding::{
    BindingIdentityError, McpPromptBinding, McpResourceBinding, McpResourceBindingKind,
    McpServerBindingIdentity, McpToolBinding, McpToolBindingDescriptor, McpTransportKind,
    PrincipalScope,
};
pub use client::{
    CatalogRejection, ClientError, McpCallOutcome, McpCatalog, McpClient, McpClientLimits,
    McpGetPromptOutcome, McpReadResourceOutcome, PromptCatalog, ResourceCatalog,
    ResourceTemplateCatalog, ToolCatalog, ToolCatalogRejection,
};
pub use codec::{CodecError, McpCodec, ProtocolLimits};
pub use headers::{
    decode_header_value, encode_header_value, InvalidToolHeader, ToolHeaderError, ToolHeaderPlan,
    ToolHeaderRejection,
};
pub use legacy::{LegacyCompatibilityTransport, LegacyError};
pub use negotiation::{negotiate_version, VersionNegotiationError};
pub use oauth::{
    OAuthAuthorizationServerMetadata, OAuthClient, OAuthClientRegistration, OAuthError,
    OAuthPkceTransaction, OAuthProtectedResourceMetadata, OAuthTokenSet,
};
pub use observability::{
    adjust_operational_gauge, prometheus_metrics, record_operational_event, set_operational_gauge,
    McpOperationalEvent, McpOperationalGauge,
};
pub use server::{
    McpRequestHeaders, McpServerBackend, McpServerDispatcher, McpServerError, McpServerReply,
    McpServerRequestContext,
};
pub use tasks::{
    valid_task_id, CreateTaskResult, GetTaskResult, Task, TaskAcknowledgement, TaskStatus,
    UpdateTaskParams, MCP_TASKS_EXTENSION_ID,
};
pub use transport::{
    build_pinned_http_client, HttpCredential, McpNotificationObserver, McpTransport,
    NoopNotificationObserver, StdioTransport, StdioTransportPolicy, StreamableHttpPolicy,
    StreamableHttpTransport, TransportError, TransportKind,
};
pub use wire::{
    CacheScope, CancelledNotificationParams, ClientCapabilities, ClientInfo, CompleteResult,
    Completion, CompletionArgument, CompletionReference, ContentBlock, CoreMethod, DiscoverParams,
    DiscoverResult, ElicitAction, ElicitRequestParams, ElicitResult, EmbeddedResource, EmptyParams,
    GetPromptResult, InputRequest, InputRequiredResult, InputResponse, JsonRpcError,
    JsonRpcErrorResponse, JsonRpcNotification, JsonRpcRequest, JsonRpcResponse, ListPromptsResult,
    ListResourceTemplatesResult, ListResourcesResult, ListToolsResult, MetaMap, ModernRequest,
    PaginatedResult, ProgressNotificationParams, Prompt, PromptArgument, PromptMessage,
    ReadResourceResult, RequestId, RequestMetadata, Resource, ResourceContents, ResourceTemplate,
    ResourceUpdatedNotificationParams, Role, ServerCapabilities, ServerInfo, SubscriptionFilter,
    SubscriptionsAcknowledgedParams, SubscriptionsListenParams, Tool, ToolAnnotations,
    ToolCallResult, JSON_RPC_VERSION, MCP_LEGACY_PROTOCOL_VERSION, MCP_PROTOCOL_VERSION,
};
