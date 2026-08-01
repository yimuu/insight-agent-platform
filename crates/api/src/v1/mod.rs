//! Version 1 HTTP API and streaming transport.

mod auth;
mod conversation;
mod mcp_catalog;
mod mcp_interactions;
mod mcp_management;
mod mcp_oauth;
mod models;
mod response;
mod routes;
mod sse;

pub use auth::{
    ApiAuth, BearerHumanPrincipalResolver, HumanPrincipalResolver, OperatorAuth, OperatorPrincipal,
    ResolvedHumanPrincipal, MCP_SERVER_DISCOVER, MCP_SERVER_PUBLISH, MCP_SERVER_READ,
    MCP_SERVER_WRITE,
};
pub use conversation::{
    AppendConversationMessageRequest, ArchiveConversationRequest, ConversationDeleteDto,
    ConversationMessageCursor, ConversationMessagePageDto, ConversationMessagesQuery,
    ConversationTurnDto, CreateConversationRequest,
};
pub use mcp_catalog::{
    build_mcp_catalog_router, McpCatalogApiState, McpCatalogRegistry, McpCatalogServer,
    McpProfileReport, McpProfileState,
};
pub use mcp_management::{
    build_mcp_management_router, LocalMcpRevisionReadiness, McpDraftAuthorization,
    McpDraftDiscovery, McpDraftDocument, McpDraftImports, McpDraftLimits, McpDraftPromptImport,
    McpDraftProtocol, McpDraftResourceImports, McpDraftResourceRule, McpDraftToolImport,
    McpDraftTransport, McpManagementApiState, McpManagementPolicy, McpManifestSignerPolicy,
    McpRevisionAuthoring, McpRevisionReadiness, McpToolDescriptionPolicy, McpToolPublicPolicy,
};
pub use mcp_oauth::{build_mcp_oauth_router, McpOAuthApiState, McpOAuthRegistry, McpOAuthServer};
pub use models::{
    PersistenceMode, RecoveryCapability, RecoveryRunDto, RunDto, RunPersistenceCapability,
};
pub use routes::{
    build_router, build_router_with_mcp, build_router_with_mcp_interactions, ApiState,
};
pub use sse::{TerminalFrameBarrier, TerminalFrameBarrierError};
