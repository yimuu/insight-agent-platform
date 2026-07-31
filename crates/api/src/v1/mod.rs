//! Version 1 HTTP API and streaming transport.

mod auth;
mod conversation;
mod mcp_catalog;
mod mcp_interactions;
mod mcp_oauth;
mod models;
mod response;
mod routes;
mod sse;

pub use auth::{
    ApiAuth, BearerHumanPrincipalResolver, HumanPrincipalResolver, ResolvedHumanPrincipal,
};
pub use conversation::{
    AppendConversationMessageRequest, ArchiveConversationRequest, ConversationDeleteDto,
    ConversationMessageCursor, ConversationMessagePageDto, ConversationMessagesQuery,
    ConversationTurnDto, CreateConversationRequest,
};
pub use mcp_catalog::{
    build_mcp_catalog_router, McpCatalogApiState, McpCatalogServer, McpProfileReport,
    McpProfileState,
};
pub use mcp_oauth::{build_mcp_oauth_router, McpOAuthApiState, McpOAuthServer};
pub use models::{
    PersistenceMode, RecoveryCapability, RecoveryRunDto, RunDto, RunPersistenceCapability,
};
pub use routes::{
    build_router, build_router_with_mcp, build_router_with_mcp_interactions, ApiState,
};
pub use sse::{TerminalFrameBarrier, TerminalFrameBarrierError};
