//! Version 1 HTTP API and streaming transport.

mod auth;
mod conversation;
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
pub use models::{
    PersistenceMode, RecoveryCapability, RecoveryRunDto, RunDto, RunPersistenceCapability,
};
pub use routes::{build_router, ApiState};
pub use sse::{TerminalFrameBarrier, TerminalFrameBarrierError};
