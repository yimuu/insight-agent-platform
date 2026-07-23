//! Version 1 HTTP API and streaming transport.

mod auth;
mod response;
mod routes;
mod sse;

pub use auth::{
    ApiAuth, BearerHumanPrincipalResolver, HumanPrincipalResolver, ResolvedHumanPrincipal,
};
pub use routes::{build_router, ApiState};
