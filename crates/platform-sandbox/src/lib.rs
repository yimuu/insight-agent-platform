//! Fail-closed Sandbox Execution Plane contracts for Platform v1.
//!
//! This crate owns the typed execution envelope, isolation selection, physical Sandbox state and
//! Executor orchestration ports. Durable current-state authority remains the shared Job/Receipt/
//! Event/Outbox repository; concrete WASI, gVisor and microVM SDKs stay behind backend ports.

mod backend;
mod broker;
mod control;
mod state;
mod types;
mod worker;

pub use backend::*;
pub use broker::*;
pub use control::*;
pub use state::*;
pub use types::*;
pub use worker::*;

#[cfg(test)]
mod tests;
