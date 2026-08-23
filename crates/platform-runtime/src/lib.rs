//! Application composition for the clean-cut Platform v1 runtime roles.
//!
//! Domain crates remain pure and PostgreSQL remains the durable authority. This crate connects
//! process-local Worker capacity to durable claim transactions and owns role-scoped runtime I/O.

mod execution;
mod identity;
mod orchestration;
mod plan_materialization;
pub mod postgres;
mod safety;
mod sandbox_executor;
mod sandbox_outcome;
mod sandbox_recovery;

pub use execution::*;
pub use identity::*;
pub use insight_platform_sandbox_rpc::{
    NatsSandboxControlListener, NatsSandboxControlSignalSink, NatsSandboxControlTransportConfig,
    NATS_SANDBOX_CONTROL_SUBJECT_PREFIX,
};
pub use orchestration::*;
pub use plan_materialization::*;
pub use safety::*;
pub use sandbox_executor::*;
pub use sandbox_outcome::*;
pub use sandbox_recovery::*;
