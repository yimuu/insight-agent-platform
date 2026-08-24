//! Application composition for the clean-cut Platform v1 runtime roles.
//!
//! Domain crates remain pure and PostgreSQL remains the durable authority. This crate connects
//! process-local Worker capacity to durable claim transactions and owns role-scoped runtime I/O.

mod controller_mutations;
mod execution;
mod generation_handler;
mod identity;
mod orchestration;
mod plan_driver;
mod plan_materialization;
pub mod postgres;
mod postgres_plan_store;
mod safety;
mod sandbox_executor;
mod sandbox_outcome;
mod sandbox_recovery;

pub use controller_mutations::*;
pub use execution::*;
pub use generation_handler::*;
pub use identity::*;
pub use insight_platform_sandbox_rpc::{
    NatsSandboxControlListener, NatsSandboxControlSignalSink, NatsSandboxControlTransportConfig,
    NATS_SANDBOX_CONTROL_SUBJECT_PREFIX,
};
pub use orchestration::*;
pub use plan_driver::*;
pub use plan_materialization::*;
pub use postgres_plan_store::*;
pub use safety::*;
pub use sandbox_executor::*;
pub use sandbox_outcome::*;
pub use sandbox_recovery::*;
