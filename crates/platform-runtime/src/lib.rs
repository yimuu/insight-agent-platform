//! Application composition for the clean-cut Platform v1 runtime roles.
//!
//! Domain crates remain pure and PostgreSQL remains the durable authority. This crate connects
//! process-local Worker capacity to durable claim transactions and owns role-scoped runtime I/O.

mod controller_admission;
mod controller_mutations;
mod execution;
mod generation_handler;
mod identity;
mod orchestration;
mod plan_driver;
mod plan_materialization;
pub mod postgres;
mod postgres_plan_store;
mod production_orchestration;
mod safety;

pub use controller_admission::*;
pub use controller_mutations::*;
pub use execution::*;
pub use generation_handler::*;
pub use identity::*;
pub use orchestration::*;
pub use plan_driver::*;
pub use plan_materialization::*;
pub use postgres_plan_store::*;
pub use production_orchestration::*;
pub use safety::*;
