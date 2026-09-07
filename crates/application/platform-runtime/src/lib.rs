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
mod safety;

pub use controller_mutations::*;
pub use execution::*;
pub use generation_handler::*;
pub use identity::*;
pub use orchestration::*;
pub use plan_driver::*;
pub use plan_materialization::*;
pub use safety::*;

mod admission;
mod run_value_materialization;
pub use admission::*;
pub use run_value_materialization::*;

#[cfg(test)]
fn test_execution_requirement() -> insight_platform_contracts::ExecutionRequirement {
    insight_platform_contracts::ExecutionRequirement::Program {
        definition_digest: format!("sha256:{}", "1".repeat(64)).parse().unwrap(),
        program_semantic_identity: insight_platform_plan::execution::program_semantic_identity(6)
            .unwrap(),
        ir_abi_version: 6,
    }
}

#[cfg(test)]
fn test_orchestration_payload(
    node_execution_id: insight_platform_contracts::ResourceId,
) -> insight_platform_contracts::TypedPayload {
    insight_platform_orchestrator::OrchestrationJobPayload {
        bindings_digest: format!("sha256:{}", "c".repeat(64)).parse().unwrap(),
        node_execution_id,
        root_scope_id: "scp_0198f1c5-0787-75e1-a9e8-d95ca0f39999".parse().unwrap(),
        retry_backoff_milliseconds: 100,
        wake_contract: None,
        convergence_failure: None,
        model_tool_continuation: None,
        external_leaf_completion: None,
    }
    .to_payload()
    .unwrap()
}
