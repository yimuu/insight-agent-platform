//! Offline generation and conformance tooling for the owning boundary contracts.
//! Runtime crates must never depend on this package.

#![recursion_limit = "256"]

pub mod machine;

pub mod recovery_validation;
pub mod schema_deployment;
pub mod worker_deployment;
