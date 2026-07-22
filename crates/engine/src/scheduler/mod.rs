//! Pure deterministic scheduler core for Canonical Typed Plans.
//!
//! This module deliberately contains no repository, clock, network, worker,
//! random, or process-global dependency. It translates a verified `LinkedPlan`
//! plus committed projection facts into at most one inert closed action.

mod action;
mod binding;
mod error;
mod facts;
mod id;
mod planner;

pub use action::*;
pub use error::*;
pub use facts::*;
pub use id::*;
pub use planner::*;
#[allow(unused_imports)]
pub(crate) use planner::{
    derive_subflow_admission, derive_subflow_invocation, scope_instance_for_occurrence,
    scope_instance_for_runtime_scope,
};
