//! Canonical Typed Plan model.
//!
//! The plan is intentionally independent from the legacy Region/SSA runtime.
//! Author documents compile into this immutable boundary; schedulers never
//! execute authored containers directly.

mod builder;
mod error;
pub mod expression;
mod id;
mod index;
mod linker;
mod model;
mod semantic;
pub mod types;
mod verify;

pub use builder::PlanBuilder;
pub use error::*;
pub use id::*;
pub use index::*;
pub use linker::*;
pub use model::*;
pub use types::{
    PlanProperty, PlanType, PlanTypeError, PLAN_TYPE_CANONICALIZATION_FAILED,
    PLAN_TYPE_REGEX_ENGINE_VERSION, PLAN_TYPE_UNION_EMPTY, PLAN_TYPE_WIRE_INVALID,
    PLAN_TYPE_WIRE_VERSION,
};
