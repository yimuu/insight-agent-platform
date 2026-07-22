//! Clean-break v3 structured authoring surface.
//!
//! The public pipeline is deliberately split into three boundaries:
//! [`raw`] owns inert wire decoding, [`ast`] owns author-language validation,
//! and [`compiler`] is the only module allowed to create executable Plan
//! nodes. Structured containers are therefore never executable runtime nodes.

pub mod ast;
pub mod compiler;
pub mod expression;
pub mod graph;
pub mod graph_repository;
pub mod raw;
mod reducer;
pub mod template;

pub use ast::{validate, StructuredAuthorDocument};
pub use compiler::{compile, compile_source, CompileOptions, V3_COMPILER_VERSION};
pub use graph::*;
pub use graph_repository::{GraphSurfaceRepository, StoredGraphView};
pub use raw::{parse, RawDocument};

pub const API_VERSION: &str = "insight.agent/v3";
pub const DOCUMENT_KIND: &str = "agent";

pub const PARSE_FAILED: &str = "DSL_V3_PARSE_FAILED";
pub const DUPLICATE_KEY: &str = "DSL_V3_DUPLICATE_KEY";
pub const INVALID_DOCUMENT: &str = "DSL_V3_DOCUMENT_INVALID";
pub const INVALID_STEP: &str = "DSL_V3_STEP_INVALID";
pub const INVALID_TYPE: &str = "DSL_V3_TYPE_INVALID";
pub const INVALID_REFERENCE: &str = "DSL_V3_REFERENCE_INVALID";
pub const INVALID_CONTROL_FLOW: &str = "DSL_V3_CONTROL_FLOW_INVALID";
pub const EXPRESSION_ENGINE_BLOCKED: &str = "DSL_V3_EXPRESSION_ENGINE_BLOCKED";
pub const DESCRIPTOR_CONTRACT_BLOCKED: &str = "DSL_V3_DESCRIPTOR_CONTRACT_BLOCKED";
pub const PROMPT_RESOURCE_BLOCKED: &str = "DSL_V3_PROMPT_RESOURCE_BLOCKED";
