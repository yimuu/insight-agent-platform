//! Structured Agent DSL authoring, validation, and compilation.

pub use insight_engine::author::{
    CompileError, DslParseError, DslPath, DslPathSegment, SourceSpan,
};

pub mod ast;
pub mod compiler;
pub mod expression;
pub mod graph;
pub mod graph_repository;
pub mod raw;
mod reducer;
pub mod template;

pub use ast::{validate, StructuredAuthorDocument};
pub use compiler::{compile, compile_source, CompileOptions, COMPILER_VERSION};
pub use graph::*;
pub use graph_repository::{GraphSurfaceRepository, StoredGraphView};
pub use raw::{parse, RawDocument};

pub const API_VERSION: &str = "insight.agent/v1";
pub const DOCUMENT_KIND: &str = "agent";

pub const PARSE_FAILED: &str = "DSL_PARSE_FAILED";
pub const DUPLICATE_KEY: &str = "DSL_DUPLICATE_KEY";
pub const INVALID_DOCUMENT: &str = "DSL_DOCUMENT_INVALID";
pub const INVALID_STEP: &str = "DSL_STEP_INVALID";
pub const INVALID_TYPE: &str = "DSL_TYPE_INVALID";
pub const INVALID_REFERENCE: &str = "DSL_REFERENCE_INVALID";
pub const INVALID_CONTROL_FLOW: &str = "DSL_CONTROL_FLOW_INVALID";
pub const EXPRESSION_ENGINE_BLOCKED: &str = "DSL_EXPRESSION_ENGINE_BLOCKED";
pub const DESCRIPTOR_CONTRACT_BLOCKED: &str = "DSL_DESCRIPTOR_CONTRACT_BLOCKED";
pub const PROMPT_RESOURCE_BLOCKED: &str = "DSL_PROMPT_RESOURCE_BLOCKED";
