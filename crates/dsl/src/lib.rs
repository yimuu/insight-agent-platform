//! DSL authoring, validation, and compilation.

pub use insight_engine::author::{
    CompileError, DslParseError, DslPath, DslPathSegment, SourceSpan,
};

pub mod v3;
