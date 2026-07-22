pub mod v3 {
    pub mod ast {
        pub use insight_dsl::v3::ast::*;
    }

    pub mod compiler {
        pub use insight_dsl::v3::compiler::*;
    }

    pub mod expression {
        pub use insight_dsl::v3::expression::*;
    }

    pub mod graph {
        pub use insight_dsl::v3::graph::*;
    }

    pub mod graph_repository {
        pub use insight_dsl::v3::graph_repository::{GraphSurfaceRepository, StoredGraphView};
    }

    pub mod raw {
        pub use insight_dsl::v3::raw::*;
    }

    pub mod template {
        pub use insight_dsl::v3::template::*;
    }

    pub use ast::{validate, StructuredAuthorDocument};
    pub use compiler::{compile, compile_source, CompileOptions, V3_COMPILER_VERSION};
    pub use graph::*;
    pub use graph_repository::{GraphSurfaceRepository, StoredGraphView};
    pub use insight_dsl::v3::{
        API_VERSION, DESCRIPTOR_CONTRACT_BLOCKED, DOCUMENT_KIND, DUPLICATE_KEY,
        EXPRESSION_ENGINE_BLOCKED, INVALID_CONTROL_FLOW, INVALID_DOCUMENT, INVALID_REFERENCE,
        INVALID_STEP, INVALID_TYPE, PARSE_FAILED, PROMPT_RESOURCE_BLOCKED,
    };
    pub use raw::{parse, RawDocument};
}

pub use insight_engine::author::{
    CompileError, DslParseError, DslPath, DslPathSegment, SourceSpan,
};
