mod author;
pub(crate) mod chat;
pub mod compiler;
pub mod eval;
pub mod input;
pub mod ir;
pub mod lower;
// Compiler-normalized message AST. Authored documents enter through `author`.
pub(crate) mod message;
pub(crate) mod operation;
pub mod plan;
mod predicate;
pub(crate) mod raw;
pub mod runtime_message;
pub mod schema;
pub mod semantics;
pub mod shape;
pub mod template;
pub mod types;
pub mod value;

pub use value::{Identifier, ValueExpr, ValuePath, ValuePathRoot};

pub const AUTHOR_PARSE_ERROR_CODE: &str = raw::PARSE_ERROR_CODE;

/// Validates the single public authoring surface without exposing the
/// compiler-normalized grammar as a second construction API.
pub fn validate_author_source(source: &str) -> Result<(), super::DslParseError> {
    raw::parse_workflow(source).map(|_| ())
}
