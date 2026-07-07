pub mod examples;

use crate::code::registry::CodeRegistry;

pub fn default_code_registry() -> CodeRegistry {
    let mut registry = CodeRegistry::default();
    examples::register(&mut registry);
    registry
}
