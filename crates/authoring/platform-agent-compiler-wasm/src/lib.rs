//! Byte transport only. All parsing, validation, defaults and compilation live in the Rust core.

use wasm_bindgen::prelude::*;

#[wasm_bindgen]
pub fn compile_agent(input: &[u8]) -> Vec<u8> {
    insight_platform_agent_compiler::compile_request_bytes(input)
}

#[wasm_bindgen]
pub fn compiler_semantic_identity() -> String {
    insight_platform_agent_compiler::compiler_semantic_identity().to_string()
}

#[wasm_bindgen]
pub fn compile_sources(input: &[u8]) -> Vec<u8> {
    insight_platform_agent_compiler::compile_authoring_request_bytes(input)
}

#[wasm_bindgen]
pub fn inspect_agent_manifest(input: &[u8]) -> Vec<u8> {
    insight_platform_agent_compiler::inspect_manifest_request_bytes(input)
}

#[wasm_bindgen]
pub fn canonical_digest_json(input: &[u8]) -> Vec<u8> {
    insight_platform_agent_compiler::canonical_digest_request_bytes(input)
}

#[wasm_bindgen]
pub fn build_expression_json(input: &[u8]) -> Vec<u8> {
    insight_platform_agent_compiler::editor::build_expression_request_bytes(input)
}

#[wasm_bindgen]
pub fn inspect_agent_sources(input: &[u8]) -> Vec<u8> {
    insight_platform_agent_compiler::inspect_source_request_bytes(input)
}
