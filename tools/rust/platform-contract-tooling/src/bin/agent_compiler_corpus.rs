//! Reproduce the current compiler conformance corpus with the native owner.
use insight_platform_agent_compiler::{
    compile_agent, AgentCompilerInput, AgentCompilerProfile, ResolvedAgentBindings,
};
use insight_platform_contracts::canonical_digest;
use serde_json::{json, Value};
use std::fs;
fn main() {
    let write = std::env::args().skip(1).collect::<Vec<_>>();
    assert!(
        write.is_empty() || write == ["--write"],
        "usage: agent_compiler_corpus [--write]"
    );
    let root = insight_platform_contract_tooling::machine::repository_root_from_manifest()
        .join("contracts/product-experience/agent-compiler/v2");
    let mut corpus: Value =
        serde_json::from_slice(&fs::read(root.join("corpus.json")).unwrap()).unwrap();
    let profile: AgentCompilerProfile = serde_json::from_value(corpus["profile"].clone()).unwrap();
    for case in corpus["cases"].as_array_mut().unwrap() {
        let compiled = compile_agent(AgentCompilerInput {
            plan_bytes: None,
            manifest_bytes: fs::read(root.join(case["manifest"].as_str().unwrap())).unwrap(),
            input_schema_bytes: fs::read(root.join(case["input_schema"].as_str().unwrap()))
                .unwrap(),
            output_schema_bytes: fs::read(root.join(case["output_schema"].as_str().unwrap()))
                .unwrap(),
            profile: profile.clone(),
            bindings: serde_json::from_value::<ResolvedAgentBindings>(case["bindings"].clone())
                .unwrap(),
        })
        .unwrap();
        let digest = |value: Value| canonical_digest(&value).unwrap();
        let expected = json!({"canonical_manifest":std::str::from_utf8(&compiled.canonical_manifest_bytes).unwrap(),"contract_digest":compiled.resource_intent.contract_digest,"deployment_intent_digest":digest(serde_json::to_value(&compiled.deployment_intent).unwrap()),"execution_kind":compiled.execution_kind,"lifecycle_plan_digest":digest(serde_json::to_value(&compiled.lifecycle_plan).unwrap()),"manifest_digest":compiled.manifest_digest,"name":compiled.name,"required_features":compiled.required_features,"resource_intent_digest":digest(serde_json::to_value(&compiled.resource_intent).unwrap()),"typed_plan":std::str::from_utf8(&compiled.typed_plan_bytes).unwrap(),"typed_plan_digest":compiled.typed_plan_digest});
        if write.is_empty() {
            assert_eq!(
                case["expected"], expected,
                "compiler corpus differs for {}",
                case["case_id"]
            );
        } else {
            case["expected"] = expected;
        }
    }
    if !write.is_empty() {
        let mut bytes = serde_json::to_vec_pretty(&corpus).unwrap();
        bytes.push(b'\n');
        fs::write(root.join("corpus.json"), bytes).unwrap();
    }
    println!("current Agent compiler corpus verified");
}
