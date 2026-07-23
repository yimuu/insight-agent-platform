use insight_dsl::v3::{compile_source, CompileOptions, GraphAuthorDocument, GraphDocumentId};
use insight_engine::{
    plan::{DescriptorValue, LeafTaskKind, NodeKind, PlanIndex},
    DefinitionRevisionId,
};

const RETRIEVAL_SOURCE: &str = r#"api_version: insight.agent/v3
kind: agent
types:
  SearchOutput:
    fields:
      answer: string
inputs:
  question: string
output: SearchOutput
workflow:
  steps:
    - type: retrieval
      id: search
      retrieval: medical.search
      inputs:
        query: $question
      response: SearchOutput
    - return: $search
"#;

fn compile_options(source: &str) -> CompileOptions {
    CompileOptions::new(
        DefinitionRevisionId::new("retrieval_dsl_fixture_revision").unwrap(),
        "retrieval/agent.yaml",
        source,
    )
}

#[test]
fn retrieval_is_a_first_class_leaf_and_publish_defaults_private_across_roundtrip() {
    let plan = compile_source(RETRIEVAL_SOURCE, compile_options(RETRIEVAL_SOURCE)).unwrap();
    let retrieval = plan
        .nodes()
        .iter()
        .find(|node| matches!(node.kind(), NodeKind::RetrievalTask(_)))
        .expect("retrieval leaf is represented by its own Plan node kind");
    let NodeKind::RetrievalTask(descriptor) = retrieval.kind() else {
        unreachable!();
    };
    assert_eq!(descriptor.implementation, "medical.search");
    assert_eq!(descriptor.descriptor_version.as_str(), "1");
    assert_eq!(
        descriptor.public_configuration.get("publish"),
        Some(&DescriptorValue::Boolean(false))
    );
    assert_eq!(
        PlanIndex::new(&plan)
            .unwrap()
            .leaf_descriptor(retrieval.id())
            .unwrap()
            .kind(),
        LeafTaskKind::Retrieval
    );

    let native = GraphAuthorDocument::from_verified_plan(
        GraphDocumentId::new("native_retrieval_roundtrip").unwrap(),
        plan,
    )
    .unwrap();
    let reduced = native.to_structured().unwrap();
    let recompiled = compile_source(reduced.source(), compile_options(reduced.source())).unwrap();
    assert_eq!(native.semantic_hash(), recompiled.semantic_hash());
    assert!(reduced.source().contains("\"type\": \"retrieval\""));
    assert!(reduced.source().contains("\"publish\": false"));
}

#[test]
fn retrieval_author_surface_is_closed_and_requires_object_inputs() {
    let missing_inputs = RETRIEVAL_SOURCE.replace("      inputs:\n        query: $question\n", "");
    assert!(compile_source(&missing_inputs, compile_options(&missing_inputs)).is_err());

    let invalid_publish = RETRIEVAL_SOURCE.replace(
        "      response: SearchOutput",
        "      publish: yes\n      response: SearchOutput",
    );
    assert!(compile_source(&invalid_publish, compile_options(&invalid_publish)).is_err());

    let unknown = RETRIEVAL_SOURCE.replace(
        "      response: SearchOutput",
        "      arbitrary: true\n      response: SearchOutput",
    );
    assert!(compile_source(&unknown, compile_options(&unknown)).is_err());
}
