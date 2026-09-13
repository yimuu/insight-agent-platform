use super::*;

fn text2sql_bundle() -> AgentSourceBundleV1 {
    let mut bundle = source_bundle();
    bundle.bindings = model_binding();
    bundle.sources.files = BTreeMap::from([
        (
            "agent.yaml".into(),
            include_str!("../../../../examples/productization/text2sql/agent.yaml").into(),
        ),
        (
            "workflow.yaml".into(),
            include_str!("../../../../examples/productization/text2sql/workflow.yaml").into(),
        ),
        (
            "input.schema.json".into(),
            include_str!("../../../../examples/productization/text2sql/input.schema.json").into(),
        ),
        (
            "output.schema.json".into(),
            include_str!("../../../../examples/productization/text2sql/output.schema.json").into(),
        ),
    ]);
    bundle
}

#[test]
fn text2sql_source_roundtrip_transport_and_registry_recompile_agree() {
    let bundle = text2sql_bundle();
    assert_eq!(
        inspect_source_files(&bundle.sources)
            .unwrap()
            .execution_kind,
        AgentExecutionKind::Workflow
    );
    let response = compile_source_bundle(bundle.clone());
    let AgentCompileResponseV1::Compiled { compilation } = &response else {
        panic!("{response:?}")
    };
    assert_eq!(
        serde_json::to_vec(&response).unwrap(),
        compile_request_bytes(&serde_json::to_vec(&bundle).unwrap())
    );
    let restored: AgentSourceBundleV1 =
        serde_json::from_slice(&compilation.source_bundle_bytes).unwrap();
    assert_eq!(restored.sources, bundle.sources);
    let AgentCompileResponseV1::Compiled {
        compilation: rebuilt,
    } = compile_source_bundle(restored)
    else {
        panic!("restored source rejected")
    };
    assert_eq!(
        rebuilt.compiled.typed_plan_bytes,
        compilation.compiled.typed_plan_bytes
    );
    let authority = |intent: &ArtifactIntent, suffix| ArtifactAuthority {
        purpose: intent.purpose,
        state: ArtifactState::Ready,
        artifact: ArtifactRef::new(
            id(ResourceKind::Artifact, suffix),
            intent.content_digest.clone(),
            intent.byte_length,
            intent.media_type.clone(),
            intent.classification,
            intent.display_name.clone(),
        )
        .unwrap(),
    };
    let source = authority(&compilation.compiled.resource_intent.authoring_artifact, 60);
    let plan = authority(
        &compilation.compiled.resource_intent.typed_plan_artifact,
        61,
    );
    let document = compilation.materialize(&source, &plan).unwrap();
    validate_frozen_agent_artifacts(
        &document,
        &source,
        &compilation.source_bundle_bytes,
        &plan,
        &compilation.compiled.typed_plan_bytes,
    )
    .unwrap();
    assert!(!compilation
        .compiled
        .resource_intent
        .author_instructions
        .as_ref()
        .unwrap()
        .contains("{{"));
    let mut changed = bundle;
    changed
        .sources
        .files
        .get_mut("workflow.yaml")
        .unwrap()
        .push_str("\nextra: false\n");
    assert!(inspect_source_files(&changed.sources).is_err());
}

#[test]
fn invalid_prompt_references_fail_offline_before_binding_resolution() {
    for bad in [
        "{{ input.missing }}",
        "{{ nodes.review.output.sql }}",
        "{{ input.schema_text",
        "{{ input.schema_text | upper }}",
    ] {
        let mut bundle = text2sql_bundle();
        bundle.bindings = ResolvedAgentBindings::default();
        let source = bundle.sources.files.get_mut("workflow.yaml").unwrap();
        *source = source.replacen("{{ input.schema_text }}", bad, 1);
        assert!(
            inspect_source_files(&bundle.sources).is_err(),
            "accepted {bad}"
        );
    }
}
