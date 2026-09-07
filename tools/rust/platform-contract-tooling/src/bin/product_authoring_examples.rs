//! Reproducible source fixtures: exact IDs are test data, never deployment proof.
use insight_platform_agent_compiler::{evaluation::*, framework::*, *};
use insight_platform_contracts::*;
use insight_platform_plan::{ChildBudgetLimit, RuntimePlan};
use serde_json::{json, Value};
use std::{collections::BTreeMap, fs, path::Path};
fn id(kind: ResourceKind, suffix: u32) -> ResourceId {
    format!(
        "{}_018f3e20-0000-7000-8000-{suffix:012x}",
        kind.descriptor().prefix
    )
    .parse()
    .unwrap()
}
fn digest(value: &Value) -> Sha256Digest {
    canonical_digest(value).unwrap().parse().unwrap()
}
fn artifact(suffix: u32, value: &Value) -> ArtifactRef {
    ArtifactRef::new(
        id(ResourceKind::Artifact, suffix),
        digest(value),
        canonical_json(value).unwrap().len() as u64,
        "application/json",
        DataClassification::Restricted,
        None,
    )
    .unwrap()
}
fn policy(suffix: u32) -> ExactPolicyBinding {
    ExactPolicyBinding {
        deployment: ExactDeploymentRef::new(
            id(ResourceKind::PolicyDeployment, suffix),
            digest(&json!({"fixture_policy":suffix})),
        )
        .unwrap(),
        revision: ExactVersionRef::new(
            id(ResourceKind::PolicyRevision, suffix + 1),
            digest(&json!({"fixture_revision":suffix})),
        )
        .unwrap(),
    }
}
fn examples(root: &Path) -> BTreeMap<String, Vec<u8>> {
    let corpus_root = root.join("contracts/product-experience/agent-compiler/v2");
    let corpus: Value =
        serde_json::from_slice(&fs::read(corpus_root.join("corpus.json")).unwrap()).unwrap();
    let profile: AgentCompilerProfile = serde_json::from_value(corpus["profile"].clone()).unwrap();
    let schema = ClosedJsonSchema::build(
        serde_json::from_slice(&fs::read(corpus_root.join("schema-message.json")).unwrap())
            .unwrap(),
    )
    .unwrap();
    let plan: RuntimePlan = serde_json::from_str(
        corpus["cases"][0]["expected"]["typed_plan"]
            .as_str()
            .unwrap(),
    )
    .unwrap();
    let graph = StaticFrameworkExportV1 {
        schema_version: FRAMEWORK_EXPORT_VERSION,
        dialect: FrameworkExportDialect::LangGraphStaticTypedPortsV1,
        adapter_semantic_identity: framework_adapter_semantic_identity(),
        entry_node_id: plan.entry_node_id,
        nodes: plan.nodes,
        dependency_slots: plan.dependency_slots,
        schema_documents: plan.schema_documents,
    };
    let imported = import_framework(FrameworkImportRequestV1 {
        schema_version: 1,
        name: "compiled-echo".into(),
        display_name: "Compiled framework echo".into(),
        input_schema: schema.clone(),
        output_schema: schema.clone(),
        input_classification: DataClassification::Internal,
        profile: profile.clone(),
        bindings: ResolvedAgentBindings::default(),
        graph,
    })
    .unwrap();
    let mut outputs = BTreeMap::new();
    for (name, source) in imported.source_bundle.sources.files {
        outputs.insert(
            format!("examples/productization/compiled-framework/{name}"),
            source.into_bytes(),
        );
    }
    let sample = json!({"message":"A reproducible evaluation sample"});
    let manifest=EvaluationManifestV1{schema_version:1,dataset_id:"echo-fixture".into(),samples:vec![EvaluationSampleV1{sample_id:"one".into(),input:artifact(501,&sample),expected:Some(artifact(502,&sample)),input_schema_digest:schema.canonical_digest.clone(),expected_schema_digest:Some(schema.canonical_digest.clone())}],repetitions:2,subject:ExactDeploymentRef::new(id(ResourceKind::AgentDeployment,503),digest(&json!({"fixture_subject":"echo"}))).unwrap(),evaluator:ExactDeploymentRef::new(id(ResourceKind::AgentDeployment,504),digest(&json!({"fixture_evaluator":"metrics"}))).unwrap(),metric_schema:ClosedJsonSchema::build(json!({"$schema":"https://json-schema.org/draft/2020-12/schema","type":"object","additionalProperties":false,"properties":{"score":{"type":"integer","minimum":0,"maximum":1}},"required":["score"]})).unwrap()};
    let manifest_value = serde_json::to_value(&manifest).unwrap();
    let mut request = EvaluationPlanRequestV1 {
        deployment_features: Vec::new(),
        schema_version: 1,
        name: "echo-evaluation".into(),
        display_name: "Echo evaluation".into(),
        manifest_artifact: artifact(505, &manifest_value),
        manifest: manifest.clone(),
        subject_input_schema: schema.clone(),
        subject_output_schema: schema.clone(),
        expected_schema: Some(schema),
        subject_selection_policy: policy(510),
        evaluator_selection_policy: policy(512),
        child_budget: ChildBudgetLimit {
            maximum_duration_milliseconds: 30000,
            maximum_model_tokens: 1024,
            maximum_capability_calls: 4,
            maximum_artifact_bytes: 65536,
            maximum_descendant_runs: 1,
        },
        profile,
    };
    // Explicit synthetic exact references for pure authoring/protocol fixtures, not Registry authority.
    let evaluator_schema =
        insight_platform_agent_compiler::evaluation::evaluation_evaluator_input_schema(
            &request.subject_input_schema,
            &request.subject_output_schema,
            request.expected_schema.as_ref(),
        )
        .unwrap();
    request.deployment_features = [
        (
            &request.manifest.subject,
            &request.subject_input_schema,
            &request.subject_output_schema,
        ),
        (
            &request.manifest.evaluator,
            &evaluator_schema,
            &request.manifest.metric_schema,
        ),
    ]
    .into_iter()
    .map(
        |(target, input, output)| insight_platform_contracts::AgentDeploymentFeaturesV1 {
            schema_version: 1,
            deployment: target.clone(),
            interface_contract_digest:
                insight_platform_agent_compiler::agent_interface_contract_digest(input, output)
                    .unwrap(),
            required_features: Vec::new(),
        },
    )
    .collect();
    request
        .deployment_features
        .sort_by(|a, b| a.deployment.deployment_id.cmp(&b.deployment.deployment_id));
    let output = compile_evaluation_plan(request.clone()).unwrap();
    let AgentCompileResponseV1::Compiled { compilation: _ } =
        compile_source_bundle(output.source_bundle.clone())
    else {
        panic!("actual evaluation compile")
    };
    let report = EvaluationReportV1 {
        schema_version: 1,
        manifest: request.manifest_artifact.clone(),
        parent_run_id: id(ResourceKind::Run, 520),
        trials: manifest
            .trials()
            .unwrap()
            .into_iter()
            .map(|trial| EvaluationTrialResultV1 {
                trial,
                input: manifest.samples[0].input.clone(),
                expected: manifest.samples[0].expected.clone(),
                evidence: EvaluationTrialEvidenceV1::Missing {
                    reason: EvaluationMissingReason::NotStarted,
                },
            })
            .collect(),
        scored_trials: 0,
        failed_trials: 0,
        missing_trials: 2,
    };
    report.validate_for(&manifest).unwrap();
    for (name, value) in [
        ("sample.json", sample),
        ("manifest.json", manifest_value),
        ("request.json", serde_json::to_value(request).unwrap()),
        ("report-missing.json", serde_json::to_value(report).unwrap()),
        (
            "evaluator-input.schema.json",
            output.evaluator_input_schema.schema,
        ),
        (
            "parent-input.schema.json",
            output.parent_input_schema.schema,
        ),
    ] {
        outputs.insert(
            format!("examples/productization/evaluation/{name}"),
            canonical_json(&value).unwrap(),
        );
    }
    outputs
}
fn main() {
    let arguments = std::env::args().skip(1).collect::<Vec<_>>();
    assert!(
        arguments.is_empty() || arguments == ["--write"],
        "usage: product_authoring_examples [--write]"
    );
    let root = insight_platform_contract_tooling::machine::repository_root_from_manifest();
    for (name, bytes) in examples(&root) {
        let path = root.join(name);
        if arguments.is_empty() {
            assert_eq!(fs::read(path).unwrap(), bytes, "fixture drift");
        } else {
            fs::create_dir_all(path.parent().unwrap()).unwrap();
            fs::write(path, bytes).unwrap();
        }
    }
    println!("actual shared compiler framework/evaluation examples verified");
}
#[cfg(test)]
mod tests {
    use super::*;
    use insight_platform_agent_compiler::evaluation_schema::*;
    #[test]
    fn immutable_source_map_schema_checks_actual_compilation_and_closed_negatives() {
        let root = insight_platform_contract_tooling::machine::repository_root_from_manifest();
        let files = examples(&root);
        let corpus: Value = serde_json::from_slice(
            &fs::read(root.join("contracts/product-experience/agent-compiler/v2/corpus.json"))
                .unwrap(),
        )
        .unwrap();
        let profile: AgentCompilerProfile =
            serde_json::from_value(corpus["profile"].clone()).unwrap();
        let sources: BTreeMap<String, String> = files
            .iter()
            .filter_map(|(name, bytes)| {
                name.strip_prefix("examples/productization/compiled-framework/")
                    .map(|name| (name.to_owned(), String::from_utf8(bytes.clone()).unwrap()))
            })
            .collect();
        let manifest_path = sources
            .keys()
            .find(|name| name.ends_with("agent.json"))
            .unwrap()
            .clone();
        let bundle = AgentSourceBundleV1 {
            schema_version: 1,
            compiler_semantic_identity: compiler_semantic_identity(),
            compile_policy_inputs_digest: AgentSourceBundleV1::policy_digest(&profile),
            sources: AgentSourceFilesV1 {
                manifest_path,
                files: sources,
            },
            profile,
            bindings: ResolvedAgentBindings::default(),
        };
        let AgentCompileResponseV1::Compiled { compilation } = compile_source_bundle(bundle) else {
            panic!("valid actual source")
        };
        let actual: Value = serde_json::from_slice(&compilation.source_map_bytes).unwrap();
        let schema: Value = serde_json::from_slice(
            &fs::read(root.join("contracts/platform-v1/schemas/agent-source-map-v1.schema.json"))
                .unwrap(),
        )
        .unwrap();
        let validator = jsonschema::validator_for(&schema).unwrap();
        assert!(validator.is_valid(&actual));
        let mut invalid = actual.clone();
        invalid["entries"][0]["target"]["kind"] = json!("unknown");
        assert!(!validator.is_valid(&invalid));
        let mut invalid = actual.clone();
        invalid["entries"][0]["source"]["line"] = json!(0);
        assert!(!validator.is_valid(&invalid));
        let mut invalid = actual.clone();
        invalid["entries"][0]["source"]["unregistered"] = json!(true);
        assert!(!validator.is_valid(&invalid));
        let mut invalid = actual;
        invalid["schema_version"] = json!(2);
        assert!(!validator.is_valid(&invalid));
    }
    #[test]
    fn independent_evaluation_schema_positive_and_closed_negative_conformance() {
        let root = insight_platform_contract_tooling::machine::repository_root_from_manifest();
        let files = examples(&root);
        for (file, schema) in [
            ("manifest.json", evaluation_manifest_schema()),
            ("request.json", evaluation_plan_request_schema()),
            ("report-missing.json", evaluation_report_schema()),
        ] {
            let valid: Value = serde_json::from_slice(
                &files[&format!("examples/productization/evaluation/{file}")],
            )
            .unwrap();
            let validator = jsonschema::validator_for(&schema).unwrap();
            assert!(
                validator.is_valid(&valid),
                "{file}: {:?}",
                validator.iter_errors(&valid).collect::<Vec<_>>()
            );
            let mut wrong = valid.clone();
            wrong["extra_command_authority"] = json!(true);
            assert!(!validator.is_valid(&wrong));
            let mut wrong = valid.clone();
            wrong["schema_version"] = json!(2);
            assert!(!validator.is_valid(&wrong));
            let mut wrong = valid;
            wrong.as_object_mut().unwrap().remove("schema_version");
            assert!(!validator.is_valid(&wrong));
        }
        let schema = evaluation_report_schema();
        assert_eq!(
            schema["required"],
            json!([
                "schema_version",
                "manifest",
                "parent_run_id",
                "trials",
                "scored_trials",
                "failed_trials",
                "missing_trials"
            ])
        );
        let validator = jsonschema::validator_for(&schema).unwrap();
        let mut report: Value = serde_json::from_slice(
            &files["examples/productization/evaluation/report-missing.json"],
        )
        .unwrap();
        report["trials"][0]["evidence"] = json!({"kind":"failed","stage":"subject","failed_run_id":id(ResourceKind::Run,530),"subject_run_id":id(ResourceKind::Run,530),"terminal_state":"failed","terminal_version":3,"failure_value":null});
        assert!(validator.is_valid(&report));
        report["trials"][0]["evidence"]["terminal_state"] = json!("succeeded");
        assert!(!validator.is_valid(&report));
        report["trials"][0]["evidence"] = json!({"kind":"scored","output":null,"score":null});
        assert!(!validator.is_valid(&report));
        let mut manifest: Value =
            serde_json::from_slice(&files["examples/productization/evaluation/manifest.json"])
                .unwrap();
        let validator = jsonschema::validator_for(&evaluation_manifest_schema()).unwrap();
        manifest["samples"][0]["input"]["artifact_id"] = json!(id(ResourceKind::Run, 530));
        assert!(!validator.is_valid(&manifest));
    }
    #[test]
    fn evaluation_embeds_local_definitions_without_changing_instance_data() {
        let root = insight_platform_contract_tooling::machine::repository_root_from_manifest();
        let files = examples(&root);
        let mut request: EvaluationPlanRequestV1 =
            serde_json::from_slice(&files["examples/productization/evaluation/request.json"])
                .unwrap();
        let make_schema = |minimum: u64, maximum: u64| {
            ClosedJsonSchema::build(json!({
            "$schema":"https://json-schema.org/draft/2020-12/schema",
            "$id":format!("https://fixture.invalid/schema-{minimum}"),
            "type":"object", "additionalProperties":false,
            "properties":{
                "value":{"$ref":"#/$defs/Message"},
                "literal":{"type":"object","additionalProperties":false,
                    "properties":{"$ref":{"type":"string","minLength":1,"maxLength":64,"x-platform-max-bytes":64}},
                    "required":["$ref"], "const":{"$ref":"#/$defs/Message"}}
            },"required":["value","literal"],
            "$defs":{"Message":{"$id":format!("https://fixture.invalid/definition-{minimum}"),"type":"integer","minimum":minimum,"maximum":maximum}}
        })).unwrap()
        };
        let input = make_schema(0, 2);
        let output = make_schema(10, 12);
        let expected = make_schema(20, 22);
        let value = |number| json!({"value":number,"literal":{"$ref":"#/$defs/Message"}});
        input.validate_instance(&value(1)).unwrap();
        output.validate_instance(&value(11)).unwrap();
        expected.validate_instance(&value(21)).unwrap();
        request.subject_input_schema = input.clone();
        request.subject_output_schema = output;
        request.expected_schema = Some(expected.clone());
        request.manifest.samples[0].input_schema_digest = input.canonical_digest.clone();
        request.manifest.samples[0].expected_schema_digest = Some(expected.canonical_digest);
        request.manifest.samples[0].input = artifact(601, &value(1));
        request.manifest.samples[0].expected = Some(artifact(602, &value(21)));
        request.manifest_artifact =
            artifact(603, &serde_json::to_value(&request.manifest).unwrap());
        // This pure schema fixture changes both interfaces; its synthetic exact
        // feature evidence must describe these new schemas as well.
        let evaluator_schema = evaluation_evaluator_input_schema(
            &request.subject_input_schema,
            &request.subject_output_schema,
            request.expected_schema.as_ref(),
        )
        .unwrap();
        for evidence in &mut request.deployment_features {
            let (input, output) = if evidence.deployment == request.manifest.subject {
                (
                    &request.subject_input_schema,
                    &request.subject_output_schema,
                )
            } else {
                assert_eq!(evidence.deployment, request.manifest.evaluator);
                (&evaluator_schema, &request.manifest.metric_schema)
            };
            evidence.interface_contract_digest =
                agent_interface_contract_digest(input, output).unwrap();
        }
        let generated = compile_evaluation_plan(request.clone())
            .expect("valid local definitions can be embedded");
        let trial = request.manifest.trials().unwrap()[0].clone();
        let mut evaluator_input = json!({"trial_digest":trial.trial_digest,"sample_id":trial.sample_id,"repetition":trial.repetition,"input":value(1),"output":value(11),"expected":value(21)});
        generated
            .evaluator_input_schema
            .validate_instance(&evaluator_input)
            .unwrap();
        generated
            .parent_input_schema
            .validate_instance(&json!({"samples":{"one":{"input":value(1),"expected":value(21)}}}))
            .unwrap();
        assert!(
            generated.evaluator_input_schema.schema["$defs"]["subject_output"]
                .get("$id")
                .is_none()
        );
        assert!(
            generated.evaluator_input_schema.schema["$defs"]["subject_output__0"]
                .get("$id")
                .is_none()
        );
        assert_eq!(
            generated.evaluator_input_schema.schema["$defs"]["subject_output"]["properties"]
                ["literal"]["const"],
            json!({"$ref":"#/$defs/Message"})
        );
        evaluator_input["output"] = value(1);
        assert!(
            generated
                .evaluator_input_schema
                .validate_instance(&evaluator_input)
                .is_err(),
            "same named definitions retain different bounds"
        );
        let native = compile_source_bundle(generated.source_bundle.clone());
        assert!(matches!(native, AgentCompileResponseV1::Compiled { .. }));
        let wire: AgentCompileResponseV1 = serde_json::from_slice(&compile_request_bytes(
            &serde_json::to_vec(&generated.source_bundle).unwrap(),
        ))
        .unwrap();
        assert_eq!(native, wire, "browser wire uses the same pure compilation");
        let mut cyclic = input.schema;
        cyclic["$defs"]["Message"] = json!({"$ref":"#/$defs/Message"});
        assert!(
            ClosedJsonSchema::build(cyclic).is_err(),
            "embedding does not introduce recursion support"
        );
    }
    #[test]
    fn source_inspection_wire_schemas_match_actual_output_and_reject_unknown_fields() {
        let root = insight_platform_contract_tooling::machine::repository_root_from_manifest();
        let sources =
            AgentSourceFilesV1 {
                manifest_path: "agent.yaml".into(),
                files: BTreeMap::from([
                    (
                        "agent.yaml".into(),
                        fs::read_to_string(root.join(
                            "contracts/product-experience/agent-compiler/v2/deterministic.yaml",
                        ))
                        .unwrap(),
                    ),
                    (
                        "schema-message.json".into(),
                        fs::read_to_string(root.join(
                            "contracts/product-experience/agent-compiler/v2/schema-message.json",
                        ))
                        .unwrap(),
                    ),
                ]),
            };
        let request = AgentSourceInspectionRequestV1 {
            schema_version: 1,
            sources,
        };
        let actual = serde_json::to_value(&request).unwrap();
        let request_validator =
            jsonschema::validator_for(&agent_source_inspection_request_schema()).unwrap();
        assert!(request_validator.is_valid(&actual));
        for value in [
            json!({"schema_version":1,"sources":{"manifest_path":"a","files":{} }}),
            json!({"schema_version":1,"sources":{"manifest_path":"a","files":{"a":""},"unexpected":true}}),
            json!({"schema_version":2,"sources":{"manifest_path":"a","files":{"a":""}}}),
        ] {
            assert!(!request_validator.is_valid(&value));
        }
        let response_validator =
            jsonschema::validator_for(&agent_source_inspection_response_schema()).unwrap();
        let response: Value = serde_json::from_slice(&inspect_source_request_bytes(
            &serde_json::to_vec(&request).unwrap(),
        ))
        .unwrap();
        assert_eq!(response["outcome"], "inspected");
        assert!(response_validator.is_valid(&response));
        let rejected: Value =
            serde_json::from_slice(&inspect_source_request_bytes(b"{ invalid")).unwrap();
        assert_eq!(rejected["outcome"], "rejected");
        assert!(response_validator.is_valid(&rejected));
        let mut invalid = response;
        invalid["resolution"]["binding_authority"] = json!(true);
        assert!(!response_validator.is_valid(&invalid));
        let mut invalid = rejected;
        invalid["diagnostics"][0]["code"] = json!("unknown");
        assert!(!response_validator.is_valid(&invalid));
    }
}
