//! Ordinary document-review authoring source. No installation or business identities are generated.
#[path = "../document_review_resources.rs"]
mod resources;
use insight_platform_agent_compiler::{
    agent_interface_contract_digest, inspect_agent_sources, AgentManifestInspectionResponseV1,
    AgentSourceFilesV1, AgentSourceInspectionRequestV1,
};
use insight_platform_contracts::*;
use insight_platform_plan::*;
use insight_platform_registry::authoring::*;
use serde_json::{json, Value};
use std::{collections::BTreeMap, fs, io::Write};
const MAX_RETRIEVED_ITEMS: u32 = 1;

fn object(properties: Value) -> Value {
    let required: Vec<_> = properties
        .as_object()
        .expect("properties")
        .keys()
        .cloned()
        .collect();
    json!({"type":"object","properties":properties,"required":required,"additionalProperties":false})
}
fn text(bytes: u32) -> Value {
    json!({"type":"string","minLength":0,"maxLength":bytes,"x-platform-max-bytes":bytes})
}
fn integer(maximum: u64) -> Value {
    json!({"type":"integer","minimum":0,"maximum":maximum})
}
fn nominal(name: &str) -> Value {
    json!({"$ref":nominal::pinned_nominal_reference(name).expect("nominal")})
}
fn nullable(value: Value) -> Value {
    json!({"oneOf":[value,{"type":"null"}]})
}
fn schema(mut value: Value) -> ClosedJsonSchema {
    value["$schema"] = json!("https://json-schema.org/draft/2020-12/schema");
    ClosedJsonSchema::build(value).expect("closed example schema")
}
fn key(name: &str) -> PlanNodeKey {
    PlanNodeKey::new(name.into()).expect("node")
}
fn port(name: &str, schema: &ClosedJsonSchema) -> ExactDataPortRef {
    ExactDataPortRef::NodeOutput {
        producer_node_id: key(name),
        port_id: DataPortKey::new("output".into()).expect("port"),
        schema_digest: schema.canonical_digest.clone(),
    }
}
fn requirement(slot: &str) -> Sha256Digest {
    canonical_digest(&json!({"sample":"insight.document-review","version":1,"slot":slot}))
        .expect("canonical")
        .parse()
        .expect("digest")
}
fn fields_schema() -> ClosedJsonSchema {
    schema(object(
        json!({"source_uri":text(2048),"source_revision":text(128),"raw_content_digest":nominal("Digest"),"start_line":integer(1_000_000),"end_line":integer(1_000_000)}),
    ))
}
fn observation_schema() -> ClosedJsonSchema {
    let digest = nominal("Digest");
    let id = nominal("UuidV7Id");
    let dataset =
        object(json!({"kind":{"const":"external_observation"},"source_identity_digest":digest}));
    let citation = object(
        json!({"context_deployment":object(json!({"deployment_id":id,"resource_kind":{"const":ResourceKind::ContextDeployment},"deployment_digest":digest})),"interface_revision":object(json!({"revision_id":id,"resource_kind":{"const":ResourceKind::ContextSourceInterfaceRevision},"semantic_digest":digest})),"dataset_view":dataset,"locator":object(json!({"kind":{"const":"remote_opaque"},"locator_digest":digest})),"strength":{"const":"observation_only"},"content_digest":digest,"observed_at":nominal("UtcTimestamp"),"display_label":text(4096)}),
    );
    let item = object(
        json!({"item_id":id,"source_item_identity_digest":digest,"content":object(json!({"kind":{"const":"inline"},"value":text(16_384)})),"structured_fields":object(json!({"schema_digest":digest,"canonical_digest":digest,"value":fields_schema().schema})),"score":nullable(object(json!({"millionths":{"type":"integer","minimum":-1_000_000,"maximum":1_000_000},"score_domain_digest":digest}))),"classification":{"const":"public"},"citation":citation,"authorization_evidence_digest":digest}),
    );
    schema(object(
        json!({"schema_version":{"const":1},"observation_id":id,"context_query_id":id,"dataset_view":dataset,"normalized_query_digest":digest,"items":{"type":"array","minItems":0,"maxItems":MAX_RETRIEVED_ITEMS,"items":item},"next_cursor_digest":{"type":"null"},"evidence":object(json!({"backend_request_digest":digest,"backend_response_digest":digest,"authorization_evidence_digest":digest,"ranking_evidence_digest":digest,"candidate_count":integer(100_000),"rejected_count":integer(100_000),"truncated":{"type":"boolean"}})),"observed_at":nominal("UtcTimestamp"),"total_bytes":integer(65_536)}),
    ))
}
fn assignment(
    node: &str,
    output: &ClosedJsonSchema,
    inputs: &[(&str, ExactDataPortRef)],
    next: &str,
) -> RuntimeNode {
    let ports: Vec<_> = inputs.iter().map(|(_, port)| port.clone()).collect();
    let mut instructions: Vec<_> = ports
        .iter()
        .map(|port| TypedInstruction::LoadPort { port: port.clone() })
        .collect();
    instructions.push(TypedInstruction::MakeObject {
        ordered_fields: inputs
            .iter()
            .map(|(name, _)| ExpressionFieldName::new((*name).into()).expect("field"))
            .collect(),
    });
    RuntimeNode::Compute {
        assignments: vec![PortAssignment {
            output_port: port(node, output),
            expression: TypedExpressionProgram::build(
                ports,
                instructions,
                output.canonical_digest.clone(),
                ExpressionLimits::ABSOLUTE,
            )
            .expect("expression"),
        }],
        next: key(next),
    }
}
fn input_schema() -> ClosedJsonSchema {
    schema(object(
        json!({"question":{"type":"string","minLength":1,"maxLength":1024,"x-platform-max-bytes":1024}}),
    ))
}
fn sources() -> AgentSourceFilesV1 {
    let input = input_schema();
    let observation = observation_schema();
    let draft = schema(object(
        json!({"answer":text(8192),"citations":{"type":"array","minItems":0,"maxItems":MAX_RETRIEVED_ITEMS,"items":fields_schema().schema}}),
    ));
    let review = schema(object(
        json!({"decision":{"type":"string","minLength":6,"maxLength":7,"x-platform-max-bytes":7,"enum":["approve","reject"]},"comment":text(2048)}),
    ));
    let model_input = schema(object(
        json!({"question":input.schema,"observation":observation.schema}),
    ));
    let output = schema(object(json!({"draft":draft.schema,"review":review.schema})));
    let run_input = ExactDataPortRef::RunInput {
        schema_digest: input.canonical_digest.clone(),
    };
    let rules = TaskEligibilityRule::AnyAuthorized;
    let plan = RuntimePlan {
        plan_version: 6,
        interface_contract_digest: agent_interface_contract_digest(&input, &output)
            .expect("interface"),
        entry_node_id: key("start"),
        dependency_slots: [
            ("documents", RuntimeDependencyKind::Context),
            ("model", RuntimeDependencyKind::Model),
        ]
        .into_iter()
        .map(|(name, kind)| {
            (
                name.into(),
                RuntimeDependencySlot {
                    kind,
                    requirement_digest: requirement(name),
                },
            )
        })
        .collect(),
        schema_documents: [&input, &observation, &draft, &review, &model_input, &output]
            .into_iter()
            .map(|schema| {
                (
                    schema.canonical_digest.clone(),
                    ClosedValueSchema::try_from(schema.clone()).expect("value schema"),
                )
            })
            .collect(),
        nodes: BTreeMap::from([
            (
                key("start"),
                RuntimeNode::Start {
                    next: key("retrieve"),
                },
            ),
            (
                key("retrieve"),
                RuntimeNode::ContextQuery {
                    context_slot_id: "documents".into(),
                    request: run_input.clone(),
                    result: port("retrieve", &observation),
                    maximum_items: MAX_RETRIEVED_ITEMS,
                    resume: key("assemble"),
                },
            ),
            (
                key("assemble"),
                assignment(
                    "assemble",
                    &model_input,
                    &[
                        ("question", run_input),
                        ("observation", port("retrieve", &observation)),
                    ],
                    "answer",
                ),
            ),
            (
                key("answer"),
                RuntimeNode::ModelLoop {
                    model_slot_id: "model".into(),
                    skill_slot_ids: vec![],
                    capability_slot_ids: vec![],
                    input: port("assemble", &model_input),
                    model_route: None,
                    output: port("answer", &draft),
                    maximum_rounds: 1,
                    maximum_capability_calls: 0,
                    maximum_parallel_calls_per_round: 0,
                    token_budget: 10_240,
                    resume: key("review"),
                },
            ),
            (
                key("review"),
                RuntimeNode::HumanTask {
                    definition: HumanTaskDefinition::HumanWork {
                        eligibility_rule: Some(rules.clone()),
                        eligible_principal_rule_digest: rules.canonical_digest().expect("rule"),
                        safe_prompt_key: "review_document_answer_and_citations".into(),
                    },
                    response: port("review", &review),
                    timeout_milliseconds: 1_800_000,
                    resume: key("assemble_result"),
                },
            ),
            (
                key("assemble_result"),
                assignment(
                    "assemble_result",
                    &output,
                    &[
                        ("draft", port("answer", &draft)),
                        ("review", port("review", &review)),
                    ],
                    "finish",
                ),
            ),
            (
                key("finish"),
                RuntimeNode::Return {
                    value: port("assemble_result", &output),
                },
            ),
        ]),
    };
    plan.validate(PlanLimits::from_profile(&checked_in_hard_limit_profile()).expect("limits"))
        .expect("Plan");
    let manifest = json!({"apiVersion":"insight.platform/v1","kind":"Agent","metadata":{"name":"document-review","displayName":"Document answer with human review"},"spec":{"execution":{"kind":"full_plan","plan":"plan.json"},"instructions":fs::read_to_string(insight_platform_contract_tooling::machine::repository_root_from_manifest().join("examples/productization/document-review/instructions.txt")).expect("sample instructions"),"input":{"schema":"input.schema.json","classification":"internal"},"output":{"schema":"output.schema.json"},"limits":{"deadlineSeconds":3600},"publish":{"environment":"development"}}});
    let files = [
        ("agent.json", manifest),
        ("plan.json", serde_json::to_value(plan).expect("Plan JSON")),
        ("input.schema.json", input.schema),
        ("output.schema.json", output.schema),
    ]
    .into_iter()
    .map(|(name, value)| {
        (
            name.into(),
            String::from_utf8(canonical_json(&value).expect("canonical")).expect("UTF-8"),
        )
    })
    .collect();
    let sources = AgentSourceFilesV1 {
        manifest_path: "agent.json".into(),
        files,
    };
    assert!(
        matches!(
            inspect_agent_sources(AgentSourceInspectionRequestV1 {
                schema_version: 1,
                sources: sources.clone()
            }),
            AgentManifestInspectionResponseV1::Inspected { .. }
        ),
        "source preflight"
    );
    sources
}
fn bind(
    targets: BTreeMap<String, AuthoringSlotTargetV1>,
) -> Result<ResolveAgentBindingsRequestV1, &'static str> {
    if targets.len() != 2
        || !matches!(targets.get("documents"),Some(AuthoringSlotTargetV1::Context{consistency:ContextConsistencyPolicy::ExternalObservation,allowed_projection,..}) if allowed_projection.is_empty())
        || !matches!(
            targets.get("model"),
            Some(AuthoringSlotTargetV1::Model { .. })
        )
    {
        return Err("expected documents Context and model selectors");
    }
    let request = ResolveAgentBindingsRequestV1 {
        schema_version: 1,
        slots: targets
            .into_iter()
            .map(|(slot_id, target)| AuthoringSlotSelectionV1 {
                requirement_digest: requirement(&slot_id),
                slot_id,
                interface_contract_digest: None,
                target,
            })
            .collect(),
    };
    request.validate().map_err(|_| "invalid public selectors")?;
    Ok(request)
}
fn read_public_json(path: &str) -> Result<Value, Box<dyn std::error::Error>> {
    use std::io::Read;
    let metadata = fs::symlink_metadata(path)?;
    if !metadata.is_file() || metadata.len() > 262_144 {
        return Err("bounded regular public JSON file required".into());
    }
    let mut bytes = Vec::new();
    fs::File::open(path)?
        .take(262_145)
        .read_to_end(&mut bytes)?;
    Ok(parse_strict_json(
        &bytes,
        JsonLimits {
            max_bytes: 262_144,
            max_depth: 32,
            max_properties_per_object: 64,
            max_items_per_array: 256,
            max_string_bytes: 16_384,
        },
    )?)
}
fn write_new_json(path: &str, value: &Value) -> Result<(), Box<dyn std::error::Error>> {
    let bytes = canonical_json(value)?;
    let mut options = fs::OpenOptions::new();
    options.write(true).create_new(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(0o600);
    }
    let mut file = options.open(path)?;
    file.write_all(&bytes)?;
    file.sync_all()?;
    fs::File::open(
        std::path::Path::new(path)
            .parent()
            .filter(|p| !p.as_os_str().is_empty())
            .unwrap_or(std::path::Path::new(".")),
    )?
    .sync_all()?;
    Ok(())
}
fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args: Vec<_> = std::env::args().skip(1).collect();
    let root = insight_platform_contract_tooling::machine::repository_root_from_manifest();
    match args.as_slice() {
        [action] if action == "--check" || action == "--write" => {
            for (name, contents) in sources().files {
                let path = root
                    .join("examples/productization/document-review/agent")
                    .join(name);
                if action == "--check" {
                    if fs::read(&path)? != contents.as_bytes() {
                        return Err("generated source differs".into());
                    }
                } else {
                    fs::create_dir_all(path.parent().ok_or("source directory")?)?;
                    fs::write(path, contents)?;
                }
            }
        }
        [action, input, output] if action == "--bindings" => {
            let value = read_public_json(input)?;
            write_new_json(output, &serde_json::to_value(bind(serde_json::from_value(value)?)?)?)?;
        }
        [action, input, output] if action == "--resource-source" => {
            let value = read_public_json(input)?;
            write_new_json(
                output,
                &serde_json::to_value(resources::source(serde_json::from_value(value)?)?)?,
            )?;
        }
        [action, source, artifact, output] if action == "--resource-publication" => {
            let value = read_public_json(source)?;
            if fs::read(source)? != canonical_json(&value)? {
                return Err("source bytes must be canonical before upload".into());
            }
            let manifest = resources::publication(
                serde_json::from_value(value)?,
                serde_json::from_value(read_public_json(artifact)?)?,
            )?;
            write_new_json(output, &manifest)?;
        }
        [action, input, manifest, environment, output] if action == "--context-deployment" => {
            let value = resources::deployment(
                read_public_json(input)?, serde_json::from_value(read_public_json(manifest)?)?, environment,
            )?;
            write_new_json(output, &value)?;
        }
        [action, endpoint, region, public_root, output] if action == "--destination" => {
            use std::io::Read;
            let metadata = fs::symlink_metadata(public_root)?;
            if !metadata.is_file() || metadata.len() > MAX_REMOTE_CONTEXT_INSTALLATION_TRUST_BYTES as u64 {
                return Err("bounded regular public root file required".into());
            }
            let mut bytes = Vec::new();
            fs::File::open(public_root)?.take(MAX_REMOTE_CONTEXT_INSTALLATION_TRUST_BYTES as u64 + 1).read_to_end(&mut bytes)?;
            let value = resources::destination(serde_json::from_value(read_public_json(endpoint)?)?, region.parse()?, String::from_utf8(bytes)?)?;
            write_new_json(output, &serde_json::to_value(value)?)?;
        }
        _ => {
            return Err(
                "use --check, --write, --bindings SELECTORS OUTPUT, --resource-source INPUT OUTPUT, --resource-publication SOURCE ARTIFACT OUTPUT, --context-deployment CLOSURE WORKER_MANIFEST ENVIRONMENT OUTPUT, or --destination ENDPOINT REGION PUBLIC_ROOT OUTPUT".into(),
            )
        }
    }
    Ok(())
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn actual_corpus_item_fits_the_typed_observation_and_exports_only_a_capacity_fixture() {
        use std::process::{Command, Stdio};
        let query = json!({"question":"平台如何处理持久状态和人工确认？"});
        let hash =
            |value: &Value| -> Sha256Digest { canonical_digest(value).unwrap().parse().unwrap() };
        let request = json!({"schema_version":1,"query":query,"normalized_query_digest":hash(&query),"normalized_filter_digest":hash(&json!({"schema_version":1,"filter":null})),"requested_projection":[],"page_size":MAX_RETRIEVED_ITEMS,"cursor_digest":null});
        let root = insight_platform_contract_tooling::machine::repository_root_from_manifest();
        let mut child = Command::new("python3")
            .arg(root.join("examples/productization/document-review/server.py"))
            .arg("--query-stdin")
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::null())
            .spawn()
            .unwrap();
        child
            .stdin
            .take()
            .unwrap()
            .write_all(&canonical_json(&request).unwrap())
            .unwrap();
        let result = child.wait_with_output().unwrap();
        assert!(result.status.success());
        let wire: Value = parse_strict_json(
            &result.stdout,
            JsonLimits {
                max_bytes: 65_536,
                max_depth: 16,
                max_properties_per_object: 32,
                max_items_per_array: 16,
                max_string_bytes: 16_384,
            },
        )
        .unwrap();
        assert_eq!(wire["items"].as_array().unwrap().len(), 1);
        let item = &wire["items"][0];
        let id = |kind: ResourceKind, n: u16| -> ResourceId {
            format!(
                "{}_018f3e20-0000-7000-8000-{n:012x}",
                kind.descriptor().prefix
            )
            .parse()
            .unwrap()
        };
        // IDs and authorization digests below model envelope size only. They are never live conformance.
        let evidence = hash(&json!({"scope":"synthetic_capacity_metadata_only"}));
        let dataset = json!({"kind":"external_observation","source_identity_digest":evidence});
        let observed_at = "2026-09-10T00:00:00.000000Z";
        let fields = ClosedJsonValue::build(
            fields_schema().canonical_digest,
            item["structured_fields"].clone(),
        )
        .unwrap();
        let content_bytes = canonical_json(&item["content"]).unwrap().len();
        let observation: insight_platform_context::ContextObservation = serde_json::from_value(json!({
            "schema_version":1,"observation_id":id(ResourceKind::ContextObservation,1),"context_query_id":id(ResourceKind::ContextQuery,2),
            "dataset_view":dataset,"normalized_query_digest":hash(&query),"items":[{
                "item_id":id(ResourceKind::ContextItem,3),"source_item_identity_digest":hash(&item["source_identity"]),
                "content":{"kind":"inline","value":item["content"]},"structured_fields":fields,
                "score":{"millionths":item["score_millionths"],"score_domain_digest":evidence},"classification":item["classification"],
                "citation":{"context_deployment":ExactDeploymentRef::new(id(ResourceKind::ContextDeployment,4),evidence.clone()).unwrap(),
                    "interface_revision":ExactVersionRef::new(id(ResourceKind::ContextSourceInterfaceRevision,5),evidence.clone()).unwrap(),
                    "dataset_view":dataset,"locator":{"kind":"remote_opaque","locator_digest":hash(&item["locator"])},
                    "strength":"observation_only","content_digest":hash(&item["content"]),"observed_at":observed_at,"display_label":item["display_label"]},
                "authorization_evidence_digest":evidence}],
            "next_cursor_digest":null,"evidence":{"backend_request_digest":hash(&query),"backend_response_digest":hash(&wire),"authorization_evidence_digest":evidence,"ranking_evidence_digest":evidence,"candidate_count":1,"rejected_count":0,"truncated":false},
            "observed_at":observed_at,"total_bytes":content_bytes,"canonical_digest":evidence
        })).unwrap();
        let mut unsigned = serde_json::to_value(observation).unwrap();
        unsigned.as_object_mut().unwrap().remove("canonical_digest");
        observation_schema().validate_instance(&unsigned).unwrap();
        let source = sources();
        let plan: RuntimePlan = serde_json::from_str(&source.files["plan.json"]).unwrap();
        let RuntimeNode::Compute { assignments, .. } = &plan.nodes[&key("assemble")] else {
            panic!("Compute")
        };
        let values = assignments[0]
            .expression
            .input_ports
            .iter()
            .cloned()
            .zip([query.clone(), unsigned])
            .map(|(port, value)| {
                (
                    port.clone(),
                    ClosedJsonValue::build(port.schema_digest().clone(), value).unwrap(),
                )
            })
            .collect();
        let input = assignments[0]
            .expression
            .evaluate(&values, ExpressionLimits::ABSOLUTE)
            .unwrap();
        plan.schema_documents[assignments[0].output_port.schema_digest()]
            .validate_instance(&input.value)
            .unwrap();
        let RuntimeNode::ModelLoop { output, .. } = &plan.nodes[&key("answer")] else {
            panic!("ModelLoop")
        };
        let fixture = json!({"schema_version":1,"scope":"local_corpus_typed_observation_capacity_only","live_authorization_qualified":false,
            "query":query,"wire_response_digest":hash(&wire),"excerpt_utf8_bytes":item["content"].as_str().unwrap().len(),
            "model_input":input.value,"model_input_bytes":canonical_json(&input.value).unwrap().len(),
            "plan_node":plan.nodes[&key("answer")],"plan_node_key":"answer","model_output_schema":plan.schema_documents[output.schema_digest()],
            "agent_output_schema":serde_json::from_str::<Value>(&source.files["output.schema.json"]).unwrap(),
            "author_instructions":serde_json::from_str::<Value>(&source.files["agent.json"]).unwrap()["spec"]["instructions"],
            "platform_instruction":insight_platform_registry::model_configuration::BASIC_MODEL_PLATFORM_INSTRUCTION});
        if let Some(path) = std::env::var_os("PLATFORM_DOCUMENT_REVIEW_CAPACITY_FIXTURE") {
            write_new_json(path.to_str().unwrap(), &fixture).unwrap();
        }
        assert!(
            fixture["model_input_bytes"].as_u64().unwrap() < 8192,
            "this measures input bytes, not full prompt tokens"
        );
    }
    #[test]
    fn full_source_is_preflighted_and_keeps_question_evidence_and_explicit_human_decision() {
        let source = sources();
        let directory = insight_platform_contract_tooling::machine::repository_root_from_manifest()
            .join("examples/productization/document-review/agent");
        for (name, bytes) in &source.files {
            assert_eq!(
                fs::read(directory.join(name)).unwrap(),
                bytes.as_bytes(),
                "checked-in authoring source must match its compiler-validated constructor"
            );
        }
        let plan: RuntimePlan = serde_json::from_str(&source.files["plan.json"]).unwrap();
        let RuntimeNode::Compute { assignments, .. } = &plan.nodes[&key("assemble")] else {
            panic!("assemble")
        };
        let expression = &assignments[0].expression;
        let values = expression
            .input_ports
            .iter()
            .cloned()
            .zip([
                json!({"question":"Who owns durable state?"}),
                json!({"items":[]}),
            ])
            .map(|(port, value)| {
                (
                    port.clone(),
                    ClosedJsonValue::build(port.schema_digest().clone(), value).unwrap(),
                )
            })
            .collect();
        assert_eq!(
            expression
                .evaluate(&values, ExpressionLimits::ABSOLUTE)
                .unwrap()
                .value,
            json!({"question":{"question":"Who owns durable state?"},"observation":{"items":[]}})
        );
        assert!(matches!(
            &plan.nodes[&key("review")],
            RuntimeNode::HumanTask {
                definition: HumanTaskDefinition::HumanWork { .. },
                ..
            }
        ));
        let RuntimeNode::ModelLoop {
            maximum_capability_calls,
            maximum_parallel_calls_per_round,
            ..
        } = &plan.nodes[&key("answer")]
        else {
            panic!("answer")
        };
        assert_eq!(
            (*maximum_capability_calls, *maximum_parallel_calls_per_round),
            (0, 0)
        );
        assert!(bind(BTreeMap::new()).is_err());
    }
    #[test]
    fn complete_source_uses_shared_compiler_and_requires_real_dependency_evidence() {
        use insight_platform_agent_compiler::{
            compile_agent, AgentCompilerInput, AgentCompilerProfile, RequiredAgentFeature,
            ResolvedAgentBindings,
        };
        // Synthetic identities are confined to this pure compiler test, never generated source.
        let id = |kind: ResourceKind, n: u16| -> ResourceId {
            format!(
                "{}_018f3e20-0000-7000-8000-{n:012x}",
                kind.descriptor().prefix
            )
            .parse()
            .unwrap()
        };
        let exact = |kind, n| {
            ExactDeploymentRef::new(id(kind, n), requirement(&format!("fixture-{n}"))).unwrap()
        };
        let revision = |n| {
            ExactVersionRef::new(
                id(ResourceKind::PolicyRevision, n),
                requirement(&format!("fixture-{n}")),
            )
            .unwrap()
        };
        let policy = |n| ExactPolicyBinding {
            deployment: exact(ResourceKind::PolicyDeployment, n),
            revision: revision(n + 1),
        };
        let context = exact(ResourceKind::ContextDeployment, 10);
        let binding = AgentSlotTargetInputV1::Context {
            binding: Box::new(ContextBindingInputV1 {
                context_deployment: context.clone(),
                consistency: ContextConsistencyPolicy::ExternalObservation,
                allowed_projection: vec![],
                authorization_policy: revision(20),
                ranking_policy: revision(21),
            }),
        };
        let model = AgentSlotTargetInputV1::Model {
            candidates: vec![exact(ResourceKind::ModelDeployment, 30)],
            selection_policy: policy(40),
        };
        let mut bindings = ResolvedAgentBindings {
            model: None,
            slots: vec![
                AgentSlotBindingInputV1 {
                    slot_id: "documents".into(),
                    requirement_digest: requirement("documents"),
                    target: binding,
                },
                AgentSlotBindingInputV1 {
                    slot_id: "model".into(),
                    requirement_digest: requirement("model"),
                    target: model,
                },
            ],
            deployment_features: vec![AgentDeploymentFeaturesV1 {
                schema_version: 1,
                deployment: context,
                interface_contract_digest: requirement("fixture-context-interface"),
                required_features: vec![RequiredAgentFeature::Context],
            }],
        };
        let source = sources();
        let profile: AgentCompilerProfile = serde_json::from_value(
            serde_json::from_str::<Value>(
                &fs::read_to_string(
                    insight_platform_contract_tooling::machine::repository_root_from_manifest()
                        .join("contracts/product-experience/agent-compiler/v2/corpus.json"),
                )
                .expect("compiler fixture"),
            )
            .unwrap()["profile"]
                .clone(),
        )
        .unwrap();
        let compile = |bindings| {
            compile_agent(AgentCompilerInput {
                manifest_bytes: source.files["agent.json"].as_bytes().to_vec(),
                input_schema_bytes: source.files["input.schema.json"].as_bytes().to_vec(),
                output_schema_bytes: source.files["output.schema.json"].as_bytes().to_vec(),
                plan_bytes: Some(source.files["plan.json"].as_bytes().to_vec()),
                profile: profile.clone(),
                bindings,
            })
        };
        let compiled = compile(bindings.clone()).expect("complete sample must compile");
        let actual: RuntimePlan = serde_json::from_slice(&compiled.typed_plan_bytes).unwrap();
        let authored: RuntimePlan = serde_json::from_str(&source.files["plan.json"]).unwrap();
        assert_eq!(actual.nodes, authored.nodes);
        assert_eq!(actual.dependency_slots, authored.dependency_slots);
        for (digest, schema) in authored.schema_documents {
            assert_eq!(actual.schema_documents.get(&digest), Some(&schema));
        }
        bindings.deployment_features.clear();
        assert!(
            compile(bindings).is_err(),
            "source cannot fabricate missing deployment proof"
        );
    }
}
