//! Structural JSON Schema projections for evaluation file/Artifact boundaries.
//! Exact digests, relationships and byte budgets remain checked by the owning
//! evaluation validator; JSON Schema does not confer execution or read authority.
use crate::evaluation::*;
use insight_platform_contracts::{nominal_schemas, ResourceKind};
use serde_json::{json, Map, Value};

fn object(fields: &[(&str, Value)], required: &[&str]) -> Value {
    json!({"type":"object","additionalProperties":false,"properties":fields.iter().map(|(key,value)|((*key).to_owned(),value.clone())).collect::<Map<_,_>>(),"required":required})
}
fn reference(name: &str) -> Value {
    json!({"$ref":format!("#/$defs/{name}")})
}
fn nullable(value: Value) -> Value {
    json!({"oneOf":[value,{"type":"null"}]})
}
fn integer(min: u64, max: u64) -> Value {
    json!({"type":"integer","minimum":min,"maximum":max})
}
fn id(kind: ResourceKind) -> Value {
    json!({"type":"string","pattern":format!("^{}_[0-9a-f]{{8}}-[0-9a-f]{{4}}-7[0-9a-f]{{3}}-[89ab][0-9a-f]{{3}}-[0-9a-f]{{12}}$",kind.descriptor().prefix)})
}
fn exact(kind: ResourceKind) -> Value {
    let (id_key, digest_key) = if kind.is_revision() {
        ("revision_id", "semantic_digest")
    } else {
        ("deployment_id", "deployment_digest")
    };
    object(
        &[
            (id_key, id(kind)),
            ("resource_kind", json!({"const":kind.descriptor().name})),
            (digest_key, reference("Digest")),
        ],
        &[id_key, "resource_kind", digest_key],
    )
}
fn definitions() -> Map<String, Value> {
    let mut defs = Map::new();
    for name in ["ArtifactRef", "Digest"] {
        let mut schema = nominal_schemas()
            .remove(name)
            .expect("owning nominal exists");
        // Embed the exact nominal projection, retaining its local definitions.
        schema
            .as_object_mut()
            .expect("nominal object")
            .remove("$id");
        defs.insert(name.into(), schema);
    }
    defs.insert(
        "StableId".into(),
        json!({"type":"string","minLength":1,"maxLength":128,"pattern":"^[A-Za-z0-9_.-]+$"}),
    );
    defs.insert("RunId".into(), id(ResourceKind::Run));
    defs.insert(
        "AgentDeployment".into(),
        exact(ResourceKind::AgentDeployment),
    );
    defs.insert("DeploymentFeatures".into(),object(&[
        ("schema_version",json!({"const":1})),("deployment",reference("AgentDeployment")),("interface_contract_digest",reference("Digest")),
        ("required_features",json!({"type":"array","maxItems":insight_platform_contracts::MAX_AGENT_REQUIRED_FEATURES,"uniqueItems":true,"items":{"enum":insight_platform_contracts::AgentRequiredFeature::ALL.iter().map(|value|value.as_str()).collect::<Vec<_>>()}}))
    ], &["schema_version","deployment","interface_contract_digest","required_features"]));
    defs.insert("PolicyRevision".into(), exact(ResourceKind::PolicyRevision));
    defs.insert(
        "PolicyDeployment".into(),
        exact(ResourceKind::PolicyDeployment),
    );
    defs.insert(
        "PolicyBinding".into(),
        object(
            &[
                ("deployment", reference("PolicyDeployment")),
                ("revision", reference("PolicyRevision")),
            ],
            &["deployment", "revision"],
        ),
    );
    defs.insert("ClosedSchema".into(),object(&[
        ("schema_version",json!({"const":insight_platform_contracts::CLOSED_SCHEMA_DOCUMENT_VERSION})),
        ("profile",json!({"const":insight_platform_contracts::CLOSED_SCHEMA_PROFILE_ID})),
        ("schema",json!({"type":"object","description":"Validated by the owning closed object schema profile, including local/pinned references and its byte bound."})),
        ("canonical_digest",reference("Digest"))], &["schema_version","profile","schema","canonical_digest"]));
    defs.insert(
        "Sample".into(),
        object(
            &[
                ("sample_id", reference("StableId")),
                ("input", reference("ArtifactRef")),
                ("expected", nullable(reference("ArtifactRef"))),
                ("input_schema_digest", reference("Digest")),
                ("expected_schema_digest", nullable(reference("Digest"))),
            ],
            &["sample_id", "input", "input_schema_digest"],
        ),
    );
    defs.insert("Manifest".into(),object(&[("schema_version",json!({"const":EVALUATION_MANIFEST_VERSION})),("dataset_id",reference("StableId")),("samples",json!({"type":"array","minItems":1,"maxItems":MAX_EVALUATION_SAMPLES,"items":reference("Sample")})),("repetitions",integer(1,u64::from(MAX_EVALUATION_REPETITIONS))),("subject",reference("AgentDeployment")),("evaluator",reference("AgentDeployment")),("metric_schema",reference("ClosedSchema"))], &["schema_version","dataset_id","samples","repetitions","subject","evaluator","metric_schema"]));
    defs.insert(
        "Trial".into(),
        object(
            &[
                ("manifest_digest", reference("Digest")),
                ("sample_id", reference("StableId")),
                (
                    "repetition",
                    integer(0, u64::from(MAX_EVALUATION_REPETITIONS - 1)),
                ),
                ("trial_digest", reference("Digest")),
            ],
            &["manifest_digest", "sample_id", "repetition", "trial_digest"],
        ),
    );
    defs.insert(
        "RunValue".into(),
        object(
            &[
                ("run_id", reference("RunId")),
                ("value_id", id(ResourceKind::RunValue)),
                ("schema_digest", reference("Digest")),
                ("content_digest", reference("Digest")),
                ("artifact", nullable(reference("ArtifactRef"))),
            ],
            &["run_id", "value_id", "schema_digest", "content_digest"],
        ),
    );
    let scored = object(
        &[
            ("kind", json!({"const":"scored"})),
            ("subject_input", reference("RunValue")),
            ("evaluator_input", reference("RunValue")),
            ("output", reference("RunValue")),
            ("score", reference("RunValue")),
        ],
        &[
            "kind",
            "subject_input",
            "evaluator_input",
            "output",
            "score",
        ],
    );
    let failed = object(
        &[
            ("kind", json!({"const":"failed"})),
            ("stage", json!({"enum":["subject","evaluator"]})),
            ("failed_run_id", reference("RunId")),
            ("subject_run_id", nullable(reference("RunId"))),
            (
                "terminal_state",
                json!({"enum":["failed","timed_out","cancelled"]}),
            ),
            (
                "terminal_version",
                integer(1, insight_platform_contracts::MAX_SAFE_JSON_INTEGER),
            ),
            ("failure_value", nullable(reference("RunValue"))),
        ],
        &[
            "kind",
            "stage",
            "failed_run_id",
            "terminal_state",
            "terminal_version",
        ],
    );
    let missing = object(
        &[
            ("kind", json!({"const":"missing"})),
            (
                "reason",
                json!({"enum":["not_started","cancelled","deadline_exceeded","evidence_unavailable"]}),
            ),
        ],
        &["kind", "reason"],
    );
    defs.insert("Evidence".into(), json!({"oneOf":[scored,failed,missing]}));
    defs.insert(
        "Result".into(),
        object(
            &[
                ("trial", reference("Trial")),
                ("input", reference("ArtifactRef")),
                ("expected", nullable(reference("ArtifactRef"))),
                ("evidence", reference("Evidence")),
            ],
            &["trial", "input", "evidence"],
        ),
    );
    let positive = integer(1, insight_platform_contracts::MAX_SAFE_JSON_INTEGER);
    defs.insert(
        "ChildBudget".into(),
        object(
            &[
                ("maximum_duration_milliseconds", positive.clone()),
                ("maximum_model_tokens", positive.clone()),
                ("maximum_capability_calls", integer(1, u64::from(u32::MAX))),
                ("maximum_artifact_bytes", positive.clone()),
                ("maximum_descendant_runs", integer(1, u64::from(u32::MAX))),
            ],
            &[
                "maximum_duration_milliseconds",
                "maximum_model_tokens",
                "maximum_capability_calls",
                "maximum_artifact_bytes",
                "maximum_descendant_runs",
            ],
        ),
    );
    defs.insert(
        "CompilerProfile".into(),
        object(
            &[
                ("default_deadline_seconds", integer(1, u64::from(crate::MAX_AGENT_DEADLINE_SECONDS))),
                (
                    "default_environment",
                    json!({"type":"string","minLength":1,"maxLength":63,"pattern":"^[a-z][a-z0-9-]*$"}),
                ),
                (
                    "policy_versions",
                    json!({"type":"array","items":reference("PolicyRevision")}),
                ),
                (
                    "deployment_policies",
                    json!({"type":"array","items":reference("PolicyBinding")}),
                ),
                ("execution_profile", reference("PolicyBinding")),
                (
                    "model_loop",
                    object(
                        &[
                            ("maximum_rounds", integer(1, u64::from(u16::MAX))),
                            ("maximum_capability_calls", integer(1, u64::from(u32::MAX))),
                            (
                                "maximum_parallel_calls_per_round",
                                integer(1, u64::from(u16::MAX)),
                            ),
                            ("token_budget", positive),
                        ],
                        &[
                            "maximum_rounds",
                            "maximum_capability_calls",
                            "maximum_parallel_calls_per_round",
                            "token_budget",
                        ],
                    ),
                ),
            ],
            &[
                "default_deadline_seconds",
                "default_environment",
                "policy_versions",
                "deployment_policies",
                "execution_profile",
                "model_loop",
            ],
        ),
    );
    defs
}
fn document(name: &str, root: Value) -> Value {
    let mut value = root;
    let object = value.as_object_mut().expect("schema object");
    object.insert(
        "$schema".into(),
        json!("https://json-schema.org/draft/2020-12/schema"),
    );
    object.insert(
        "$id".into(),
        json!(format!("urn:insight:platform:v1:{name}")),
    );
    object.insert("$defs".into(), json!(definitions()));
    object.insert(
        "x-insight-max-canonical-bytes".into(),
        json!(MAX_EVALUATION_MANIFEST_BYTES),
    );
    object.insert("x-insight-semantic-validator".into(),json!("insight-platform-agent-compiler::evaluation; schemas check structure, owner verifies digests, unique samples/trials, exact bindings, schema profile, byte limits and report counts"));
    value
}
pub fn evaluation_manifest_schema() -> Value {
    document("evaluation-manifest-v1", reference("Manifest"))
}
pub fn evaluation_report_schema() -> Value {
    document(
        "evaluation-report-v1",
        object(
            &[
                (
                    "schema_version",
                    json!({"const":EVALUATION_MANIFEST_VERSION}),
                ),
                ("manifest", reference("ArtifactRef")),
                ("parent_run_id", reference("RunId")),
                (
                    "trials",
                    json!({"type":"array","minItems":1,"maxItems":MAX_EVALUATION_TRIALS,"items":reference("Result")}),
                ),
                ("scored_trials", integer(0, MAX_EVALUATION_TRIALS as u64)),
                ("failed_trials", integer(0, MAX_EVALUATION_TRIALS as u64)),
                ("missing_trials", integer(0, MAX_EVALUATION_TRIALS as u64)),
            ],
            &[
                "schema_version",
                "manifest",
                "parent_run_id",
                "trials",
                "scored_trials",
                "failed_trials",
                "missing_trials",
            ],
        ),
    )
}
pub fn evaluation_plan_request_schema() -> Value {
    document(
        "evaluation-plan-request-v1",
        object(
            &[
                (
                    "schema_version",
                    json!({"const":EVALUATION_MANIFEST_VERSION}),
                ),
                (
                    "name",
                    json!({"type":"string","minLength":1,"maxLength":63,"pattern":"^[a-z][a-z0-9-]*$"}),
                ),
                (
                    "display_name",
                    json!({"type":"string","minLength":1,"maxLength":crate::MAX_AGENT_DISPLAY_NAME_CHARS}),
                ),
                ("manifest", reference("Manifest")),
                ("manifest_artifact", reference("ArtifactRef")),
                ("subject_input_schema", reference("ClosedSchema")),
                ("subject_output_schema", reference("ClosedSchema")),
                ("expected_schema", nullable(reference("ClosedSchema"))),
                ("subject_selection_policy", reference("PolicyBinding")),
                ("evaluator_selection_policy", reference("PolicyBinding")),
                (
                    "deployment_features",
                    json!({"type":"array","minItems":1,"maxItems":2,"items":reference("DeploymentFeatures")}),
                ),
                ("child_budget", reference("ChildBudget")),
                ("profile", reference("CompilerProfile")),
            ],
            &[
                "schema_version",
                "name",
                "display_name",
                "manifest",
                "manifest_artifact",
                "subject_input_schema",
                "subject_output_schema",
                "subject_selection_policy",
                "evaluator_selection_policy",
                "deployment_features",
                "child_budget",
                "profile",
            ],
        ),
    )
}
