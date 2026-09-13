use insight_platform_contracts::*;
use insight_platform_registry::model_configuration::*;
use serde_json::{json, Value};
use std::{fs, path::Path};

// Resolve only the selected boundary's reachable references from checked-in files. No network
// retriever, copied nominal schemas or replacement validation rules participate in the assertion.
fn inline(value: &Value, root: &Value, directory: &Path, depth: usize) -> Value {
    assert!(depth < 64);
    if let Some(reference) = value.get("$ref").and_then(Value::as_str) {
        let (file, fragment) = reference.split_once('#').unwrap_or((reference, ""));
        if file.is_empty() {
            return inline(root.pointer(fragment).unwrap(), root, directory, depth + 1);
        }
        let path = directory.join(file);
        let external: Value = serde_json::from_slice(&fs::read(&path).unwrap()).unwrap();
        return inline(
            external.pointer(fragment).unwrap(),
            &external,
            path.parent().unwrap(),
            depth + 1,
        );
    }
    match value {
        Value::Array(values) => Value::Array(
            values
                .iter()
                .map(|v| inline(v, root, directory, depth + 1))
                .collect(),
        ),
        Value::Object(values) => Value::Object(
            values
                .iter()
                .filter(|(k, _)| k.as_str() != "$id")
                .map(|(k, v)| (k.clone(), inline(v, root, directory, depth + 1)))
                .collect(),
        ),
        value => value.clone(),
    }
}
fn validator(name: &str) -> jsonschema::Validator {
    let directory = insight_platform_contract_tooling::machine::repository_root_from_manifest()
        .join("contracts/platform-v1/schemas");
    let root: Value = serde_json::from_slice(
        &fs::read(directory.join("model-configuration.schema.json")).unwrap(),
    )
    .unwrap();
    jsonschema::options()
        .with_draft(jsonschema::Draft::Draft202012)
        .should_validate_formats(true)
        .build(&inline(&root["$defs"][name], &root, &directory, 0))
        .unwrap()
}
fn id(kind: ResourceKind, n: u16) -> ResourceId {
    format!(
        "{}_0198f1cc-32e4-75e1-a9e8-{n:012x}",
        kind.descriptor().prefix
    )
    .parse()
    .unwrap()
}
fn digest() -> Sha256Digest {
    format!("sha256:{}", "a".repeat(64)).parse().unwrap()
}

#[test]
fn authoring_model_references_match_the_compiler_and_machine_schema() {
    let directory = insight_platform_contract_tooling::machine::repository_root_from_manifest()
        .join("contracts/platform-v1/schemas");
    let root: Value = serde_json::from_slice(
        &fs::read(directory.join("agent-authoring-profile-v1.schema.json")).unwrap(),
    )
    .unwrap();
    let validator = jsonschema::options()
        .with_draft(jsonschema::Draft::Draft202012)
        .build(&inline(
            &root["$defs"]["ModelBinding"],
            &root,
            &directory,
            0,
        ))
        .unwrap();
    let policy = ExactPolicyBinding {
        revision: ExactVersionRef::new(id(ResourceKind::PolicyRevision, 901), digest()).unwrap(),
        deployment: ExactDeploymentRef::new(id(ResourceKind::PolicyDeployment, 902), digest())
            .unwrap(),
    };
    let cases = [
        ("project/default".to_owned(), true),
        ("project/a".to_owned(), true),
        (format!("project/{}", "a".repeat(63)), true),
        ("default".to_owned(), false),
        ("project/".to_owned(), false),
        ("project/a.b".to_owned(), false),
        ("project/a_b".to_owned(), false),
        ("project/A".to_owned(), false),
        ("project/a/b".to_owned(), false),
        ("project/中文".to_owned(), false),
        (format!("project/{}", "a".repeat(64)), false),
        (id(ResourceKind::ModelDeployment, 903).to_string(), false),
    ];
    for (reference, accepted) in cases {
        let binding = json!({"alias":reference,"deployment":ExactDeploymentRef::new(id(ResourceKind::ModelDeployment, 904), digest()).unwrap(),"selection_policy":policy});
        assert_eq!(
            validator.is_valid(&serde_json::to_value(&binding).unwrap()),
            accepted,
            "{reference}"
        );
        if accepted {
            let manifest = json!({"apiVersion":"insight.platform/v1","kind":"Agent","metadata":{"name":"reference-conformance"},"spec":{"execution":{"kind":"model_chat"},"instructions":"Answer the input.","model":{"ref":reference},"input":{"schema":"input.json","classification":"internal"},"output":{"schema":"output.json"}}});
            let inspected = insight_platform_agent_compiler::inspect_manifest(
                &serde_json::to_vec(&manifest).unwrap(),
            )
            .unwrap();
            assert_eq!(inspected.model_ref.as_deref(), Some(reference.as_str()));
        }
    }
}

#[test]
fn model_configuration_machine_schema_accepts_real_typed_inputs_and_rejects_boundary_mutations() {
    let validator = validator("ModelConfigurationInputV1");
    let credential = ExactSecretBindingRef::build(
        id(ResourceKind::SecretBinding, 1),
        1,
        id(ResourceKind::SecretProvider, 2),
        MODEL_API_KEY_PURPOSE.parse().unwrap(),
        SecretResolutionPolicy::Pinned {
            opaque_version_identity_digest: digest(),
        },
    )
    .unwrap();
    let input = ModelConfigurationInputV1::Source(ModelSourceConfigurationV2 {
        schema_version: 2,
        alias: "work.qwen".parse().unwrap(),
        display_name: "Work account".into(),
        endpoint: insight_platform_contracts::normalize_model_base_url(
            "https://api.example.com/v1",
        )
        .unwrap(),
        protocol: ModelProviderWireProtocol::OpenAiResponses,
        region: "global".parse().unwrap(),
        credential,
    });
    let source = serde_json::to_value(input).unwrap();
    assert!(validator.is_valid(&source));
    for (pointer, value) in [
        ("/configuration/alias", json!("7wrong")),
        ("/configuration/credential/binding_generation", json!(0)),
        (
            "/configuration/credential/provider_id",
            json!(id(ResourceKind::SecretBinding, 2)),
        ),
        ("/kind", json!("provider")),
    ] {
        let mut bad = source.clone();
        *bad.pointer_mut(pointer).unwrap() = value;
        assert!(!validator.is_valid(&bad), "schema accepted {pointer}");
        if let Ok(ModelConfigurationInputV1::Source(source)) =
            serde_json::from_value::<ModelConfigurationInputV1>(bad)
        {
            assert!(source.credential.validate().is_err());
        }
    }
    let input = ModelConfigurationInputV1::Model(BasicModelConfigurationV1 {
        schema_version: 1,
        alias: "work.chat".parse().unwrap(),
        display_name: "Chat".into(),
        source: ExactDeploymentRef::new(id(ResourceKind::ModelProviderDeployment, 3), digest())
            .unwrap(),
        model: "example-model".into(),
        maximum_input_tokens: 8192,
        maximum_output_tokens: 2048,
        declared_at: chrono::Utc::now(),
    });
    let model = serde_json::to_value(input).unwrap();
    assert!(validator.is_valid(&model));
    for (pointer, value) in [
        ("/configuration/maximum_input_tokens", json!(8193)),
        ("/configuration/maximum_output_tokens", json!(0)),
        ("/configuration/model", json!("m".repeat(256))),
        (
            "/configuration/source/deployment_id",
            json!(id(ResourceKind::ModelDeployment, 3)),
        ),
    ] {
        let mut bad = model.clone();
        *bad.pointer_mut(pointer).unwrap() = value;
        assert!(!validator.is_valid(&bad), "schema accepted {pointer}");
    }
    let mut bad = model.clone();
    bad["configuration"]["api_key"] = json!("not-a-supported-field");
    assert!(!validator.is_valid(&bad));
    assert!(serde_json::from_value::<ModelConfigurationInputV1>(bad).is_err());
    let request = validator_request(&model);
    assert!(validator_request_schema().is_valid(&request));
    let mut unknown = request;
    unknown["endpoint"] = json!("https://uninstalled.example");
    assert!(!validator_request_schema().is_valid(&unknown));
}
fn validator_request(model: &Value) -> Value {
    json!({"schema_version":1,"installation_digest":digest(),"input":model})
}
fn validator_request_schema() -> jsonschema::Validator {
    validator("DeclareModelConfigurationRequestV1")
}

#[test]
fn quota_machine_contract_is_closed_and_matches_typed_nominal_limits() {
    let target = ExactDeploymentRef::new(id(ResourceKind::ModelDeployment, 20), digest()).unwrap();
    let limits = ModelQuotaLimitsV1 {
        requests: 20,
        tokens: 204800,
        cost_microunits: 20000000,
    };
    let request = SetModelQuotaRequestV1 {
        schema_version: 1,
        model_deployment: target.clone(),
        limits,
    };
    request.validate().unwrap();
    let wire = serde_json::to_value(&request).unwrap();
    let check = validator("SetModelQuotaRequestV1");
    assert!(check.is_valid(&wire));
    for (pointer, value) in [
        ("/limits/requests", json!(-1)),
        ("/limits/tokens", json!(MAX_MODEL_QUOTA_VALUE + 1)),
        (
            "/model_deployment/deployment_id",
            json!(id(ResourceKind::ModelProfile, 20)),
        ),
        ("/model_deployment/resource_kind", json!("model_profile")),
    ] {
        let mut bad = wire.clone();
        *bad.pointer_mut(pointer).unwrap() = value;
        assert!(!check.is_valid(&bad), "accepted {pointer}");
        assert!(serde_json::from_value::<SetModelQuotaRequestV1>(bad)
            .map_or(true, |value| value.validate().is_err()));
    }
    let mut bad = wire;
    bad["limits"]["unlimited"] = json!(true);
    assert!(!check.is_valid(&bad));
    let view = ModelQuotaViewV1 {
        schema_version: 1,
        tenant_id: id(ResourceKind::Tenant, 1),
        model_deployment: target,
        allocation: None,
        tenant_concurrency: ModelQuotaCounterV1 {
            limit: 8,
            reserved: 0,
            used: 0,
        },
        etag: format!("\"model-quota-{}\"", "a".repeat(64)),
    };
    view.validate().unwrap();
    let wire = serde_json::to_value(view).unwrap();
    let check = validator("ModelQuotaViewV1");
    assert!(check.is_valid(&wire));
    let mut missing = wire.clone();
    missing.as_object_mut().unwrap().remove("allocation");
    assert!(!check.is_valid(&missing));
    assert!(serde_json::from_value::<ModelQuotaViewV1>(missing).is_err());
    let mut weak = wire.clone();
    weak["etag"] = json!(format!("W/{}", wire["etag"].as_str().unwrap()));
    assert!(!check.is_valid(&weak));
}
