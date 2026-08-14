use insight_platform_contracts::{
    canonical_digest,
    id::ResourceKind,
    parse_strict_json,
    registry::{
        require_cursor_purpose, validate_public_event_envelope, CursorPurpose, DataClassification,
        Effect, EventDurability, Permission, PublicRunEventType,
    },
    schema::validate_closed_schema,
    state::{
        all_state_machines, attempt_commit_disposition, AttemptCommitDisposition,
        AttemptObservationState,
    },
    ApiProblem, ArtifactRef, DecimalMoney, DurablePublicRunEventData, Failure, JsonLimits,
    OpaqueListCursor, OpaqueRunEventCursor, ResourceId, Sha256Digest, UtcTimestamp, ValueRef,
};
use serde_json::Value;
use std::{
    fs,
    path::{Path, PathBuf},
    str::FromStr,
};

fn repository_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .and_then(Path::parent)
        .unwrap()
        .to_owned()
}

fn fixture(name: &str) -> Value {
    let bytes = fs::read(
        repository_root()
            .join("contracts/platform-v1/fixtures")
            .join(name),
    )
    .unwrap();
    parse_strict_json(&bytes, JsonLimits::CONTRACT_FIXTURE).unwrap()
}

fn cases(name: &str) -> Vec<Value> {
    fixture(name)["cases"].as_array().unwrap().clone()
}

fn assert_matches_nominal_schema(relative: &str, value: &Value) {
    let bytes = fs::read(
        repository_root()
            .join("contracts/platform-v1/schemas/nominal")
            .join(relative),
    )
    .unwrap();
    let schema = parse_strict_json(&bytes, JsonLimits::CONTRACT_FIXTURE).unwrap();
    let validator = jsonschema::options()
        .with_draft(jsonschema::Draft::Draft202012)
        .build(&schema)
        .unwrap();
    assert!(validator.is_valid(value), "{relative} rejected {value}");
}

#[test]
fn shared_foundation_scalar_examples_round_trip_through_nominal_types() {
    let bytes =
        fs::read(repository_root().join("contracts/platform-v1/examples/foundation-scalars.json"))
            .unwrap();
    let examples = parse_strict_json(&bytes, JsonLimits::CONTRACT_FIXTURE).unwrap();

    examples["resource_id"]
        .as_str()
        .unwrap()
        .parse::<ResourceId>()
        .unwrap();
    examples["digest"]
        .as_str()
        .unwrap()
        .parse::<Sha256Digest>()
        .unwrap();
    examples["timestamp"]
        .as_str()
        .unwrap()
        .parse::<UtcTimestamp>()
        .unwrap();
    serde_json::from_value::<DecimalMoney>(examples["money"].clone())
        .unwrap()
        .validate()
        .unwrap();
    serde_json::from_value::<ArtifactRef>(examples["artifact_ref"].clone())
        .unwrap()
        .validate()
        .unwrap();
    serde_json::from_value::<ValueRef>(examples["value_ref"].clone())
        .unwrap()
        .validate(JsonLimits::CONTRACT_FIXTURE)
        .unwrap();
    serde_json::from_value::<OpaqueListCursor>(examples["list_cursor"].clone()).unwrap();
    serde_json::from_value::<OpaqueRunEventCursor>(examples["run_event_cursor"].clone()).unwrap();
    serde_json::from_value::<Failure>(examples["failure"].clone())
        .unwrap()
        .validate(16_384)
        .unwrap();
    serde_json::from_value::<ApiProblem>(examples["api_problem"].clone())
        .unwrap()
        .validate(16_384, 256)
        .unwrap();

    for (file, field) in [
        ("digest.schema.json", "digest"),
        ("utc-timestamp.schema.json", "timestamp"),
        ("decimal-money.schema.json", "money"),
        ("artifact-ref.schema.json", "artifact_ref"),
        ("opaque-list-cursor.schema.json", "list_cursor"),
        ("opaque-run-event-cursor.schema.json", "run_event_cursor"),
        ("failure.schema.json", "failure"),
        ("api-problem.schema.json", "api_problem"),
    ] {
        assert_matches_nominal_schema(file, &examples[field]);
    }
}

fn case<'a>(cases: &'a [Value], id: &str) -> &'a Value {
    cases.iter().find(|case| case["fixture_id"] == id).unwrap()
}

#[test]
fn every_fixture_and_suite_digest_is_canonical_and_current() {
    let manifest = fixture("manifest.json");
    for suite in manifest["suites"].as_array().unwrap() {
        let document = fixture(suite["file"].as_str().unwrap());
        assert_eq!(canonical_digest(&document).unwrap(), suite["digest"]);
        for case in document["cases"].as_array().unwrap() {
            let input = case
                .get("input")
                .or_else(|| case.get("input_artifact"))
                .unwrap();
            assert_eq!(
                canonical_digest(input).unwrap(),
                case["input_digest"],
                "{}",
                case["fixture_id"]
            );
        }
    }
}

#[test]
fn f_id_executes_the_nominal_kind_contract() {
    let fixtures = cases("f-id.json");
    let valid: ResourceId = case(&fixtures, "fid-valid-agent-v7")["input"]["value"]
        .as_str()
        .unwrap()
        .parse()
        .unwrap();
    assert_eq!(valid.kind(), ResourceKind::Agent);

    assert!(case(&fixtures, "fid-unknown-prefix")["input"]["value"]
        .as_str()
        .unwrap()
        .parse::<ResourceId>()
        .is_err());
    assert!(ResourceId::parse_expected(
        case(&fixtures, "fid-wrong-field-kind")["input"]["value"]
            .as_str()
            .unwrap(),
        ResourceKind::AgentDeployment,
    )
    .is_err());
    assert!(case(&fixtures, "fid-uuid-not-v7")["input"]["value"]
        .as_str()
        .unwrap()
        .parse::<ResourceId>()
        .is_err());
}

#[test]
fn f_canon_rejects_duplicate_keys_before_value_construction() {
    let fixtures = cases("f-canon.json");
    let duplicate = case(&fixtures, "fcanon-duplicate-key")["input_artifact"]["utf8"]
        .as_str()
        .unwrap();
    assert!(parse_strict_json(duplicate.as_bytes(), JsonLimits::CONTRACT_FIXTURE).is_err());

    let positive = case(&fixtures, "fcanon-object-key-order");
    assert_eq!(
        canonical_digest(&positive["input"]).unwrap(),
        positive["expected"]["digest"]
    );

    assert!(parse_strict_json(&[0xff], JsonLimits::CONTRACT_FIXTURE).is_err());
    for fixture_id in [
        "fcanon-integer-outside-interoperable-range",
        "fcanon-number-not-finite",
    ] {
        let input = case(&fixtures, fixture_id)["input_artifact"]["utf8"]
            .as_str()
            .unwrap();
        assert!(parse_strict_json(input.as_bytes(), JsonLimits::CONTRACT_FIXTURE).is_err());
    }
}

#[test]
fn f_schema_executes_positive_and_negative_profile_cases() {
    let fixtures = cases("f-schema.json");
    validate_closed_schema(&case(&fixtures, "fschema-closed-bounded-object")["input"]).unwrap();
    assert_eq!(
        validate_closed_schema(&case(&fixtures, "fschema-open-object")["input"])
            .unwrap_err()
            .code,
        "schema_object_open"
    );
    assert_eq!(
        validate_closed_schema(&case(&fixtures, "fschema-unknown-keyword")["input"])
            .unwrap_err()
            .code,
        "schema_unknown_keyword"
    );
}

#[test]
fn f_state_and_f_fence_execute_closed_transition_rules() {
    let fixtures = cases("f-state.json");
    let allowed = &case(&fixtures, "fstate-run-wait-resume")["input"];
    let machine = all_state_machines()
        .into_iter()
        .find(|machine| machine.name == allowed["machine"])
        .unwrap();
    assert!(machine.transitions.iter().any(|edge| {
        edge[0] == allowed["from"].as_str().unwrap() && edge[1] == allowed["to"].as_str().unwrap()
    }));
    let forbidden = &case(&fixtures, "fstate-run-terminal-cannot-leave")["input"];
    assert!(!machine.transitions.iter().any(|edge| {
        edge[0] == forbidden["from"].as_str().unwrap()
            && edge[1] == forbidden["to"].as_str().unwrap()
    }));
    assert!(AttemptObservationState::from_str("rejected_stale_fence").is_err());

    let fence = cases("f-fence.json");
    for fixture in fence {
        let input = &fixture["input"];
        let disposition = attempt_commit_disposition(
            input["stored_epoch"].as_u64().unwrap(),
            input["attempt_epoch"].as_u64().unwrap(),
            input["terminal_already_committed"].as_bool().unwrap(),
        );
        assert_eq!(
            disposition.as_str(),
            fixture["expected"]["commit_disposition"]
        );
    }
    assert_eq!(
        attempt_commit_disposition(7, 7, true),
        AttemptCommitDisposition::RejectedTerminalWinner
    );
}

#[test]
fn f_event_enforces_durability_identity_shape() {
    let fixtures = cases("f-event.json");
    for fixture in fixtures {
        let input = &fixture["input"];
        if fixture["fixture_id"] == "fevent-list-cursor-cannot-resume-run" {
            assert!(require_cursor_purpose(
                input["actual_purpose"]
                    .as_str()
                    .unwrap()
                    .parse::<CursorPurpose>()
                    .unwrap(),
                input["required_purpose"]
                    .as_str()
                    .unwrap()
                    .parse::<CursorPurpose>()
                    .unwrap(),
            )
            .is_err());
            continue;
        }
        let event_type = input["event_type"]
            .as_str()
            .unwrap()
            .parse::<PublicRunEventType>()
            .unwrap();
        let durability = input["durability"]
            .as_str()
            .unwrap()
            .parse::<EventDurability>()
            .unwrap();
        let envelope_result = validate_public_event_envelope(
            event_type,
            durability,
            input["event_id"] == "present",
            input["sequence"] == "present",
            input["cursor"] == "present",
        );
        let payload_result = if durability == EventDurability::Durable {
            input.get("data").ok_or(()).and_then(|data| {
                DurablePublicRunEventData::from_value(event_type, data).map_err(|_| ())
            })
        } else {
            Ok(DurablePublicRunEventData {
                source_kind: insight_platform_contracts::PublicRunEventSourceKind::Run,
                source_id: "run_018f0000-0000-7000-8000-000000000001".parse().unwrap(),
                source_projection_version: 1,
                safe_summary: None,
            })
        };
        assert_eq!(
            envelope_result.is_ok() && payload_result.is_ok(),
            fixture["polarity"] == "positive"
        );
    }
}

#[test]
fn f_policy_enforces_risk_classification_and_permission_closure() {
    let fixtures = cases("f-policy.json");
    let effects = &case(&fixtures, "fpolicy-effect-max-risk")["input"];
    let interface = effects["interface_effect"]
        .as_str()
        .unwrap()
        .parse::<Effect>()
        .unwrap();
    let implementation = effects["implementation_effect"]
        .as_str()
        .unwrap()
        .parse::<Effect>()
        .unwrap();
    let effective = if interface.risk_rank() >= implementation.risk_rank() {
        interface
    } else {
        implementation
    };
    assert_eq!(effective.as_str(), "non_idempotent_write");

    let classification = case(&fixtures, "fpolicy-classification-join")["input"]["classifications"]
        .as_array()
        .unwrap()
        .iter()
        .map(|value| {
            value
                .as_str()
                .unwrap()
                .parse::<DataClassification>()
                .unwrap()
        })
        .fold(DataClassification::Public, DataClassification::join);
    assert_eq!(classification, DataClassification::Restricted);
    assert!("secret.read".parse::<Permission>().is_err());
}
