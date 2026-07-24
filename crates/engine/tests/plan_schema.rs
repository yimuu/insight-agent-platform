use std::collections::BTreeMap;

use insight_engine::{plan::PlanProperty, schema::compile_schema_2020, PlanType, RuntimeValue};
use serde_json::{json, Value};

fn assert_schema_equivalent(value_type: PlanType, samples: &[Value]) {
    let schema = value_type.json_schema_document().unwrap();
    let validator = compile_schema_2020(&schema).unwrap();
    for sample in samples {
        let runtime = RuntimeValue::new(sample.clone()).unwrap();
        assert_eq!(
            validator.is_valid(sample),
            runtime.matches(&value_type),
            "schema={schema}, sample={sample}"
        );
    }
}

#[test]
fn canonical_plan_types_publish_one_equivalent_json_schema_projection() {
    let samples = [
        json!(null),
        json!(false),
        json!(1),
        json!(1.5),
        json!(""),
        json!("alpha"),
        json!([]),
        json!(["a", "b"]),
        json!({}),
        json!({"name":"alpha"}),
        json!({"name":"alpha", "note":"ok"}),
        json!({"name":"alpha", "extra": 1}),
        json!({"fixed": true}),
    ];
    let object = PlanType::Object {
        properties: BTreeMap::from([
            (
                "name".to_owned(),
                PlanProperty::new(
                    PlanType::string(1, Some(8), Some("^[a-z]+$".to_owned()), None).unwrap(),
                    true,
                )
                .unwrap(),
            ),
            (
                "note".to_owned(),
                PlanProperty::new(PlanType::String, false).unwrap(),
            ),
        ]),
        additional_properties: None,
    }
    .normalized()
    .unwrap();
    let contracts = [
        PlanType::Never,
        PlanType::Any,
        PlanType::Null,
        PlanType::Boolean,
        PlanType::Integer,
        PlanType::Number,
        PlanType::String,
        PlanType::string(
            1,
            Some(5),
            Some("^[a-z]+$".to_owned()),
            Some(vec![json!("alpha"), json!("beta")]),
        )
        .unwrap(),
        PlanType::literal(json!({"fixed": true})).unwrap(),
        PlanType::array(PlanType::String, 1, Some(2)).unwrap(),
        object,
        PlanType::union([PlanType::Null, PlanType::String]).unwrap(),
    ];
    for contract in contracts {
        assert_schema_equivalent(contract, &samples);
    }
}

#[test]
fn root_schema_declares_the_frozen_2020_12_dialect() {
    let schema = PlanType::String.json_schema_document().unwrap();
    assert_eq!(
        schema["$schema"],
        json!("https://json-schema.org/draft/2020-12/schema")
    );
}
