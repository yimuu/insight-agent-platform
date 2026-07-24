use insight_engine::{RunId, RuntimeValue, SchedulerFacts};
use serde_json::json;

#[test]
fn scheduler_fact_wire_rejects_unknown_fields() {
    let facts = SchedulerFacts::new(
        RunId::new("run_wire_closed").unwrap(),
        0,
        RuntimeValue::new(json!({})).unwrap(),
    );
    let mut encoded = serde_json::to_value(facts).unwrap();
    encoded["unknown_future_fact"] = json!(true);
    assert!(serde_json::from_value::<SchedulerFacts>(encoded).is_err());
}
