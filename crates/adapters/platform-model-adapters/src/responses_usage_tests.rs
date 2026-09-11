use super::*;

fn billing_entry() -> Value {
    serde_json::json!({
        "input_tokens": 50,
        "output_tokens": 10,
        "total_tokens": 60,
        "x_billing_type": "response_api",
        "prompt_tokens_details": {"cached_tokens": 7},
        "output_tokens_details": {"reasoning_tokens": 3}
    })
}

fn events_with_details(details: Value) -> Vec<ModelProviderWireEvent> {
    let mut events = openai_text_events("hello");
    events.last_mut().unwrap().data["response"]["usage"]["x_details"] = details;
    events
}

async fn assert_invalid_details(details: Value) {
    let (outcome, wire) = execute_wire_fixture(
        wire_fixture(OPENAI_RESPONSES_ADAPTER_NAME),
        events_with_details(details),
    )
    .await;
    let ModelAdapterExecutionOutcome::Failed(failure) = outcome else {
        panic!("invalid billing details accepted")
    };
    assert_eq!(failure.safe_code, "openai_responses_invalid_usage_details");
    assert!(failure.request_sent);
    assert!(!format!("{failure:?}").contains("canary"));
    assert_eq!(wire.protocol, ModelProviderWireProtocol::OpenAiResponses);
}

#[tokio::test]
async fn billing_details_preserve_aggregate_usage_and_full_terminal_evidence() {
    let (baseline, _) = execute_wire_fixture(
        wire_fixture(OPENAI_RESPONSES_ADAPTER_NAME),
        openai_text_events("hello"),
    )
    .await;
    let ModelAdapterExecutionOutcome::Succeeded(baseline) = baseline else {
        panic!("baseline")
    };
    for count in [1, 0, 2, 16] {
        let mut entry = billing_entry();
        // Metadata is not a second accounting authority, even when totals differ or cannot sum.
        entry["input_tokens"] = serde_json::json!(u64::MAX);
        entry["output_tokens"] = serde_json::json!(u64::MAX);
        entry["total_tokens"] = serde_json::json!(1);
        let events = events_with_details(serde_json::json!(vec![entry; count]));
        let expected_digest = canonical_digest(&events.last().unwrap().data["response"]).unwrap();
        let mut fixture = wire_fixture(OPENAI_RESPONSES_ADAPTER_NAME);
        fixture.request.profile.usage.provider_reports_usage = count != 1;
        let (outcome, _) = execute_wire_fixture(fixture, events).await;
        let ModelAdapterExecutionOutcome::Succeeded(success) = outcome else {
            panic!("documented billing details rejected for {count} entries")
        };
        assert_eq!(success.response.message, baseline.response.message);
        assert_eq!(
            success.response.structured_output,
            baseline.response.structured_output
        );
        assert_eq!(
            success.response.tool_intents,
            baseline.response.tool_intents
        );
        assert_eq!(success.response.usage, baseline.response.usage);
        assert_eq!(success.stream_evidence, baseline.stream_evidence);
        assert_eq!(success.response.usage.input_tokens, Some(50));
        assert_eq!(success.response.usage.output_tokens, Some(10));
        assert_eq!(
            success.response.usage.accounting_quality,
            AccountingQuality::ProviderReported
        );
        assert_eq!(
            success.response.observation.actual_model_identity,
            baseline.response.observation.actual_model_identity
        );
        assert_eq!(
            success
                .response
                .observation
                .provider_response_digest
                .as_ref()
                .unwrap()
                .as_str(),
            expected_digest
        );
        assert_ne!(
            success.response.observation.provider_response_digest,
            baseline.response.observation.provider_response_digest
        );
    }
    assert_invalid_details(serde_json::json!(vec![billing_entry(); 17])).await;
}

#[tokio::test]
async fn billing_detail_containers_and_every_required_field_are_closed() {
    assert_invalid_details(serde_json::json!([[]])).await;
    for invalid in [
        Value::Null,
        serde_json::json!({}),
        serde_json::json!(true),
        serde_json::json!(1),
        serde_json::json!("canary"),
    ] {
        assert_invalid_details(invalid.clone()).await;
        assert_invalid_details(serde_json::json!([invalid])).await;
    }
    for key in [
        "input_tokens",
        "output_tokens",
        "total_tokens",
        "x_billing_type",
    ] {
        let mut missing = billing_entry();
        missing.as_object_mut().unwrap().remove(key);
        assert_invalid_details(serde_json::json!([missing])).await;
        for invalid in [
            Value::Null,
            serde_json::json!(-1),
            serde_json::json!(0.5),
            serde_json::json!(true),
            serde_json::json!([]),
            serde_json::json!({}),
            serde_json::json!("canary"),
        ] {
            let mut entry = billing_entry();
            entry[key] = invalid;
            assert_invalid_details(serde_json::json!([entry])).await;
        }
    }
    for invalid in [
        serde_json::json!(0),
        serde_json::json!(""),
        serde_json::json!("response_api\0"),
        serde_json::json!("RESPONSE_API"),
    ] {
        let mut entry = billing_entry();
        entry["x_billing_type"] = invalid;
        assert_invalid_details(serde_json::json!([entry])).await;
    }
    for key in [
        "unknown_canary",
        "plugins",
        "image_tokens",
        "input_tokens_details",
        "cache_creation",
    ] {
        let mut entry = billing_entry();
        entry[key] = serde_json::json!({});
        assert_invalid_details(serde_json::json!([entry])).await;
    }
}

#[tokio::test]
async fn optional_detail_objects_are_typed_and_do_not_bypass_profile_gates() {
    for (key, counter) in [
        ("prompt_tokens_details", "cached_tokens"),
        ("output_tokens_details", "reasoning_tokens"),
    ] {
        for object in [
            None,
            Some(serde_json::json!({})),
            Some(serde_json::json!({counter: 0})),
            Some(serde_json::json!({counter: u64::MAX})),
        ] {
            let mut entry = billing_entry();
            if let Some(object) = object {
                entry[key] = object;
            } else {
                entry.as_object_mut().unwrap().remove(key);
            }
            let (outcome, _) = execute_wire_fixture(
                wire_fixture(OPENAI_RESPONSES_ADAPTER_NAME),
                events_with_details(serde_json::json!([entry])),
            )
            .await;
            let ModelAdapterExecutionOutcome::Succeeded(success) = outcome else {
                panic!("optional detail rejected")
            };
            assert_eq!(success.response.usage.cached_input_tokens, None);
            assert_eq!(success.response.usage.reasoning_tokens, None);
        }
        for invalid in [
            Value::Null,
            serde_json::json!([]),
            serde_json::json!(true),
            serde_json::json!(1),
            serde_json::json!("canary"),
        ] {
            let mut entry = billing_entry();
            entry[key] = invalid;
            assert_invalid_details(serde_json::json!([entry])).await;
        }
        for invalid in [
            Value::Null,
            serde_json::json!(-1),
            serde_json::json!(0.5),
            serde_json::json!(true),
            serde_json::json!([]),
            serde_json::json!({}),
            serde_json::json!("canary"),
        ] {
            let mut entry = billing_entry();
            entry[key][counter] = invalid;
            assert_invalid_details(serde_json::json!([entry])).await;
        }
        let mut entry = billing_entry();
        entry[key]["unknown_canary"] = serde_json::json!(0);
        assert_invalid_details(serde_json::json!([entry])).await;
    }
    let mut fixture = wire_fixture(OPENAI_RESPONSES_ADAPTER_NAME);
    fixture.request.profile.usage.reports_cached_input_tokens = true;
    fixture.request.profile.usage.reports_reasoning_tokens = true;
    let mut events = events_with_details(serde_json::json!([billing_entry()]));
    let usage = &mut events.last_mut().unwrap().data["response"]["usage"];
    usage["input_tokens_details"] = serde_json::json!({"cached_tokens": 2});
    usage["output_tokens_details"] = serde_json::json!({"reasoning_tokens": 1});
    let (outcome, _) = execute_wire_fixture(fixture.clone(), events.clone()).await;
    let ModelAdapterExecutionOutcome::Succeeded(success) = outcome else {
        panic!("aggregate details rejected")
    };
    assert_eq!(success.response.usage.cached_input_tokens, Some(2));
    assert_eq!(success.response.usage.reasoning_tokens, Some(1));
    for key in ["input_tokens_details", "output_tokens_details"] {
        let mut events = events.clone();
        events.last_mut().unwrap().data["response"]["usage"]
            .as_object_mut()
            .unwrap()
            .remove(key);
        assert!(!matches!(
            execute_wire_fixture_result(fixture.clone(), events).await.0,
            Ok(ModelAdapterExecutionOutcome::Succeeded(_))
        ));
    }
}

#[tokio::test]
async fn billing_details_never_replace_missing_or_invalid_aggregate_usage() {
    for key in ["input_tokens", "output_tokens"] {
        for invalid in [
            None,
            Some(Value::Null),
            Some(serde_json::json!(-1)),
            Some(serde_json::json!(0.5)),
            Some(serde_json::json!("50")),
            Some(serde_json::json!(true)),
        ] {
            let mut events = events_with_details(serde_json::json!([billing_entry()]));
            let usage = events.last_mut().unwrap().data["response"]["usage"]
                .as_object_mut()
                .unwrap();
            if let Some(invalid) = invalid {
                usage.insert(key.into(), invalid);
            } else {
                usage.remove(key);
            }
            let (outcome, _) =
                execute_wire_fixture(wire_fixture(OPENAI_RESPONSES_ADAPTER_NAME), events).await;
            let ModelAdapterExecutionOutcome::Failed(failure) = outcome else {
                panic!("detail substituted for aggregate")
            };
            assert_eq!(failure.safe_code, "openai_responses_invalid_usage");
        }
    }
    let mut events = events_with_details(serde_json::json!([billing_entry()]));
    events.last_mut().unwrap().data["response"]["usage"]["unknown_canary"] =
        serde_json::json!(true);
    let (outcome, _) =
        execute_wire_fixture(wire_fixture(OPENAI_RESPONSES_ADAPTER_NAME), events).await;
    let ModelAdapterExecutionOutcome::Failed(failure) = outcome else {
        panic!("unknown aggregate key accepted")
    };
    assert_eq!(failure.safe_code, "openai_responses_unknown_field");
}

#[tokio::test]
async fn brokered_sse_billing_details_preserve_output_and_cannot_manufacture_terminal() {
    let mut events = events_with_details(serde_json::json!([billing_entry(), billing_entry()]));
    events.insert(
        1,
        event(
            "response.reasoning_text.delta",
            serde_json::json!({"type":"response.reasoning_text.delta","delta":"hidden-canary"}),
        ),
    );
    let encoded = metadata_sse(&events, "\r\n");
    for chunk in [1, 7, 4096] {
        let broker = Arc::new(FixtureEgressBroker::raw(
            200,
            "text/event-stream",
            encoded.chunks(chunk).map(<[u8]>::to_vec).collect(),
        ));
        let outcome =
            execute_brokered_fixture(wire_fixture(OPENAI_RESPONSES_ADAPTER_NAME), broker.clone())
                .await;
        let ModelAdapterExecutionOutcome::Succeeded(success) = outcome else {
            panic!("brokered details rejected")
        };
        assert_eq!(success.response.usage.input_tokens, Some(50));
        assert_eq!(success.response.usage.output_tokens, Some(10));
        assert_eq!(success.stream_evidence.accepted_delta_count, 1);
        assert!(!format!("{success:?}").contains("canary"));
        assert_eq!(broker.calls.load(Ordering::SeqCst), 1);
    }
    events.pop();
    let broker = Arc::new(FixtureEgressBroker::from_events(events));
    assert!(!matches!(
        execute_brokered_fixture_result(
            wire_fixture(OPENAI_RESPONSES_ADAPTER_NAME),
            broker.clone()
        )
        .await,
        Ok(ModelAdapterExecutionOutcome::Succeeded(_))
    ));
    assert_eq!(broker.calls.load(Ordering::SeqCst), 1);

    // The raw budget still counts billing metadata that would otherwise be valid and discarded.
    let mut fixture = wire_fixture(OPENAI_RESPONSES_ADAPTER_NAME);
    fixture
        .request
        .provider
        .request_limits
        .maximum_response_bytes = 1024;
    fixture
        .request
        .provider
        .request_limits
        .maximum_stream_delta_bytes = 512;
    let events = events_with_details(serde_json::json!(vec![billing_entry(); 16]));
    let broker = Arc::new(FixtureEgressBroker::from_events(events));
    assert!(!matches!(
        execute_brokered_fixture_result(fixture, broker.clone()).await,
        Ok(ModelAdapterExecutionOutcome::Succeeded(_))
    ));
    assert_eq!(broker.calls.load(Ordering::SeqCst), 1);
}
