use super::*;

#[tokio::test]
async fn empty_text_fragments_preserve_only_real_output_and_usage() {
    for structured in [false, true] {
        let mut fixture = wire_fixture(OPENAI_RESPONSES_ADAPTER_NAME);
        let text = if structured {
            enable_structured_output(&mut fixture);
            r#"{"answer":"hello"}"#
        } else {
            "hello"
        };
        let baseline = openai_text_events(text);
        let (expected, _) = execute_wire_fixture(fixture.clone(), baseline.clone()).await;
        let ModelAdapterExecutionOutcome::Succeeded(expected) = expected else {
            panic!("baseline")
        };
        let mut events = baseline;
        let mut empty = events[1].clone();
        empty.data["delta"] = serde_json::json!("");
        events.insert(1, empty.clone());
        events.insert(3, empty);
        events.last_mut().unwrap().data["response"]["output"][0]["content"]
            .as_array_mut()
            .unwrap()
            .insert(0, serde_json::json!({"type":"output_text","text":""}));
        let (actual, _) = execute_wire_fixture(fixture, events).await;
        let ModelAdapterExecutionOutcome::Succeeded(actual) = actual else {
            panic!("empty fragments rejected")
        };
        assert_eq!(actual.response.message, expected.response.message);
        assert_eq!(
            actual.response.structured_output,
            expected.response.structured_output
        );
        assert_eq!(actual.response.usage, expected.response.usage);
        assert_eq!(actual.stream_evidence.accepted_delta_count, 1);
        assert_ne!(
            actual.response.observation.provider_response_digest,
            expected.response.observation.provider_response_digest
        );
    }
}

#[tokio::test]
async fn empty_tool_fragments_still_require_known_projection_and_final_arguments() {
    let mut fixture = wire_fixture(OPENAI_RESPONSES_ADAPTER_NAME);
    enable_tool(&mut fixture);
    let baseline = openai_tool_events();
    let mut events = baseline.clone();
    let mut empty = events[1].clone();
    empty.data["delta"] = serde_json::json!("");
    events.insert(1, empty.clone());
    let (outcome, _) = execute_wire_fixture(fixture.clone(), events).await;
    let ModelAdapterExecutionOutcome::Succeeded(success) = outcome else {
        panic!("empty arguments delta rejected")
    };
    assert_eq!(success.stream_evidence.accepted_delta_count, 1);
    assert_eq!(
        success.response.tool_intents[0].arguments.value,
        serde_json::json!({"q":"hello"})
    );
    for case in [
        "unknown_item",
        "empty_item",
        "empty_name",
        "empty_call",
        "empty_final",
        "wrong_schema",
    ] {
        let mut events = baseline.clone();
        match case {
            "unknown_item" => {
                empty.data["item_id"] = serde_json::json!("unknown");
                events.insert(1, empty.clone());
            }
            "empty_item" => events[1].data["item_id"] = serde_json::json!(""),
            "empty_name" => events[0].data["item"]["name"] = serde_json::json!(""),
            "empty_call" => events[0].data["item"]["call_id"] = serde_json::json!(""),
            "empty_final" => {
                events[2].data["response"]["output"][0]["arguments"] = serde_json::json!("")
            }
            "wrong_schema" => {
                events[2].data["response"]["output"][0]["arguments"] =
                    serde_json::json!(r#"{"q":12}"#)
            }
            _ => unreachable!(),
        }
        assert!(
            !matches!(
                execute_wire_fixture_result(fixture.clone(), events).await.0,
                Ok(ModelAdapterExecutionOutcome::Succeeded(_))
            ),
            "{case}"
        );
    }
}

#[tokio::test]
async fn invalid_content_identity_and_fingerprint_have_closed_categories() {
    for (field, code) in [
        ("delta", "openai_responses_invalid_text"),
        ("text", "openai_responses_invalid_text"),
        ("model", "openai_responses_invalid_identity"),
        ("fingerprint", "openai_responses_invalid_fingerprint"),
    ] {
        for invalid in [
            None,
            Some(Value::Null),
            Some(serde_json::json!(5)),
            Some(serde_json::json!(true)),
            Some(serde_json::json!([])),
            Some(serde_json::json!({})),
            Some(serde_json::json!("bad\0canary")),
        ] {
            if field == "fingerprint" && matches!(invalid, None | Some(Value::Null)) {
                continue;
            }
            let mut events = openai_text_events("hello");
            let (object, key) = match field {
                "delta" => (&mut events[1].data, "delta"),
                "text" => (
                    &mut events[2].data["response"]["output"][0]["content"][0],
                    "text",
                ),
                "model" => (&mut events[2].data["response"], "model"),
                "fingerprint" => (&mut events[2].data["response"], "system_fingerprint"),
                _ => unreachable!(),
            };
            let object = object.as_object_mut().unwrap();
            if let Some(invalid) = invalid {
                object.insert(key.into(), invalid);
            } else {
                object.remove(key);
            }
            let (outcome, wire) =
                execute_wire_fixture(wire_fixture(OPENAI_RESPONSES_ADAPTER_NAME), events).await;
            let ModelAdapterExecutionOutcome::Failed(failure) = outcome else {
                panic!("{field} invalid value accepted")
            };
            assert_eq!(failure.safe_code, code);
            assert!(failure.request_sent);
            assert!(!format!("{failure:?}").contains("canary"));
            assert_eq!(wire.protocol, ModelProviderWireProtocol::OpenAiResponses);
        }
    }
}

#[tokio::test]
async fn optional_fingerprint_absence_is_not_a_fabricated_identity() {
    let mut digests = Vec::new();
    for fingerprint in [
        None,
        Some(Value::Null),
        Some(serde_json::json!("")),
        Some(serde_json::json!("actual-fingerprint")),
    ] {
        let mut events = openai_text_events("hello");
        let object = events[2].data["response"].as_object_mut().unwrap();
        if let Some(value) = &fingerprint {
            object.insert("system_fingerprint".into(), value.clone());
        } else {
            object.remove("system_fingerprint");
        }
        let (outcome, _) =
            execute_wire_fixture(wire_fixture(OPENAI_RESPONSES_ADAPTER_NAME), events).await;
        let ModelAdapterExecutionOutcome::Succeeded(success) = outcome else {
            panic!("optional fingerprint rejected")
        };
        assert_eq!(
            success.response.observation.model_fingerprint.as_deref(),
            fingerprint
                .as_ref()
                .and_then(Value::as_str)
                .filter(|s| !s.is_empty())
        );
        digests.push(
            success
                .response
                .observation
                .provider_response_digest
                .unwrap(),
        );
    }
    for (i, a) in digests.iter().enumerate() {
        for b in &digests[i + 1..] {
            assert_ne!(a, b);
        }
    }
    for field in ["model", "system_fingerprint"] {
        for bad in [
            "".to_owned(),
            "wrong-model".to_owned(),
            "bad\nvalue".to_owned(),
            "a".repeat(10_000),
        ] {
            if field == "system_fingerprint" && (bad.is_empty() || bad == "wrong-model") {
                continue;
            }
            let mut events = openai_text_events("hello");
            events[2].data["response"][field] = serde_json::json!(bad);
            assert!(!matches!(
                execute_wire_fixture_result(wire_fixture(OPENAI_RESPONSES_ADAPTER_NAME), events)
                    .await
                    .0,
                Ok(ModelAdapterExecutionOutcome::Succeeded(_))
            ));
        }
    }
}

#[tokio::test]
async fn brokered_empty_fragments_remain_bounded_and_cannot_invent_success() {
    let fixture = wire_fixture(OPENAI_RESPONSES_ADAPTER_NAME);
    let baseline_events = openai_text_events("hello");
    let baseline = execute_brokered_fixture(
        fixture.clone(),
        Arc::new(FixtureEgressBroker::from_events(baseline_events.clone())),
    )
    .await;
    let mut events = baseline_events.clone();
    events.insert(
        1,
        event(
            "response.output_text.delta",
            serde_json::json!({"type":"response.output_text.delta","delta":""}),
        ),
    );
    events.insert(
        2,
        event(
            "response.reasoning_text.delta",
            serde_json::json!({"type":"response.reasoning_text.delta","delta":"hidden-canary"}),
        ),
    );
    let encoded = metadata_sse(&events, "\r\n");
    let broker = Arc::new(FixtureEgressBroker::raw(
        200,
        "text/event-stream",
        encoded.chunks(1).map(<[u8]>::to_vec).collect(),
    ));
    let actual = execute_brokered_fixture(fixture.clone(), broker.clone()).await;
    assert_eq!(actual, baseline);
    assert!(!format!("{actual:?}").contains("canary"));
    assert_eq!(broker.calls.load(Ordering::SeqCst), 1);
    for case in ["all_empty", "unknown_key", "no_terminal", "raw_limit"] {
        let mut fixture = fixture.clone();
        let mut events = if case == "all_empty" {
            openai_text_events("")
        } else {
            baseline_events.clone()
        };
        if case == "unknown_key" {
            events[1].data["extra"] = serde_json::json!(true);
        }
        if case == "no_terminal" {
            events.pop();
        }
        if case == "raw_limit" {
            fixture
                .request
                .provider
                .request_limits
                .maximum_response_bytes = 4096;
            fixture
                .request
                .provider
                .request_limits
                .maximum_stream_delta_bytes = 1024;
            let empty = event(
                "response.output_text.delta",
                serde_json::json!({"type":"response.output_text.delta","delta":""}),
            );
            events.splice(1..1, std::iter::repeat_n(empty, 100));
        }
        let broker = Arc::new(FixtureEgressBroker::from_events(events));
        assert!(
            !matches!(
                execute_brokered_fixture_result(fixture, broker.clone()).await,
                Ok(ModelAdapterExecutionOutcome::Succeeded(_))
            ),
            "{case}"
        );
        assert_eq!(broker.calls.load(Ordering::SeqCst), 1);
    }
}
