//! Synthetic response bytes through the real SSE and RPC owners; no provider is contacted.
use super::*;
use futures::StreamExt;
use insight_platform_model_adapters::decode_model_provider_sse;
use serde_json::json;

fn terminal_event(text: String) -> ModelProviderWireEvent {
    ModelProviderWireEvent {
        event_name: "response.completed".to_owned(),
        data: json!({
            "type": "response.completed",
            "response": {
                "id": "response-capacity-fixture", "object": "response",
                "status": "completed", "model": "capacity-fixture",
                "output": [{
                    "id": "message-capacity-fixture", "type": "message",
                    "role": "assistant", "status": "completed",
                    "content": [{"type": "output_text", "text": text, "annotations": []}]
                }],
                "usage": {"input_tokens": 50, "output_tokens": 10, "total_tokens": 60}
            }
        }),
    }
}

fn limits() -> EgressInternalRpcLimits {
    EgressInternalRpcLimits::new(MAX_EGRESS_METADATA_BYTES_HARD, 1_048_576).unwrap()
}

fn assert_event_roundtrip(event: ModelProviderWireEvent) {
    let envelope = encode_metadata(
        &ModelStreamFrame::Event(event.clone()),
        MODEL_PROVIDER_FRAME,
        limits(),
    )
    .unwrap();
    assert!(envelope.payload.is_empty());
    let expected_bytes = serde_jcs::to_vec(&ModelStreamFrame::Event(event.clone())).unwrap();
    assert_eq!(envelope.canonical_metadata_json, expected_bytes);
    match decode_metadata::<ModelStreamFrame>(envelope, MODEL_PROVIDER_FRAME, limits()).unwrap() {
        ModelStreamFrame::Event(decoded) => assert_eq!(decoded, event),
        ModelStreamFrame::Failed(_) => panic!("an exact response event became a failure"),
    }
}

#[tokio::test]
async fn large_terminal_sse_roundtrips_with_the_supported_metadata_allowance() {
    let overhead = serde_json::to_vec(&json!({"answer": ""})).unwrap().len();
    let answer = json!({"answer": "x".repeat(262_144 - overhead)});
    let text = serde_json::to_string(&answer).unwrap();
    assert_eq!(text.len(), 262_144);
    let event = terminal_event(text);
    let wire = format!("event: response.completed\ndata: {}\n\n", event.data).into_bytes();
    let response_limit = insight_platform_contracts::inline_model_provider_response_capacity(
        insight_platform_contracts::checked_in_hard_limit_profile()
            .run_scheduler
            .inline_value_bytes
            .hard_max,
    )
    .unwrap();
    assert!(wire.len() < response_limit as usize);
    let chunks = wire
        .chunks(4096)
        .map(|chunk| Ok(chunk.to_vec()))
        .collect::<Vec<_>>();
    let mut stream =
        decode_model_provider_sse(futures::stream::iter(chunks).boxed(), response_limit).unwrap();
    let decoded = stream.next().await.unwrap().unwrap();
    assert_eq!(decoded, event);
    assert!(stream.next().await.is_none());
    let frame = ModelStreamFrame::Event(decoded.clone());
    let frame_bytes = serde_jcs::to_vec(&frame).unwrap();
    assert!(frame_bytes.len() > 65_536);
    assert!(encode_metadata(
        &frame,
        MODEL_PROVIDER_FRAME,
        EgressInternalRpcLimits::new(65_536, 1_048_576).unwrap()
    )
    .is_err());
    assert_event_roundtrip(decoded);
}

#[test]
fn metadata_frame_exact_limit_passes_and_one_more_byte_is_rejected() {
    let overhead = serde_jcs::to_vec(&ModelStreamFrame::Event(terminal_event(String::new())))
        .unwrap()
        .len();
    let exact = terminal_event("x".repeat(MAX_EGRESS_METADATA_BYTES_HARD - overhead));
    assert_eq!(
        serde_jcs::to_vec(&ModelStreamFrame::Event(exact.clone()))
            .unwrap()
            .len(),
        MAX_EGRESS_METADATA_BYTES_HARD
    );
    assert_event_roundtrip(exact);
    let oversized = ModelStreamFrame::Event(terminal_event(
        "x".repeat(MAX_EGRESS_METADATA_BYTES_HARD - overhead + 1),
    ));
    assert!(encode_metadata(&oversized, MODEL_PROVIDER_FRAME, limits()).is_err());
    let mut incoming = encode_metadata(
        &ModelStreamFrame::Event(terminal_event(String::new())),
        MODEL_PROVIDER_FRAME,
        limits(),
    )
    .unwrap();
    incoming.canonical_metadata_json = serde_jcs::to_vec(&oversized).unwrap();
    assert!(decode_metadata::<ModelStreamFrame>(incoming, MODEL_PROVIDER_FRAME, limits()).is_err());
    let larger = EgressInternalRpcLimits::new(MAX_EGRESS_METADATA_BYTES_HARD + 1, 1_048_576);
    assert!(larger.is_err());
}

#[test]
fn model_frame_identity_and_closed_json_checks_are_retained() {
    let frame = ModelStreamFrame::Event(terminal_event("small".to_owned()));
    let original = encode_metadata(&frame, MODEL_PROVIDER_FRAME, limits()).unwrap();
    let mut changed = original.clone();
    changed.canonical_metadata_json = changed
        .canonical_metadata_json
        .iter()
        .map(|byte| if *byte == b's' { b't' } else { *byte })
        .collect();
    assert!(decode_metadata::<ModelStreamFrame>(changed, MODEL_PROVIDER_FRAME, limits()).is_err());
    let mut changed = original.clone();
    changed.schema_version += 1;
    assert!(decode_metadata::<ModelStreamFrame>(changed, MODEL_PROVIDER_FRAME, limits()).is_err());
    let mut changed = original.clone();
    changed.operation = "model_provider.other/v1".to_owned();
    assert!(decode_metadata::<ModelStreamFrame>(changed, MODEL_PROVIDER_FRAME, limits()).is_err());
    let mut changed = original;
    changed.canonical_metadata_json = b"{\"kind\":\"event\",\"kind\":\"failed\"}".to_vec();
    assert!(decode_metadata::<ModelStreamFrame>(changed, MODEL_PROVIDER_FRAME, limits()).is_err());
    let mut unknown = serde_json::to_value(&frame).unwrap();
    unknown["unknown"] = json!(true);
    let envelope = encode_metadata(&unknown, MODEL_PROVIDER_FRAME, limits()).unwrap();
    assert!(decode_metadata::<ModelStreamFrame>(envelope, MODEL_PROVIDER_FRAME, limits()).is_err());
}
