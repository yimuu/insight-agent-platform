//! A fixed, non-streaming text exchange. It is never a ModelTurn or conformance test.
use insight_platform_contracts::*;
use serde_json::{json, Value};

pub fn model_connection_request(
    target: &ModelConnectionTargetV1,
) -> Result<Vec<u8>, ModelConnectionError> {
    if !target.validate() {
        return Err(ModelConnectionError::Rejected);
    }
    let body = match target.protocol {
        ModelProviderWireProtocol::OpenAiResponses => {
            json!({"model":target.model_identity.value,"input":[{"role":"user","content":[{"type":"input_text","text":MODEL_PROBE_PROMPT}]}],"max_output_tokens":target.maximum_output_tokens,"stream":false,"store":false})
        }
        ModelProviderWireProtocol::AnthropicMessages => {
            json!({"model":target.model_identity.value,"messages":[{"role":"user","content":[{"type":"text","text":MODEL_PROBE_PROMPT}]}],"max_tokens":target.maximum_output_tokens,"stream":false})
        }
    };
    let bytes = canonical_json(&body).map_err(|_| ModelConnectionError::Rejected)?;
    if bytes.len()
        > MODEL_PROBE_MAXIMUM_REQUEST_BYTES
            .min(target.request_limits.maximum_request_bytes as usize)
    {
        return Err(ModelConnectionError::Rejected);
    }
    Ok(bytes)
}

/// Recognize a bounded protocol response, including budget-truncated reasoning.
/// This is not proof of a completed answer; all response content is discarded.
pub fn model_connection_response(protocol: ModelProviderWireProtocol, bytes: &[u8]) -> bool {
    let Ok(value) = parse_strict_json(
        bytes,
        JsonLimits {
            max_bytes: MODEL_PROBE_MAXIMUM_RESPONSE_BYTES,
            max_depth: 12,
            max_properties_per_object: 64,
            max_items_per_array: 64,
            max_string_bytes: MODEL_PROBE_MAXIMUM_RESPONSE_BYTES,
        },
    ) else {
        return false;
    };
    if !nonempty(&value, "id") || !nonempty(&value, "model") {
        return false;
    }
    match protocol {
        ModelProviderWireProtocol::OpenAiResponses => {
            keys(
                &value,
                &[
                    "id",
                    "object",
                    "created_at",
                    "completed_at",
                    "frequency_penalty",
                    "presence_penalty",
                    "status",
                    "background",
                    "error",
                    "incomplete_details",
                    "instructions",
                    "max_output_tokens",
                    "max_tool_calls",
                    "model",
                    "output",
                    "parallel_tool_calls",
                    "previous_response_id",
                    "prompt_cache_key",
                    "prompt_cache_retention",
                    "reasoning",
                    "safety_identifier",
                    "service_tier",
                    "store",
                    "system_fingerprint",
                    "temperature",
                    "text",
                    "tool_choice",
                    "tools",
                    "top_logprobs",
                    "top_p",
                    "truncation",
                    "usage",
                    "user",
                    "metadata",
                ],
            ) && crate::responses_metadata::valid_response_metadata(&value)
                && value.get("object").and_then(Value::as_str) == Some("response")
                && matches!(
                    value.get("status").and_then(Value::as_str),
                    Some("completed" | "incomplete")
                )
                && value.get("error").is_none_or(Value::is_null)
                && openai_probe_output(&value)
        }
        ModelProviderWireProtocol::AnthropicMessages => {
            keys(
                &value,
                &[
                    "id",
                    "type",
                    "role",
                    "model",
                    "content",
                    "stop_reason",
                    "stop_sequence",
                    "usage",
                ],
            ) && value.get("type").and_then(Value::as_str) == Some("message")
                && value.get("role").and_then(Value::as_str) == Some("assistant")
                && matches!(
                    value.get("stop_reason").and_then(Value::as_str),
                    Some("end_turn" | "max_tokens" | "stop_sequence")
                )
                && value
                    .get("content")
                    .and_then(Value::as_array)
                    .is_some_and(|parts| {
                        !parts.is_empty()
                            && parts.iter().all(|part| {
                                keys(part, &["type", "text"])
                                    && part.get("type").and_then(Value::as_str) == Some("text")
                                    && part.get("text").is_some_and(Value::is_string)
                            })
                    })
        }
    }
}
fn openai_probe_output(response: &Value) -> bool {
    let Some(items) = response.get("output").and_then(Value::as_array) else {
        return false;
    };
    if items.is_empty() {
        return false;
    }
    let mut message_seen = false;
    for item in items {
        match item.get("type").and_then(Value::as_str) {
            Some("message") => {
                if !keys(item, &["id", "type", "status", "role", "content"])
                    || item.get("role").and_then(Value::as_str) != Some("assistant")
                    || !item
                        .get("content")
                        .and_then(Value::as_array)
                        .is_some_and(|parts| {
                            !parts.is_empty()
                                && parts.iter().all(|part| {
                                    keys(part, &["type", "text", "annotations", "logprobs"])
                                        && part.get("type").and_then(Value::as_str)
                                            == Some("output_text")
                                        && part.get("text").is_some_and(Value::is_string)
                                })
                        })
                {
                    return false;
                }
                message_seen = true;
            }
            Some("reasoning") if probe_reasoning_item(item) => {}
            _ => return false,
        }
    }
    message_seen || response.get("status").and_then(Value::as_str) == Some("incomplete")
}

fn probe_reasoning_item(item: &Value) -> bool {
    keys(
        item,
        &[
            "id",
            "type",
            "summary",
            "status",
            "content",
            "encrypted_content",
        ],
    ) && nonempty(item, "id")
        && item
            .get("summary")
            .is_some_and(|parts| probe_reasoning_parts(parts, "summary_text"))
        && item
            .get("content")
            .is_none_or(|parts| probe_reasoning_parts(parts, "reasoning_text"))
        && item.get("status").is_none_or(|status| {
            matches!(
                status.as_str(),
                Some("in_progress" | "completed" | "incomplete")
            )
        })
        && item
            .get("encrypted_content")
            .is_none_or(|content| content.is_null() || content.is_string())
}

fn probe_reasoning_parts(parts: &Value, expected_type: &str) -> bool {
    parts.as_array().is_some_and(|parts| {
        parts.iter().all(|part| {
            keys(part, &["type", "text"])
                && part.get("type").and_then(Value::as_str) == Some(expected_type)
                && part.get("text").is_some_and(Value::is_string)
        })
    })
}

fn keys(v: &Value, allowed: &[&str]) -> bool {
    v.as_object()
        .is_some_and(|o| o.keys().all(|k| allowed.contains(&k.as_str())))
}
fn nonempty(v: &Value, key: &str) -> bool {
    v.get(key)
        .and_then(Value::as_str)
        .is_some_and(|s| !s.is_empty() && s.len() <= 255 && !s.chars().any(char::is_control))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn diagnostics_recognize_bounded_reasoning_without_claiming_an_answer() {
        let reasoning = json!({"id":"rs_1","type":"reasoning","summary":[{"type":"summary_text","text":"synthetic diagnostic fixture"}]});
        let message = json!({"type":"message","role":"assistant","content":[{"type":"output_text","text":"OK"}]});
        let response = |status: &str, items: Value| json!({"id":"resp_1","object":"response","model":"actual-revision","status":status,"output":items});
        let accepts = |body: &Value| {
            model_connection_response(
                ModelProviderWireProtocol::OpenAiResponses,
                &serde_json::to_vec(body).unwrap(),
            )
        };
        assert!(accepts(&response("completed", json!([reasoning, message]))));
        assert!(accepts(&response("incomplete", json!([reasoning]))));
        assert!(accepts(&response(
            "incomplete",
            json!([{
                "id":"rs_2","type":"reasoning","summary":[],"status":"incomplete",
                "content":[{"type":"reasoning_text","text":"synthetic fixture"}],
                "encrypted_content":null
            }])
        )));
        assert!(accepts(&response(
            "incomplete",
            json!([{
                "id":"rs_3","type":"reasoning","summary":[],"encrypted_content":"opaque fixture"
            }])
        )));
        assert!(!accepts(&response("completed", json!([reasoning]))));
        assert!(!accepts(&response("incomplete", json!([]))));
        assert!(!accepts(&response("failed", json!([reasoning]))));
        assert!(!accepts(&response("in_progress", json!([reasoning]))));
        for bad_item in [
            json!({"id":"rs_1","type":"reasoning"}),
            json!({"id":"rs_1","type":"reasoning","summary":{}}),
            json!({"id":"rs_1","type":"reasoning","summary":[{"type":"summary_text","text":42}]}),
            json!({"id":"rs_1","type":"reasoning","summary":[{"type":"summary_text","text":"x","extension":true}]}),
            json!({"id":"rs_1","type":"reasoning","summary":[],"status":"unknown"}),
            json!({"id":"rs_1","type":"reasoning","summary":[],"status":null}),
            json!({"id":"rs_1","type":"reasoning","summary":[],"encrypted_content":false}),
            json!({"id":"rs_1","type":"reasoning","summary":[],"content":null}),
            json!({"id":"rs_1","type":"reasoning","summary":[],"content":"wrong"}),
            json!({"id":"rs_1","type":"reasoning","summary":[],"content":[{"type":"tool_call","text":"x"}]}),
            json!({"id":"rs_1","type":"reasoning","summary":[],"content":[{"type":"reasoning_text","text":false}]}),
            json!({"type":"reasoning","summary":[]}),
            json!({"id":null,"type":"reasoning","summary":[]}),
            json!({"id":"rs_1","type":"reasoning","summary":null}),
            json!({"id":"rs_1","type":"reasoning","summary":[],"extension":"x"}),
            json!({"id":"rs_1","type":"function_call","name":"unexpected_tool","arguments":"{}"}),
        ] {
            assert!(!accepts(&response("incomplete", json!([bad_item]))));
            assert!(!accepts(&response("completed", json!([message, bad_item]))));
        }
        assert!(!accepts(&response(
            "incomplete",
            Value::Array(vec![reasoning; 65])
        )));
        let oversized = json!({"id":"rs_1","type":"reasoning","summary":[{"type":"summary_text","text":"x".repeat(MODEL_PROBE_MAXIMUM_RESPONSE_BYTES)}]});
        assert!(!accepts(&response("incomplete", json!([oversized]))));
    }

    #[test]
    fn diagnostics_require_text_protocol_envelopes_without_prompt_compliance_claims() {
        for (protocol, value) in [
            (
                ModelProviderWireProtocol::OpenAiResponses,
                json!({"id":"resp_1","object":"response","model":"actual-revision","status":"completed","output":[{"type":"message","role":"assistant","content":[{"type":"output_text","text":"Not OK"}]}]}),
            ),
            (
                ModelProviderWireProtocol::AnthropicMessages,
                json!({"id":"msg_1","type":"message","model":"actual-revision","role":"assistant","stop_reason":"end_turn","content":[{"type":"text","text":"Not OK"}]}),
            ),
        ] {
            assert!(model_connection_response(
                protocol,
                &serde_json::to_vec(&value).unwrap()
            ));
            for invalid in [
                b"{}".as_slice(),
                b"{\"id\":\"a\",\"id\":\"b\"}",
                b"[]",
                b"null",
            ] {
                assert!(!model_connection_response(protocol, invalid));
            }
            let mut extra = value.clone();
            extra["private_extension"] = json!("secret");
            assert!(!model_connection_response(
                protocol,
                &serde_json::to_vec(&extra).unwrap()
            ));
            let mut bad = value.clone();
            bad["model"] = json!(null);
            assert!(!model_connection_response(
                protocol,
                &serde_json::to_vec(&bad).unwrap()
            ));
        }
        assert!(!model_connection_response(ModelProviderWireProtocol::AnthropicMessages,br#"{"id":"x","type":"message","model":"x","role":"assistant","stop_reason":"tool_use","content":[{"type":"tool_use","id":"x","name":"x","input":{}}]}"#));
    }
}
