//! The Remote Search HTTP v1 codec. No business identity or authorization evidence lives here.
use insight_platform_context::{
    MAX_REMOTE_CONTEXT_ITEMS, MAX_REMOTE_CONTEXT_LABEL_BYTES, MAX_REMOTE_CONTEXT_PROJECTION_FIELDS,
};
use insight_platform_contracts::{
    canonical_digest, canonical_json, parse_strict_json, DataClassification, JsonLimits,
    Sha256Digest, REMOTE_CONTEXT_PROTOCOL_VERSION,
};
use serde::{Deserialize, Serialize};
use serde_json::Value;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum RemoteSearchWireError {
    InvalidRequest,
    RequestTooLarge,
    InvalidResponse,
}

#[derive(Serialize)]
#[serde(deny_unknown_fields)]
struct RemoteSearchWireRequest<'a> {
    schema_version: u32,
    query: &'a Value,
    normalized_query_digest: &'a Sha256Digest,
    normalized_filter_digest: &'a Sha256Digest,
    requested_projection: &'a [String],
    page_size: u32,
    cursor_digest: &'a Option<Sha256Digest>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct RemoteSearchWireResponse {
    pub schema_version: u32,
    pub items: Vec<RemoteSearchWireItem>,
    pub next_cursor_digest: Option<Sha256Digest>,
    pub remote_revision_digest: Option<Sha256Digest>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct RemoteSearchWireItem {
    pub source_identity: String,
    pub content: Value,
    pub structured_fields: Value,
    pub score_millionths: Option<i32>,
    pub locator: String,
    pub display_label: String,
    pub classification: DataClassification,
}

pub(super) struct DecodedRemoteSearchWire {
    pub response: RemoteSearchWireResponse,
    pub canonical_response_digest: Sha256Digest,
}

pub(super) fn encode_wire(
    query: &Value,
    query_digest: &Sha256Digest,
    filter_digest: &Sha256Digest,
    projection: &[String],
    page_size: u32,
    cursor: &Option<Sha256Digest>,
    maximum_request_bytes: u32,
) -> Result<Vec<u8>, RemoteSearchWireError> {
    if page_size == 0
        || page_size as usize > MAX_REMOTE_CONTEXT_ITEMS
        || maximum_request_bytes == 0
        || projection.len() > MAX_REMOTE_CONTEXT_PROJECTION_FIELDS
        || !projection.windows(2).all(|pair| pair[0] < pair[1])
    {
        return Err(RemoteSearchWireError::InvalidRequest);
    }
    // These are separate semantic digests. The business caller binds actual Inline bytes to its
    // admitted input; a wire encoder must not claim they equal a normalized query/filter digest.
    let value = serde_json::to_value(RemoteSearchWireRequest {
        schema_version: REMOTE_CONTEXT_PROTOCOL_VERSION,
        query,
        normalized_query_digest: query_digest,
        normalized_filter_digest: filter_digest,
        requested_projection: projection,
        page_size,
        cursor_digest: cursor,
    })
    .map_err(|_| RemoteSearchWireError::InvalidRequest)?;
    let bytes = canonical_json(&value).map_err(|_| RemoteSearchWireError::InvalidRequest)?;
    if bytes.len() > maximum_request_bytes as usize {
        return Err(RemoteSearchWireError::RequestTooLarge);
    }
    Ok(bytes)
}

pub(super) fn decode_wire(
    bytes: &[u8],
    page_size: u32,
    maximum_classification: DataClassification,
    maximum_response_bytes: u32,
) -> Result<DecodedRemoteSearchWire, RemoteSearchWireError> {
    if page_size == 0
        || page_size as usize > MAX_REMOTE_CONTEXT_ITEMS
        || maximum_response_bytes == 0
    {
        return Err(RemoteSearchWireError::InvalidResponse);
    }
    let value = parse_strict_json(
        bytes,
        JsonLimits {
            max_bytes: maximum_response_bytes as usize,
            max_depth: 32,
            max_items_per_array: page_size as usize,
            max_properties_per_object: 16,
            max_string_bytes: maximum_response_bytes as usize,
        },
    )
    .map_err(|_| RemoteSearchWireError::InvalidResponse)?;
    let canonical_response_digest = canonical_digest(&value)
        .map_err(|_| RemoteSearchWireError::InvalidResponse)?
        .parse()
        .map_err(|_| RemoteSearchWireError::InvalidResponse)?;
    let response: RemoteSearchWireResponse =
        serde_json::from_value(value).map_err(|_| RemoteSearchWireError::InvalidResponse)?;
    if response.schema_version != REMOTE_CONTEXT_PROTOCOL_VERSION
        || response.items.len() > page_size as usize
        || response.items.iter().any(|item| {
            item.classification.rank() > maximum_classification.rank()
                || item.display_label.is_empty()
                || item.display_label.len() > MAX_REMOTE_CONTEXT_LABEL_BYTES
                || item.display_label.chars().any(char::is_control)
                || !item.structured_fields.is_object()
                || item
                    .score_millionths
                    .is_some_and(|score| !(-1_000_000..=1_000_000).contains(&score))
        })
    {
        return Err(RemoteSearchWireError::InvalidResponse);
    }
    Ok(DecodedRemoteSearchWire {
        response,
        canonical_response_digest,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    fn digest(value: &Value) -> Sha256Digest {
        canonical_digest(value).unwrap().parse().unwrap()
    }
    fn response() -> Value {
        json!({"schema_version":1,"items":[{"source_identity":"document#L1-L2","content":"actual text","structured_fields":{"start_line":1,"end_line":2},"score_millionths":1_000_000,"locator":"document#L1-L2","display_label":"document:1-2","classification":"public"}],"next_cursor_digest":null,"remote_revision_digest":null})
    }
    fn accepted(value: &Value) -> bool {
        decode_wire(
            &serde_json::to_vec(value).unwrap(),
            1,
            DataClassification::Public,
            8192,
        )
        .is_ok()
    }
    #[test]
    fn wire_keeps_actual_query_and_separate_semantic_digests_without_business_ids() {
        let query = json!({"question":"原文 \\\" PostgreSQL"});
        let query_digest = digest(&json!({"normalized":true}));
        let filter = digest(&json!({"schema_version":1,"filter":null}));
        let bytes = encode_wire(&query, &query_digest, &filter, &[], 1, &None, 8192).unwrap();
        let actual: Value = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(
            actual,
            json!({"schema_version":1,"query":query,"normalized_query_digest":query_digest,"normalized_filter_digest":filter,"requested_projection":[],"page_size":1,"cursor_digest":null})
        );
        assert_eq!(
            encode_wire(
                &query,
                &query_digest,
                &filter,
                &[],
                1,
                &None,
                bytes.len() as u32 - 1
            ),
            Err(RemoteSearchWireError::RequestTooLarge)
        );
        assert_eq!(
            encode_wire(&query, &query_digest, &filter, &[], 0, &None, 8192),
            Err(RemoteSearchWireError::InvalidRequest)
        );
        let decoded = decode_wire(
            &serde_json::to_vec_pretty(&response()).unwrap(),
            1,
            DataClassification::Public,
            8192,
        )
        .unwrap();
        assert_eq!(decoded.canonical_response_digest, digest(&response()));
        assert_eq!(decoded.response.items[0].source_identity, "document#L1-L2");
        assert_eq!(decoded.response.items[0].locator, "document#L1-L2");
    }
    #[test]
    fn strict_decode_preserves_closed_profile_bounds_and_never_normalizes_invalid_items() {
        let original = response();
        assert!(accepted(&original));
        for (pointer, replacement) in [
            ("/schema_version", json!(2)),
            ("/items/0/classification", json!("internal")),
            ("/items/0/score_millionths", json!(1_000_001)),
            ("/items/0/score_millionths", json!(-1_000_001)),
            ("/items/0/score_millionths", json!(0.5)),
            ("/items/0/structured_fields", json!([])),
            ("/items/0/display_label", json!("")),
            ("/items/0/display_label", json!("bad\nlabel")),
            (
                "/items/0/display_label",
                json!("a".repeat(MAX_REMOTE_CONTEXT_LABEL_BYTES + 1)),
            ),
        ] {
            let mut value = original.clone();
            *value.pointer_mut(pointer).unwrap() = replacement;
            assert!(!accepted(&value), "{pointer}");
        }
        let mut unknown = original.clone();
        unknown["secret"] = json!("untrusted");
        assert!(!accepted(&unknown));
        let mut extra = original.clone();
        extra["items"]
            .as_array_mut()
            .unwrap()
            .push(original["items"][0].clone());
        assert!(!accepted(&extra));
        let duplicate = serde_json::to_string(&original).unwrap().replacen(
            "\"schema_version\":1",
            "\"schema_version\":1,\"schema_version\":1",
            1,
        );
        assert!(decode_wire(duplicate.as_bytes(), 1, DataClassification::Public, 8192).is_err());
        let bytes = serde_json::to_vec(&original).unwrap();
        assert!(decode_wire(
            &bytes,
            1,
            DataClassification::Public,
            bytes.len() as u32 - 1
        )
        .is_err());
        assert!(decode_wire(b"{\xff}", 1, DataClassification::Public, 8192).is_err());
    }
}
