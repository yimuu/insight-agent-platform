//! Real closed RPC encoding and outcome classification; no physical provider is invoked.
use super::*;
use insight_platform_context::{RemoteContextFailureClass, RemoteContextItem};
use insight_platform_contracts::{
    CanonicalHttpEndpoint, CapabilityEndpointScheme, DataClassification, ExactDeploymentRef,
    ExactVersionRef, ResourceId, ValueRef,
};
use serde_json::json;

fn digest(label: &str) -> Sha256Digest {
    typed_digest(&json!({"test":label})).unwrap()
}
fn id(kind: ResourceKind) -> ResourceId {
    ResourceId::from_uuid_v7(kind, uuid::Uuid::now_v7()).unwrap()
}
fn request() -> RemoteContextSearchRequest {
    let endpoint = CanonicalHttpEndpoint {
        scheme: CapabilityEndpointScheme::Https,
        host: "search.example.test".into(),
        port: 443,
        base_path: "/v1/query".into(),
    };
    let policy =
        |name| ExactVersionRef::new(id(ResourceKind::PolicyRevision), digest(name)).unwrap();
    RemoteContextSearchRequest {
        schema_version: 2,
        tenant_id: id(ResourceKind::Tenant),
        context_query_id: id(ResourceKind::ContextQuery),
        job_id: id(ResourceKind::Job),
        worker_process_generation_id: id(ResourceKind::WorkerProcessGeneration),
        physical_attempt: 1,
        lease_generation: 1,
        lease_token_digest: digest("lease"),
        admission_digest: digest("admission"),
        context_deployment: ExactDeploymentRef::new(
            id(ResourceKind::ContextDeployment),
            digest("deployment"),
        )
        .unwrap(),
        implementation_revision: ExactVersionRef::new(
            id(ResourceKind::ContextSourceImplementationRevision),
            digest("implementation"),
        )
        .unwrap(),
        protocol_contract_digest:
            insight_platform_contracts::remote_context_protocol_contract_digest(),
        result_mapping_digest: insight_platform_contracts::remote_context_result_mapping_digest(),
        endpoint_identity_digest: endpoint.canonical_digest().unwrap(),
        endpoint,
        region: "cn-east-1".parse().unwrap(),
        secret_bindings: vec![],
        network_policy: policy("network"),
        tls_policy: policy("tls"),
        trust_policy: policy("trust"),
        query_input: ValueRef::Inline {
            value: json!({"question":"capacity"}),
        },
        normalized_query_digest: digest("query"),
        normalized_filter_digest: digest("filter"),
        requested_projection: vec![],
        maximum_classification: DataClassification::Public,
        page_size: 1,
        cursor_digest: None,
        maximum_request_bytes: 65_536,
        maximum_response_bytes: 1_048_576,
        deadline: Utc::now() + chrono::Duration::minutes(1),
    }
}
fn response(request: &RemoteContextSearchRequest, bytes: usize) -> RemoteContextSearchResponse {
    RemoteContextSearchResponse {
        schema_version: 1,
        items: vec![RemoteContextItem {
            source_item_identity_digest: digest("source"),
            content: json!("x".repeat(bytes)),
            structured_fields: json!({}),
            score_millionths: None,
            locator_digest: digest("locator"),
            authorization_evidence_digest: digest("authorization"),
            display_label: "capacity sample".into(),
            classification: DataClassification::Public,
        }],
        next_cursor_digest: None,
        backend_request_digest: request.normalized_query_digest.clone(),
        backend_response_digest: digest("backend"),
        ranking_evidence_digest: digest("ranking"),
        remote_revision_digest: None,
        observed_at: Utc::now(),
    }
}
fn limits() -> EgressInternalRpcLimits {
    EgressInternalRpcLimits::new(MAX_EGRESS_METADATA_BYTES_HARD, 1_048_576).unwrap()
}
#[test]
fn complete_remote_result_roundtrips_above_old_capacity_and_at_exact_limit() {
    let request = request();
    request.validate_at(Utc::now()).unwrap();
    let request_digest = typed_digest(&request).unwrap();
    let mut result = response(&request, 0);
    let overhead = serde_jcs::to_vec(&UnaryOutcome::<_, RemoteContextFailure>::Succeeded(
        result.clone(),
    ))
    .unwrap()
    .len();
    for bytes in [262_144, MAX_EGRESS_METADATA_BYTES_HARD - overhead] {
        result.items[0].content = json!("x".repeat(bytes));
        let outcome = UnaryOutcome::<_, RemoteContextFailure>::Succeeded(result.clone());
        let expected = serde_jcs::to_vec(&outcome).unwrap();
        assert!(expected.len() > 65_536);
        let envelope = encode_remote_context_outcome(outcome, &request_digest, limits()).unwrap();
        assert_eq!(envelope.canonical_metadata_json, expected);
        assert!(envelope.payload.is_empty());
        let decoded =
            decode_remote_context_outcome(Ok(envelope), &request, &digest("attempted"), limits())
                .unwrap();
        assert_eq!(decoded, result);
    }
}
#[test]
fn one_byte_frame_overflow_preserves_known_result_evidence_as_permanent_failure() {
    let request = request();
    let request_digest = typed_digest(&request).unwrap();
    let mut result = response(&request, 0);
    let overhead = serde_jcs::to_vec(&UnaryOutcome::<_, RemoteContextFailure>::Succeeded(
        result.clone(),
    ))
    .unwrap()
    .len();
    result.items[0].content = json!("x".repeat(MAX_EGRESS_METADATA_BYTES_HARD - overhead + 1));
    let expected_evidence = typed_digest(&json!({
        "schema_version":1, "stage":"remote_context_result_rpc_capacity_rejected",
        "request_digest":request_digest, "response_digest":typed_digest(&result).unwrap(),
    }))
    .unwrap();
    let envelope =
        encode_remote_context_outcome(UnaryOutcome::Succeeded(result), &request_digest, limits())
            .unwrap();
    assert!(envelope.canonical_metadata_json.len() < 1024);
    let failure =
        decode_remote_context_outcome(Ok(envelope), &request, &digest("attempted"), limits())
            .unwrap_err();
    assert_eq!(
        failure.class,
        RemoteContextFailureClass::PermanentAfterDispatch
    );
    assert_eq!(failure.code, "context_egress_rpc_result_too_large");
    assert_eq!(failure.dispatch_evidence_digest, Some(expected_evidence));
    failure.validate().unwrap();
}
#[test]
fn failures_after_rpc_attempt_never_claim_no_dispatch_or_return_upstream_details() {
    let request = request();
    let attempted = digest("actual-envelope-submission");
    let valid = encode_remote_context_outcome(
        UnaryOutcome::Succeeded(response(&request, 1)),
        &typed_digest(&request).unwrap(),
        limits(),
    )
    .unwrap();
    let mut changed = valid.clone();
    changed.canonical_metadata_json.push(b'x');
    let mut old = valid.clone();
    old.operation = "remote_context.outcome/v1".into();
    let mut wrong_result = response(&request, 1);
    wrong_result.backend_request_digest = digest("wrong-request");
    let wrong = encode_remote_context_outcome(
        UnaryOutcome::Succeeded(wrong_result),
        &digest("wrong"),
        limits(),
    )
    .unwrap();
    for result in [
        Err(Status::unavailable("provider-secret-text")),
        Err(Status::deadline_exceeded("provider-secret-text")),
        Ok(changed),
        Ok(old),
        Ok(wrong),
    ] {
        let failure =
            decode_remote_context_outcome(result, &request, &attempted, limits()).unwrap_err();
        assert_eq!(failure.class, RemoteContextFailureClass::UncertainDispatch);
        assert_eq!(failure.dispatch_evidence_digest.as_ref(), Some(&attempted));
        assert!(!failure.safe_message.contains("provider-secret-text"));
        failure.validate().unwrap();
    }
    let before = remote_context_rpc_failure("context_egress_rpc_request_invalid", false);
    assert_eq!(
        before.class,
        RemoteContextFailureClass::RejectedBeforeDispatch
    );
    assert!(before.dispatch_evidence_digest.is_none());
}
