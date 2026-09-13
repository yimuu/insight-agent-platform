//! Cross-owner basic declaration → actual production Inline materializer regression.
//! All identities and model contents below are synthetic; no provider or database is contacted.
use super::{
    InlineModelOutputMaterializer, ModelOutputMaterializer, ModelProviderWireProtocol,
    ModelTurnLimits, UuidModelWorkerIdentityFactory, ANTHROPIC_MESSAGES_ADAPTER_NAME,
    OPENAI_RESPONSES_ADAPTER_NAME,
};
use chrono::{Duration as ChronoDuration, Utc};
use insight_platform_contracts::*;
use insight_platform_models::execution::{canonical_request_digest, ModelAdapterExecutionRequest};
use insight_platform_models::*;
use insight_platform_registry::model_configuration::basic_provider_request_limits;
use std::sync::Arc;

fn id(kind: ResourceKind, suffix: u16) -> ResourceId {
    format!(
        "{}_0198f1c8-32e4-75e1-a9e8-d95ca0f4{suffix:04x}",
        kind.descriptor().prefix
    )
    .parse()
    .unwrap()
}

fn sha(character: char) -> Sha256Digest {
    format!("sha256:{}", character.to_string().repeat(64))
        .parse()
        .unwrap()
}

fn version(kind: ResourceKind, suffix: u16, character: char) -> ExactVersionRef {
    ExactVersionRef::new(id(kind, suffix), sha(character)).unwrap()
}

fn deployment(kind: ResourceKind, suffix: u16, character: char) -> ExactDeploymentRef {
    ExactDeploymentRef::new(id(kind, suffix), sha(character)).unwrap()
}

fn policy(suffix: u16, character: char) -> ExactVersionRef {
    version(ResourceKind::PolicyRevision, suffix, character)
}

fn exact_secret_binding(suffix: u16) -> ExactSecretBindingRef {
    ExactSecretBindingRef::build(
        id(ResourceKind::SecretBinding, suffix),
        1,
        id(ResourceKind::SecretProvider, suffix),
        "provider.api_key".parse().unwrap(),
        SecretResolutionPolicy::Pinned {
            opaque_version_identity_digest: sha('0'),
        },
    )
    .unwrap()
}

fn artifact(suffix: u16, character: char) -> ArtifactRef {
    ArtifactRef::new(
        id(ResourceKind::Artifact, suffix),
        sha(character),
        16,
        "application/json",
        DataClassification::Internal,
        Some("evidence.json".to_owned()),
    )
    .unwrap()
}

fn authoring(suffix: u16, character: char) -> AuthoringPackage {
    AuthoringPackage {
        artifact: artifact(suffix, character),
        manifest_digest: sha(character),
    }
}

fn limits() -> ModelTurnLimits {
    let profile = checked_in_hard_limit_profile();
    ModelTurnLimits::from_profile(&profile).unwrap()
}

struct Fixture {
    request: ModelAdapterExecutionRequest,
    response: CanonicalModelResponse,
}

fn fixture(adapter_name: &str, manifest: char, contract: char) -> Fixture {
    let contract_digest = match adapter_name {
        OPENAI_RESPONSES_ADAPTER_NAME => {
            ModelProviderWireProtocol::OpenAiResponses.adapter_contract_digest()
        }
        ANTHROPIC_MESSAGES_ADAPTER_NAME => {
            ModelProviderWireProtocol::AnthropicMessages.adapter_contract_digest()
        }
        _ => sha(contract),
    };
    let now = Utc::now();
    let tenant_id = id(ResourceKind::Tenant, 1);
    let model_turn_id = id(ResourceKind::ModelTurn, 2);
    let model_deployment = deployment(ResourceKind::ModelDeployment, 3, '1');
    let provider_deployment = deployment(ResourceKind::ModelProviderDeployment, 4, '2');
    let profile_revision = version(ResourceKind::ModelProfileRevision, 5, '3');
    let provider_revision = version(ResourceKind::ModelProviderRevision, 6, '4');
    let protocol_policy = policy(7, '5');
    let parameter_schema_digest = sha('6');
    let region: DataRegion = "cn-east-1".parse().unwrap();
    let installed_adapter = InstalledModelAdapter {
        qualified_name: adapter_name.to_owned(),
        worker_manifest_digest: sha(manifest),
        adapter_contract_digest: contract_digest.clone(),
    };
    let provider = ModelProviderResourceSpec {
        authoring_package: authoring(8, '7'),
        contract_digest: sha('8'),
        dependency_versions: vec![protocol_policy.clone()],
        policy_versions: vec![protocol_policy.clone()],
        installed_adapter,
        protocol_policy: protocol_policy.clone(),
        credential_requirements: vec!["provider.api_key".parse::<SecretPurpose>().unwrap()],
        request_limits: basic_provider_request_limits(),
    };
    let profile = ModelProfileResourceSpec {
        authoring_package: authoring(9, '9'),
        contract_digest: sha('a'),
        dependency_versions: vec![provider_revision.clone()],
        policy_versions: vec![policy(10, 'b')],
        provider_revision: provider_revision.clone(),
        model_identity: ProviderModelIdentity {
            value: "fixture-model-2026-08".to_owned(),
            stability: ModelIdentityStability::Pinned,
        },
        modalities: ModelModalities {
            input: vec![insight_platform_contracts::ModelModality::Text],
            output: vec![insight_platform_contracts::ModelModality::Text],
        },
        context: ContextWindowContract {
            maximum_context_tokens: 4_096,
            maximum_output_tokens: 512,
            tokenizer_contract_digest: Some(sha('c')),
            estimator_contract_digest: sha('d'),
        },
        tools: ModelToolContract {
            supported: false,
            parallel: false,
            maximum_tools: 0,
            maximum_calls_per_turn: 0,
            maximum_argument_bytes: 0,
        },
        structured_output: StructuredOutputContract {
            native: true,
            textual_json_fallback: true,
            may_combine_with_tool_intent: false,
            maximum_schema_bytes: 65_536,
            maximum_output_bytes: 1_048_576,
        },
        parameter_schema_digest: parameter_schema_digest.clone(),
        usage: ModelUsageContract {
            provider_reports_usage: true,
            reports_cached_input_tokens: false,
            reports_reasoning_tokens: false,
            reports_cost: false,
            cost_currency: None,
            estimator_contract_digest: sha('d'),
        },
        data_handling: ProviderDataHandlingContract {
            maximum_classification: DataClassification::Confidential,
            allowed_regions: vec![region.clone()],
            maximum_retention_milliseconds: Some(86_400_000),
            training: ProviderTrainingPolicy::Prohibited,
            subprocessor_set_digest: Some(sha('e')),
        },
        limits: ModelLimits {
            maximum_messages: 16,
            maximum_parts: 32,
            maximum_text_bytes: 32_768,
            maximum_tools: 0,
            maximum_parallel_tool_calls: 0,
            maximum_rounds: 8,
            maximum_input_tokens: 3_000,
            maximum_output_tokens: 512,
        },
        catalog_evidence: ModelCatalogEvidence {
            basis: insight_platform_contracts::ModelEvidenceBasis::Qualification,
            artifact: artifact(11, 'f'),
            source_digest: sha('1'),
            adapter_contract_digest: contract_digest.clone(),
            observed_at: now - ChronoDuration::minutes(1),
            expires_at: now + ChronoDuration::days(1),
        },
    };
    let provider_closure = ModelProviderDeploymentClosure {
        provider_revision,
        endpoint: insight_platform_contracts::normalize_model_base_url(
            "https://api.example.com/v1",
        )
        .unwrap(),
        endpoint_identity_digest: insight_platform_contracts::normalize_model_base_url(
            "https://api.example.com/v1",
        )
        .unwrap()
        .canonical_digest()
        .unwrap(),
        secret_bindings: vec![exact_secret_binding(12)],
        protocol_policy: protocol_policy.clone(),
        network_policy: policy(13, '3'),
        tls_policy: policy(14, '4'),
        trust_policy: policy(15, '5'),
        data_policy: policy(16, '6'),
        region,
        admission_evidence: insight_platform_contracts::ModelAdmissionEvidence {
            basis: insight_platform_contracts::ModelEvidenceBasis::Qualification,
            artifact: artifact(17, '7'),
        },
    };
    let model_closure = ModelDeploymentClosure {
        profile_revision: profile_revision.clone(),
        provider_deployment: provider_deployment.clone(),
        data_policy: policy(18, '8'),
        safety_policy: policy(21, 'b'),
        budget_policy: policy(19, '9'),
        public_projection_policy: policy(20, 'a'),
        generation_defaults: ClosedJsonValue::build(
            parameter_schema_digest.clone(),
            serde_json::json!({"temperature": 0}),
        )
        .unwrap(),
    };
    let canonical_request = CanonicalModelRequest {
        schema_version: 1,
        model_turn_id: model_turn_id.clone(),
        messages: vec![CanonicalMessage {
            role: insight_platform_models::CanonicalMessageRole::Platform,
            parts: vec![CanonicalMessagePart::Text("Answer safely.".to_owned())],
            classification: DataClassification::Internal,
            source: ModelContentSource {
                source_kind: "agent_contract".to_owned(),
                source_id: "agent-fixture".to_owned(),
                source_digest: sha('b'),
                content_digest: sha('b'),
                assembly_phase: insight_platform_models::PromptAssemblyPhase::AgentContract,
                ordinal: 0,
                byte_budget: 1_024,
                token_budget: 256,
                trusted_instruction: true,
            },
        }],
        tools: vec![],
        response_contract: ModelResponseContract {
            output_schema_digest: sha('c'),
            structured_schema: None,
            allow_tool_intents: false,
            allow_message_with_tool_intents: false,
        },
        generation_parameters: ClosedJsonValue::build(
            parameter_schema_digest,
            serde_json::json!({"temperature": 0}),
        )
        .unwrap(),
        max_output_tokens: 100,
        input_token_estimate: 100,
        estimator_contract_digest: sha('d'),
        source_map_digest: sha('e'),
        truncation_policy: policy(21, 'f'),
        classification: DataClassification::Internal,
        deadline: now + ChronoDuration::minutes(5),
        trace_context: SafeTraceContext {
            trace_id_digest: sha('1'),
            parent_span_id_digest: sha('2'),
        },
    };
    let live = NormalizedModelDelta::Text("hello".to_owned());
    let live_bytes = serde_json::to_vec(&live).unwrap().len() as u64;
    let response = CanonicalModelResponse {
        schema_version: 1,
        message: Some(CanonicalAssistantMessage {
            parts: vec![CanonicalMessagePart::Text("hello".to_owned())],
            classification: DataClassification::Internal,
        }),
        structured_output: None,
        tool_intents: vec![],
        finish_reason: CanonicalFinishReason::Completed,
        usage: ModelUsage {
            input_tokens: Some(50),
            output_tokens: Some(10),
            cached_input_tokens: None,
            reasoning_tokens: None,
            provider_reported_cost: None,
            accounting_quality: AccountingQuality::ProviderReported,
        },
        observation: ModelObservation {
            request_sent: true,
            provider_response_digest: Some(sha('3')),
            actual_model_identity: Some("fixture-model-2026-08".to_owned()),
            model_fingerprint: Some("fixture-fingerprint".to_owned()),
            possible_duplicate_charge: false,
            stream_delta_count: 1,
            stream_bytes: live_bytes,
        },
    };
    let request_digest = canonical_request_digest(&canonical_request).unwrap();
    Fixture {
        request: ModelAdapterExecutionRequest {
            schema_version: 1,
            tenant_id,
            run_id: id(ResourceKind::Run, 19),
            model_turn_id,
            job_id: id(ResourceKind::Job, 22),
            worker_process_generation_id: id(ResourceKind::WorkerProcessGeneration, 23),
            worker_manifest_digest: sha(manifest),
            attempt_no: 1,
            attempt_limit: 3,
            lease_generation: 1,
            admission_digest: sha('4'),
            request_digest,
            quota_ceiling: ModelQuotaCeiling {
                concurrent_units: 1,
                requests: 1,
                tokens: 4_096,
                cost_microunits: 10_000,
            },
            model_deployment,
            model_closure,
            profile_revision,
            provider_deployment,
            provider_closure,
            provider_revision: profile.provider_revision.clone(),
            provider,
            profile: Box::new(profile),
            request: Box::new(canonical_request),
        },
        response,
    }
}

#[test]
fn basic_source_default_fits_production_inline_materializer() {
    let materializer =
        InlineModelOutputMaterializer::new(Arc::new(UuidModelWorkerIdentityFactory), limits());
    for protocol in [
        OPENAI_RESPONSES_ADAPTER_NAME,
        ANTHROPIC_MESSAGES_ADAPTER_NAME,
    ] {
        let fixture = fixture(protocol, 'a', 'b');
        fixture.request.validate_at(Utc::now(), limits()).unwrap();
        fixture
            .response
            .validate_for(
                &fixture.request.request,
                &fixture.request.provider,
                &fixture.request.profile,
                limits(),
            )
            .unwrap();
        materializer.validate_execution(&fixture.request).unwrap();
    }
}

#[test]
fn preflight_rejects_above_shared_capacity_without_dispatch() {
    let materializer =
        InlineModelOutputMaterializer::new(Arc::new(UuidModelWorkerIdentityFactory), limits());
    let mut fixture = fixture(OPENAI_RESPONSES_ADAPTER_NAME, 'a', 'b');
    let capacity = inline_model_provider_response_capacity(
        u64::try_from(limits().inline_value_limits().max_bytes).unwrap(),
    )
    .unwrap();
    fixture
        .request
        .provider
        .request_limits
        .maximum_response_bytes = capacity;
    materializer.validate_execution(&fixture.request).unwrap();
    for size in [0, capacity + 1, u32::MAX] {
        fixture
            .request
            .provider
            .request_limits
            .maximum_response_bytes = size;
        let error = materializer
            .validate_execution(&fixture.request)
            .unwrap_err();
        assert_eq!(
            error.class,
            insight_platform_model_adapters::ModelAdapterFailureClass::RejectedBeforeDispatch
        );
        assert_eq!(error.safe_code, "model_output_too_large");
        assert!(!error.request_sent);
        assert!(error.retry_at.is_none());
    }
}

#[derive(Default)]
struct CountingIdentities(std::sync::atomic::AtomicUsize);

impl super::ModelWorkerIdentityFactory for CountingIdentities {
    fn new_resource_id(
        &self,
        kind: ResourceKind,
    ) -> Result<ResourceId, super::ModelWorkerIdentityError> {
        let ordinal = self.0.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        Ok(id(kind, u16::try_from(900 + ordinal).unwrap()))
    }
    fn new_opaque_digest(&self) -> Result<Sha256Digest, super::ModelWorkerIdentityError> {
        panic!("Inline output must not allocate an opaque physical identity")
    }
}

fn success(
    response: CanonicalModelResponse,
) -> insight_platform_model_adapters::ModelAdapterSuccess {
    insight_platform_model_adapters::ModelAdapterSuccess {
        response: Box::new(response),
        stream_evidence: ModelStreamEvidence {
            accepted_delta_count: 1,
            accepted_delta_bytes: 16,
            terminal_sequence: 2,
        },
    }
}

#[tokio::test]
async fn actual_inline_output_keeps_content_digest_and_byte_limit() {
    let fixture = fixture(OPENAI_RESPONSES_ADAPTER_NAME, 'a', 'b');
    let identities = Arc::new(CountingIdentities::default());
    let materializer = InlineModelOutputMaterializer::new(Arc::clone(&identities), limits());
    let value = serde_json::to_value(&fixture.response).unwrap();
    let output = materializer
        .materialize(&fixture.request, success(fixture.response.clone()))
        .await
        .unwrap();
    assert_eq!(
        output.value,
        ValueRef::Inline {
            value: value.clone()
        }
    );
    assert_eq!(
        output.content_digest.as_str(),
        canonical_digest(&value).unwrap()
    );
    assert_eq!(identities.0.load(std::sync::atomic::Ordering::SeqCst), 1);

    let identities = Arc::new(CountingIdentities::default());
    let materializer = InlineModelOutputMaterializer::new(Arc::clone(&identities), limits());
    let mut oversized = fixture.response;
    oversized.message.as_mut().unwrap().parts = vec![CanonicalMessagePart::Text(
        "x".repeat(limits().inline_value_limits().max_bytes),
    )];
    assert!(
        serde_json::to_vec(&oversized).unwrap().len() > limits().inline_value_limits().max_bytes
    );
    let failure = materializer
        .materialize(&fixture.request, success(oversized))
        .await
        .unwrap_err();
    assert_eq!(failure.safe_code, "model_output_too_large");
    assert!(failure.request_sent);
    assert_eq!(
        failure.class,
        insight_platform_model_adapters::ModelAdapterFailureClass::Permanent
    );
    assert!(failure.retry_at.is_none());
    assert_eq!(identities.0.load(std::sync::atomic::Ordering::SeqCst), 0);
}
