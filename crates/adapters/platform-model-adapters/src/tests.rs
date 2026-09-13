use super::*;

#[path = "responses_text_tests.rs"]
mod responses_text_tests;
#[path = "responses_usage_tests.rs"]
mod responses_usage_tests;
use chrono::Duration as ChronoDuration;
use futures::{stream, StreamExt};
use insight_platform_contracts::{
    checked_in_hard_limit_profile, ArtifactRef, AuthoringPackage, ClosedJsonValue, CommandOutcome,
    ContextWindowContract, DataClassification, DataRegion, Effect, ExactSecretBindingRef,
    ExactVersionRef, ExternalLeafFailureMutationIds, ExternalLeafResumeMutationIds,
    InstalledModelAdapter, ModelCatalogEvidence, ModelIdentityStability, ModelLimits,
    ModelModalities, ModelToolContract, ModelUsageContract, ProviderDataHandlingContract,
    ProviderModelIdentity, ProviderRequestLimits, ProviderTrainingPolicy, Retryability,
    SecretPurpose, SecretResolutionPolicy, StructuredOutputContract, ValueRef,
};
use insight_platform_contracts::{
    ModelDeploymentClosure, ModelProfileResourceSpec, ModelProviderDeploymentClosure,
    ModelProviderResourceSpec,
};
use insight_platform_jobs::JobFence;
use insight_platform_models::execution::{
    canonical_request_digest, ExecuteModelAdapterJob, ModelExecutionAuthority,
};
use insight_platform_models::CanonicalModelRequest;
use insight_platform_models::{
    AccountingQuality, CanonicalAssistantMessage, CanonicalFinishReason, CanonicalMessage,
    CanonicalMessagePart, CanonicalMessageRole, ClosedSchemaDocument, ModelContentSource,
    ModelDispatchOutcome, ModelObservation, ModelOutputValue, ModelQuotaCeiling,
    ModelResponseContract, ModelToolProjection, ModelUsage, ModelWorkerAudit, NormalizedModelDelta,
    SafeTraceContext,
};
use serde_json::Value;
use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc, Mutex,
};

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

#[derive(Clone)]
struct Fixture {
    request: ModelAdapterExecutionRequest,
    response: CanonicalModelResponse,
    descriptor: InstalledModelAdapterDescriptor,
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
    let descriptor = InstalledModelAdapterDescriptor::from(&installed_adapter);
    let provider = ModelProviderResourceSpec {
        authoring_package: authoring(8, '7'),
        contract_digest: sha('8'),
        dependency_versions: vec![protocol_policy.clone()],
        policy_versions: vec![protocol_policy.clone()],
        installed_adapter,
        protocol_policy: protocol_policy.clone(),
        credential_requirements: vec!["provider.api_key".parse::<SecretPurpose>().unwrap()],
        request_limits: ProviderRequestLimits {
            maximum_request_bytes: 1_048_576,
            maximum_response_bytes: 1_048_576,
            maximum_messages: 32,
            maximum_parts: 64,
            maximum_tools: 8,
            maximum_parallel_tool_calls: 8,
            maximum_stream_delta_bytes: 262_144,
            connect_timeout_milliseconds: 1_000,
            first_byte_timeout_milliseconds: 2_000,
            idle_timeout_milliseconds: 3_000,
            total_timeout_milliseconds: 30_000,
        },
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
        descriptor,
    }
}

struct StaticAdapter {
    descriptor: InstalledModelAdapterDescriptor,
    response: CanonicalModelResponse,
}

struct FailingAdapter {
    descriptor: InstalledModelAdapterDescriptor,
    failure: ModelAdapterFailure,
}

struct DispatchCountingAdapter {
    descriptor: InstalledModelAdapterDescriptor,
    dispatch_count: Arc<AtomicUsize>,
}

struct PendingAdapter {
    descriptor: InstalledModelAdapterDescriptor,
}

#[async_trait]
impl ModelProviderAdapter for PendingAdapter {
    fn descriptor(&self) -> InstalledModelAdapterDescriptor {
        self.descriptor.clone()
    }

    async fn invoke(
        &self,
        _request: ModelAdapterExecutionRequest,
    ) -> Result<NormalizedModelStream, ModelAdapterFailure> {
        std::future::pending().await
    }

    async fn cancel(
        &self,
        _request: ModelAdapterCancelRequest,
    ) -> Result<ModelAdapterCancelOutcome, ModelAdapterFailure> {
        Ok(ModelAdapterCancelOutcome::Unsupported)
    }
}

#[async_trait]
impl ModelProviderAdapter for DispatchCountingAdapter {
    fn descriptor(&self) -> InstalledModelAdapterDescriptor {
        self.descriptor.clone()
    }

    async fn invoke(
        &self,
        _request: ModelAdapterExecutionRequest,
    ) -> Result<NormalizedModelStream, ModelAdapterFailure> {
        self.dispatch_count.fetch_add(1, Ordering::SeqCst);
        panic!("preflight-rejected execution reached the Provider")
    }

    async fn cancel(
        &self,
        _request: ModelAdapterCancelRequest,
    ) -> Result<ModelAdapterCancelOutcome, ModelAdapterFailure> {
        Ok(ModelAdapterCancelOutcome::Unsupported)
    }
}

#[async_trait]
impl ModelProviderAdapter for FailingAdapter {
    fn descriptor(&self) -> InstalledModelAdapterDescriptor {
        self.descriptor.clone()
    }

    async fn invoke(
        &self,
        _request: ModelAdapterExecutionRequest,
    ) -> Result<NormalizedModelStream, ModelAdapterFailure> {
        Err(self.failure.clone())
    }

    async fn cancel(
        &self,
        _request: ModelAdapterCancelRequest,
    ) -> Result<ModelAdapterCancelOutcome, ModelAdapterFailure> {
        Ok(ModelAdapterCancelOutcome::Unsupported)
    }
}

#[async_trait]
impl ModelProviderAdapter for StaticAdapter {
    fn descriptor(&self) -> InstalledModelAdapterDescriptor {
        self.descriptor.clone()
    }

    async fn invoke(
        &self,
        request: ModelAdapterExecutionRequest,
    ) -> Result<NormalizedModelStream, ModelAdapterFailure> {
        Ok(Box::pin(stream::iter(vec![
            Ok(NormalizedModelFrame {
                model_turn_id: request.model_turn_id.clone(),
                attempt_no: request.attempt_no,
                lease_generation: request.lease_generation,
                transport_sequence: 1,
                delta: NormalizedModelDelta::Text("hello".to_owned()),
            }),
            Ok(NormalizedModelFrame {
                model_turn_id: request.model_turn_id,
                attempt_no: request.attempt_no,
                lease_generation: request.lease_generation,
                transport_sequence: 2,
                delta: NormalizedModelDelta::Terminal(Box::new(self.response.clone())),
            }),
        ])))
    }

    async fn cancel(
        &self,
        _request: ModelAdapterCancelRequest,
    ) -> Result<ModelAdapterCancelOutcome, ModelAdapterFailure> {
        Ok(ModelAdapterCancelOutcome::Accepted)
    }
}

#[derive(Default)]
struct CapturingSink {
    frames: Mutex<Vec<NormalizedModelFrame>>,
    text_sequences: Mutex<Vec<u64>>,
}

#[async_trait]
impl ModelLiveDeltaSink for CapturingSink {
    async fn publish(
        &self,
        execution: &ModelAdapterExecutionRequest,
        frame: &NormalizedModelFrame,
        text_sequence: u64,
    ) {
        assert_eq!(execution.model_turn_id, frame.model_turn_id);
        self.frames.lock().unwrap().push(frame.clone());
        self.text_sequences.lock().unwrap().push(text_sequence);
    }
}

#[tokio::test]
async fn two_exact_provider_adapters_share_one_conformance_boundary() {
    let first = fixture("fixture.responses/v1", '9', 'a');
    let second = fixture("fixture.messages/v1", '8', 'b');
    let mut registry = InstalledModelAdapterRegistry::default();
    for fixture in [&first, &second] {
        registry
            .install(Arc::new(StaticAdapter {
                descriptor: fixture.descriptor.clone(),
                response: fixture.response.clone(),
            }))
            .unwrap();
    }
    let sink = Arc::new(CapturingSink::default());
    let host = ModelAdapterHost::new(registry, sink.clone(), limits());
    for fixture in [first, second] {
        let outcome = host.execute(fixture.request).await.unwrap();
        let ModelAdapterExecutionOutcome::Succeeded(success) = outcome else {
            panic!("adapter did not return a normalized terminal response")
        };
        assert_eq!(success.stream_evidence.accepted_delta_count, 1);
    }
    assert_eq!(sink.frames.lock().unwrap().len(), 2);
}

#[tokio::test]
async fn compatible_rebuilt_worker_uses_frozen_adapter_semantics() {
    let mut fixture = fixture("fixture.responses/v1", '9', 'a');
    let mut descriptor = fixture.descriptor;
    descriptor.worker_manifest_digest = sha('0');
    let mut registry = InstalledModelAdapterRegistry::default();
    registry
        .install(Arc::new(StaticAdapter {
            descriptor,
            response: fixture.response,
        }))
        .unwrap();
    fixture.request.worker_manifest_digest = sha('0');
    let host = ModelAdapterHost::new(registry, Arc::new(DropModelLiveDeltas), limits());
    assert!(host.execute(fixture.request).await.is_ok());
}

#[tokio::test]
async fn changed_adapter_contract_fails_before_provider_dispatch() {
    let original = fixture("fixture.responses/v1", '9', 'a');
    let changed = fixture("fixture.responses/v1", '0', 'b');
    let mut registry = InstalledModelAdapterRegistry::default();
    registry
        .install(Arc::new(StaticAdapter {
            descriptor: original.descriptor,
            response: original.response,
        }))
        .unwrap();
    let host = ModelAdapterHost::new(registry, Arc::new(DropModelLiveDeltas), limits());
    assert_eq!(
        host.execute(changed.request).await,
        Err(ModelAdapterHostError::AdapterNotInstalled)
    );
}

#[tokio::test]
async fn cancel_is_bound_to_the_same_exact_attempt_and_provider_deployment() {
    let fixture = fixture("fixture.responses/v1", '9', 'a');
    let mut registry = InstalledModelAdapterRegistry::default();
    registry
        .install(Arc::new(StaticAdapter {
            descriptor: fixture.descriptor,
            response: fixture.response,
        }))
        .unwrap();
    let host = ModelAdapterHost::new(registry, Arc::new(DropModelLiveDeltas), limits());
    let cancel = ModelAdapterCancelRequest {
        tenant_id: fixture.request.tenant_id.clone(),
        model_turn_id: fixture.request.model_turn_id.clone(),
        job_id: fixture.request.job_id.clone(),
        worker_process_generation_id: fixture.request.worker_process_generation_id.clone(),
        provider_deployment: fixture.request.provider_deployment.clone(),
        attempt_no: fixture.request.attempt_no,
        lease_generation: fixture.request.lease_generation,
        deadline: Utc::now() + ChronoDuration::seconds(5),
    };
    assert_eq!(
        host.cancel(&fixture.request, cancel.clone()).await.unwrap(),
        ModelAdapterCancelExecutionOutcome::Completed(ModelAdapterCancelOutcome::Accepted)
    );
    let mut stale = cancel;
    stale.lease_generation += 1;
    assert_eq!(
        host.cancel(&fixture.request, stale).await,
        Err(ModelAdapterHostError::InvalidCancelRequest)
    );

    let stale_worker = ModelAdapterCancelRequest {
        tenant_id: fixture.request.tenant_id.clone(),
        model_turn_id: fixture.request.model_turn_id.clone(),
        job_id: fixture.request.job_id.clone(),
        worker_process_generation_id: id(ResourceKind::WorkerProcessGeneration, 24),
        provider_deployment: fixture.request.provider_deployment.clone(),
        attempt_no: fixture.request.attempt_no,
        lease_generation: fixture.request.lease_generation,
        deadline: Utc::now() + ChronoDuration::seconds(5),
    };
    assert_eq!(
        host.cancel(&fixture.request, stale_worker).await,
        Err(ModelAdapterHostError::InvalidCancelRequest)
    );
}

#[tokio::test]
async fn provider_delta_limit_is_enforced_before_live_projection() {
    let mut fixture = fixture("fixture.responses/v1", '9', 'a');
    fixture
        .request
        .provider
        .request_limits
        .maximum_stream_delta_bytes = 1;
    let mut registry = InstalledModelAdapterRegistry::default();
    registry
        .install(Arc::new(StaticAdapter {
            descriptor: fixture.descriptor,
            response: fixture.response,
        }))
        .unwrap();
    let sink = Arc::new(CapturingSink::default());
    let host = ModelAdapterHost::new(registry, sink.clone(), limits());
    assert_eq!(
        host.execute(fixture.request).await,
        Err(ModelAdapterHostError::InvalidNormalizedStream)
    );
    assert!(sink.frames.lock().unwrap().is_empty());
}

#[test]
fn adapter_failure_cannot_hide_dispatch_or_retry_state() {
    let fixture = fixture("fixture.responses/v1", '9', 'a');
    let failure = ModelAdapterFailure {
        class: ModelAdapterFailureClass::RetryableBeforeDispatch,
        safe_code: "rate_limited".to_owned(),
        safe_message: "Provider capacity is unavailable".to_owned(),
        evidence_digest: sha('1'),
        request_sent: true,
        retry_at: Some(Utc::now() + ChronoDuration::seconds(1)),
    };
    assert_eq!(
        failure.validate_for(&fixture.request, Utc::now()),
        Err(ModelAdapterHostError::InvalidAdapterFailure)
    );
}

#[test]
fn internally_generated_retry_failure_has_a_worker_handoff_window() {
    let fixture = fixture("fixture.responses/v1", '9', 'a');
    let generated_not_before = Utc::now();
    let failure =
        ModelAdapterFailure::retryable_after_dispatch("model_connect_timeout", &fixture.request);
    let retry_at = failure
        .retry_at
        .expect("a timeout before the Run deadline remains retryable");

    assert!(retry_at >= generated_not_before + ChronoDuration::milliseconds(250));
    failure
        .validate_for(&fixture.request, Utc::now())
        .expect("the Host-to-Worker handoff must not expire its own retry instruction");
}

struct InlineMaterializer;

#[async_trait]
impl ModelOutputMaterializer for InlineMaterializer {
    fn validate_execution(
        &self,
        _execution: &ModelAdapterExecutionRequest,
    ) -> Result<(), ModelAdapterFailure> {
        Ok(())
    }

    async fn materialize(
        &self,
        execution: &ModelAdapterExecutionRequest,
        success: ModelAdapterSuccess,
    ) -> Result<ModelOutputValue, ModelAdapterFailure> {
        let value = serde_json::to_value(&success.response).unwrap();
        let content_digest: Sha256Digest = canonical_digest(&value).unwrap().parse().unwrap();
        let structured_output_value_id = success
            .response
            .structured_output
            .as_ref()
            .map(|_| id(ResourceKind::RunValue, 41));
        Ok(ModelOutputValue {
            value_id: id(ResourceKind::RunValue, 40),
            structured_output_value_id,
            classification: execution.request.classification,
            schema_digest: execution
                .request
                .response_contract
                .output_schema_digest
                .clone(),
            content_digest,
            value: ValueRef::Inline { value },
            response: *success.response,
            validation_evidence_digest: sha('5'),
        })
    }
}

struct RejectingMaterializer;

#[async_trait]
impl ModelOutputMaterializer for RejectingMaterializer {
    fn validate_execution(
        &self,
        _execution: &ModelAdapterExecutionRequest,
    ) -> Result<(), ModelAdapterFailure> {
        Err(ModelAdapterFailure {
            class: ModelAdapterFailureClass::RejectedBeforeDispatch,
            safe_code: "model_output_too_large".to_owned(),
            safe_message: "Output requires Artifact materialization".to_owned(),
            evidence_digest: sha('8'),
            request_sent: false,
            retry_at: None,
        })
    }

    async fn materialize(
        &self,
        _execution: &ModelAdapterExecutionRequest,
        _success: ModelAdapterSuccess,
    ) -> Result<ModelOutputValue, ModelAdapterFailure> {
        panic!("preflight-rejected execution reached output materialization")
    }
}

struct StaticValidationFailureMaterializer {
    failure: ModelAdapterFailure,
}

#[async_trait]
impl ModelOutputMaterializer for StaticValidationFailureMaterializer {
    fn validate_execution(
        &self,
        _execution: &ModelAdapterExecutionRequest,
    ) -> Result<(), ModelAdapterFailure> {
        Err(self.failure.clone())
    }

    async fn materialize(
        &self,
        _execution: &ModelAdapterExecutionRequest,
        _success: ModelAdapterSuccess,
    ) -> Result<ModelOutputValue, ModelAdapterFailure> {
        panic!("failed execution validation reached output materialization")
    }
}

#[derive(Clone)]
struct CapturingAuthority {
    outcome: Arc<Mutex<Option<ModelDispatchOutcome>>>,
    fence: Arc<Mutex<Option<JobFence>>>,
    terminal_mutations: Arc<Mutex<Option<(bool, bool)>>>,
}

#[async_trait]
impl ModelExecutionAuthority for CapturingAuthority {
    type Error = String;
    type Record = String;

    async fn commit_model_outcome(
        &self,
        command: insight_platform_models::CommitModelOutcome,
    ) -> Result<CommandOutcome<Self::Record>, Self::Error> {
        *self.fence.lock().unwrap() = Some(command.fence.clone());
        *self.terminal_mutations.lock().unwrap() = Some((
            command.resume_mutations.is_some(),
            command.failure_mutations.is_some(),
        ));
        *self.outcome.lock().unwrap() = Some(command.outcome);
        Ok(CommandOutcome::Applied("committed".to_owned()))
    }
}

fn worker_command(execution: ModelAdapterExecutionRequest) -> ExecuteModelAdapterJob {
    let now = Utc::now();
    let worker_process_generation_id = execution.worker_process_generation_id.clone();
    ExecuteModelAdapterJob {
        audit: ModelWorkerAudit {
            tenant_id: execution.tenant_id.clone(),
            worker_process_generation_id: worker_process_generation_id.clone(),
            receipt_id: id(ResourceKind::Receipt, 41),
            event_id: id(ResourceKind::Event, 42),
            outbox_id: id(ResourceKind::OutboxEvent, 43),
            idempotency_key_digest: sha('6'),
            request_digest: execution.request_digest.clone(),
            receipt_expires_at: now + ChronoDuration::minutes(5),
        },
        expected_turn_version: 3,
        fence: JobFence {
            expected_version: 4,
            worker_process_generation_id,
            lease_generation: execution.lease_generation,
            token_digest: sha('7'),
        },
        usage_reservation_id: id(ResourceKind::UsageReservation, 44),
        resume_mutations: Some(ExternalLeafResumeMutationIds {
            continuation_job_id: id(ResourceKind::Job, 51),
            run_event_id: id(ResourceKind::Event, 52),
            run_outbox_id: id(ResourceKind::OutboxEvent, 53),
            leaf_node_event_id: id(ResourceKind::Event, 54),
            leaf_node_outbox_id: id(ResourceKind::OutboxEvent, 55),

            continuation_job_event_id: id(ResourceKind::Event, 58),
            continuation_job_outbox_id: id(ResourceKind::OutboxEvent, 59),
        }),
        failure_mutations: Some(ExternalLeafFailureMutationIds {
            convergence_job_id: id(ResourceKind::Job, 60),
            run_event_id: id(ResourceKind::Event, 61),
            run_outbox_id: id(ResourceKind::OutboxEvent, 62),
            leaf_node_event_id: id(ResourceKind::Event, 63),
            leaf_node_outbox_id: id(ResourceKind::OutboxEvent, 64),
            convergence_job_event_id: id(ResourceKind::Event, 65),
            convergence_job_outbox_id: id(ResourceKind::OutboxEvent, 66),
        }),
        tool_continuation_mutations: Some(
            insight_platform_models::ModelToolContinuationMutationIds {
                continuation_job_id: id(ResourceKind::Job, 67),
                run_event_id: id(ResourceKind::Event, 68),
                run_outbox_id: id(ResourceKind::OutboxEvent, 69),
                node_event_id: id(ResourceKind::Event, 70),
                node_outbox_id: id(ResourceKind::OutboxEvent, 71),
                continuation_job_event_id: id(ResourceKind::Event, 72),
                continuation_job_outbox_id: id(ResourceKind::OutboxEvent, 73),
            },
        ),
        quota_entry_ids: (45..49)
            .map(|suffix| id(ResourceKind::QuotaLedgerEntry, suffix))
            .collect(),
        execution,
    }
}

#[tokio::test]
async fn worker_materializes_and_commits_one_fenced_terminal_outcome() {
    let fixture = fixture("fixture.responses/v1", '9', 'a');
    let mut registry = InstalledModelAdapterRegistry::default();
    registry
        .install(Arc::new(StaticAdapter {
            descriptor: fixture.descriptor,
            response: fixture.response,
        }))
        .unwrap();
    let authority = CapturingAuthority {
        outcome: Arc::new(Mutex::new(None)),
        fence: Arc::new(Mutex::new(None)),
        terminal_mutations: Arc::new(Mutex::new(None)),
    };
    let worker = ModelAdapterWorker::new(
        Arc::new(ModelAdapterHost::new(
            registry,
            Arc::new(DropModelLiveDeltas),
            limits(),
        )),
        InlineMaterializer,
        authority.clone(),
    );
    let result = worker
        .execute(worker_command(fixture.request))
        .await
        .unwrap();
    assert_eq!(result, CommandOutcome::Applied("committed".to_owned()));
    assert!(matches!(
        authority.outcome.lock().unwrap().as_ref(),
        Some(ModelDispatchOutcome::Succeeded(_))
    ));
    assert_eq!(
        *authority.terminal_mutations.lock().unwrap(),
        Some((true, false))
    );
}

#[tokio::test]
async fn output_capacity_rejection_is_committed_without_provider_dispatch() {
    let fixture = fixture("fixture.responses/v1", '9', 'a');
    let dispatch_count = Arc::new(AtomicUsize::new(0));
    let mut registry = InstalledModelAdapterRegistry::default();
    registry
        .install(Arc::new(DispatchCountingAdapter {
            descriptor: fixture.descriptor,
            dispatch_count: dispatch_count.clone(),
        }))
        .unwrap();
    let authority = CapturingAuthority {
        outcome: Arc::new(Mutex::new(None)),
        fence: Arc::new(Mutex::new(None)),
        terminal_mutations: Arc::new(Mutex::new(None)),
    };
    let worker = ModelAdapterWorker::new(
        Arc::new(ModelAdapterHost::new(
            registry,
            Arc::new(DropModelLiveDeltas),
            limits(),
        )),
        RejectingMaterializer,
        authority.clone(),
    );

    worker
        .execute(worker_command(fixture.request))
        .await
        .unwrap();

    assert_eq!(dispatch_count.load(Ordering::SeqCst), 0);
    let outcome = authority.outcome.lock().unwrap();
    let ModelDispatchOutcome::PermanentFailure {
        failure,
        measurement,
    } = outcome.as_ref().unwrap()
    else {
        panic!("preflight rejection was not committed as a permanent failure")
    };
    assert_eq!(failure.safe_code, "model_output_too_large");
    assert!(!measurement.observation.request_sent);
    assert!(measurement.usage.is_none());
    assert_eq!(
        *authority.terminal_mutations.lock().unwrap(),
        Some((false, true))
    );
}

#[tokio::test]
async fn prepared_outcome_accepts_only_same_lease_heartbeat_fence() {
    let fixture = fixture("fixture.responses/v1", '9', 'a');
    let mut registry = InstalledModelAdapterRegistry::default();
    registry
        .install(Arc::new(StaticAdapter {
            descriptor: fixture.descriptor,
            response: fixture.response,
        }))
        .unwrap();
    let authority = CapturingAuthority {
        outcome: Arc::new(Mutex::new(None)),
        fence: Arc::new(Mutex::new(None)),
        terminal_mutations: Arc::new(Mutex::new(None)),
    };
    let worker = ModelAdapterWorker::new(
        Arc::new(ModelAdapterHost::new(
            registry,
            Arc::new(DropModelLiveDeltas),
            limits(),
        )),
        InlineMaterializer,
        authority.clone(),
    );
    let command = worker_command(fixture.request);
    let mut prepared = worker.prepare(command.clone()).await.unwrap();
    assert!(authority.outcome.lock().unwrap().is_none());

    let mut wrong_generation = command.fence.clone();
    wrong_generation.worker_process_generation_id = id(ResourceKind::WorkerProcessGeneration, 99);
    assert_eq!(
        prepared.refresh_fence(wrong_generation),
        Err(ModelAdapterWorkerContractError::InvalidCommand)
    );

    let mut heartbeat_fence = command.fence;
    heartbeat_fence.expected_version += 1;
    prepared.refresh_fence(heartbeat_fence.clone()).unwrap();
    worker.commit(prepared).await.unwrap();
    assert_eq!(
        authority.fence.lock().unwrap().as_ref(),
        Some(&heartbeat_fence)
    );
}

#[tokio::test]
async fn dispatched_failure_is_conservatively_accounted_and_attempt_bounded() {
    for (attempt_limit, retry_expected) in [(3, true), (1, false)] {
        let mut fixture = fixture("fixture.responses/v1", '9', 'a');
        fixture.request.attempt_limit = attempt_limit;
        fixture.request.profile.usage.reports_cost = true;
        fixture.request.profile.usage.cost_currency = Some("USD".to_owned());
        let failure = ModelAdapterFailure {
            class: ModelAdapterFailureClass::RetryableAfterDispatch,
            safe_code: "provider_stream_lost".to_owned(),
            safe_message: "Provider stream completion was not observed".to_owned(),
            evidence_digest: sha('8'),
            request_sent: true,
            retry_at: Some(Utc::now() + ChronoDuration::seconds(1)),
        };
        let mut registry = InstalledModelAdapterRegistry::default();
        registry
            .install(Arc::new(FailingAdapter {
                descriptor: fixture.descriptor,
                failure,
            }))
            .unwrap();
        let authority = CapturingAuthority {
            outcome: Arc::new(Mutex::new(None)),
            fence: Arc::new(Mutex::new(None)),
            terminal_mutations: Arc::new(Mutex::new(None)),
        };
        let worker = ModelAdapterWorker::new(
            Arc::new(ModelAdapterHost::new(
                registry,
                Arc::new(DropModelLiveDeltas),
                limits(),
            )),
            InlineMaterializer,
            authority.clone(),
        );
        worker
            .execute(worker_command(fixture.request))
            .await
            .unwrap();
        let outcome = authority.outcome.lock().unwrap();
        let (retryable, measurement) = match outcome.as_ref().unwrap() {
            ModelDispatchOutcome::RetryableFailure { measurement, .. } => (true, measurement),
            ModelDispatchOutcome::PermanentFailure { measurement, .. } => (false, measurement),
            _ => panic!("failure was not mapped to a terminal or retry outcome"),
        };
        assert_eq!(retryable, retry_expected);
        let usage = measurement.usage.as_ref().unwrap();
        assert_eq!(usage.accounting_quality, AccountingQuality::Reconciled);
        assert_eq!(usage.input_tokens, Some(100));
        assert_eq!(usage.output_tokens, Some(100));
        assert_eq!(
            usage.provider_reported_cost.as_ref().unwrap().minor_units(),
            10_000
        );
        assert!(measurement.observation.possible_duplicate_charge);
    }
}

#[tokio::test]
async fn expired_retry_instruction_on_final_attempt_is_rejected_as_stale() {
    let mut fixture = fixture("fixture.responses/v1", '9', 'a');
    fixture.request.attempt_limit = 1;
    let evidence_digest = sha('8');
    let failure = ModelAdapterFailure {
        class: ModelAdapterFailureClass::RetryableAfterDispatch,
        safe_code: "model_connect_timeout".to_owned(),
        safe_message: "Model Provider completion could not be observed".to_owned(),
        evidence_digest: evidence_digest.clone(),
        request_sent: true,
        retry_at: Some(Utc::now() - ChronoDuration::milliseconds(1)),
    };
    let dispatch_count = Arc::new(AtomicUsize::new(0));
    let mut registry = InstalledModelAdapterRegistry::default();
    registry
        .install(Arc::new(DispatchCountingAdapter {
            descriptor: fixture.descriptor,
            dispatch_count: dispatch_count.clone(),
        }))
        .unwrap();
    let authority = CapturingAuthority {
        outcome: Arc::new(Mutex::new(None)),
        fence: Arc::new(Mutex::new(None)),
        terminal_mutations: Arc::new(Mutex::new(None)),
    };
    let worker = ModelAdapterWorker::new(
        Arc::new(ModelAdapterHost::new(
            registry,
            Arc::new(DropModelLiveDeltas),
            limits(),
        )),
        StaticValidationFailureMaterializer { failure },
        authority.clone(),
    );

    let result = worker.execute(worker_command(fixture.request)).await;

    assert!(matches!(
        result,
        Err(ModelAdapterWorkerError::Contract(
            ModelAdapterWorkerContractError::InvalidFailure
        ))
    ));
    assert_eq!(dispatch_count.load(Ordering::SeqCst), 0);
    assert!(authority.outcome.lock().unwrap().is_none());
}

#[tokio::test]
async fn final_attempt_connect_timeout_is_committed_as_permanent() {
    let mut fixture = fixture("fixture.responses/v1", '9', 'a');
    fixture.request.attempt_limit = 1;
    fixture
        .request
        .provider
        .request_limits
        .connect_timeout_milliseconds = 1;
    let mut registry = InstalledModelAdapterRegistry::default();
    registry
        .install(Arc::new(PendingAdapter {
            descriptor: fixture.descriptor,
        }))
        .unwrap();
    let authority = CapturingAuthority {
        outcome: Arc::new(Mutex::new(None)),
        fence: Arc::new(Mutex::new(None)),
        terminal_mutations: Arc::new(Mutex::new(None)),
    };
    let worker = ModelAdapterWorker::new(
        Arc::new(ModelAdapterHost::new(
            registry,
            Arc::new(DropModelLiveDeltas),
            limits(),
        )),
        InlineMaterializer,
        authority.clone(),
    );

    worker
        .execute(worker_command(fixture.request))
        .await
        .unwrap();

    let outcome = authority.outcome.lock().unwrap();
    let ModelDispatchOutcome::PermanentFailure {
        failure,
        measurement,
    } = outcome.as_ref().unwrap()
    else {
        panic!("final-attempt connect timeout was not committed as permanent")
    };
    assert_eq!(failure.failure.retryability, Retryability::Never);
    assert_eq!(failure.safe_code, "model_connect_timeout");
    assert!(measurement.observation.request_sent);
    assert!(measurement.observation.possible_duplicate_charge);
}

#[tokio::test]
async fn final_attempt_still_rejects_a_retry_instruction_outside_the_deadline() {
    let mut fixture = fixture("fixture.responses/v1", '9', 'a');
    fixture.request.attempt_limit = 1;
    let failure = ModelAdapterFailure {
        class: ModelAdapterFailureClass::RetryableAfterDispatch,
        safe_code: "model_connect_timeout".to_owned(),
        safe_message: "Model Provider completion could not be observed".to_owned(),
        evidence_digest: sha('8'),
        request_sent: true,
        retry_at: Some(fixture.request.request.deadline),
    };
    let mut registry = InstalledModelAdapterRegistry::default();
    registry
        .install(Arc::new(DispatchCountingAdapter {
            descriptor: fixture.descriptor,
            dispatch_count: Arc::new(AtomicUsize::new(0)),
        }))
        .unwrap();
    let authority = CapturingAuthority {
        outcome: Arc::new(Mutex::new(None)),
        fence: Arc::new(Mutex::new(None)),
        terminal_mutations: Arc::new(Mutex::new(None)),
    };
    let worker = ModelAdapterWorker::new(
        Arc::new(ModelAdapterHost::new(
            registry,
            Arc::new(DropModelLiveDeltas),
            limits(),
        )),
        StaticValidationFailureMaterializer { failure },
        authority.clone(),
    );

    let result = worker.execute(worker_command(fixture.request)).await;

    assert!(matches!(
        result,
        Err(ModelAdapterWorkerError::Contract(
            ModelAdapterWorkerContractError::InvalidFailure
        ))
    ));
    assert!(authority.outcome.lock().unwrap().is_none());
}

struct FixtureWireConnector {
    request: Mutex<Option<ModelProviderWireRequest>>,
    events: Mutex<Option<Vec<Result<ModelProviderWireEvent, ModelAdapterFailure>>>>,
    calls: AtomicUsize,
}

impl FixtureWireConnector {
    fn new(events: Vec<ModelProviderWireEvent>) -> Self {
        Self {
            request: Mutex::new(None),
            events: Mutex::new(Some(events.into_iter().map(Ok).collect())),
            calls: AtomicUsize::new(0),
        }
    }

    fn take_request(&self) -> ModelProviderWireRequest {
        self.request.lock().unwrap().take().unwrap()
    }
}

#[test]
fn physical_adapter_constructor_rejects_unrecognized_contract_digest_before_io() {
    for protocol in [
        ModelProviderWireProtocol::OpenAiResponses,
        ModelProviderWireProtocol::AnthropicMessages,
    ] {
        let fixture = wire_fixture(protocol.qualified_name());
        let connector = Arc::new(FixtureWireConnector::new(vec![]));
        let mut descriptor = fixture.descriptor;
        descriptor.adapter_contract_digest = sha('f');
        let rejected = match protocol {
            ModelProviderWireProtocol::OpenAiResponses => {
                OpenAiResponsesAdapter::new(descriptor, connector.clone()).is_err()
            }
            ModelProviderWireProtocol::AnthropicMessages => {
                AnthropicMessagesAdapter::new(descriptor, connector.clone()).is_err()
            }
        };
        assert!(rejected);
        assert_eq!(connector.calls.load(Ordering::SeqCst), 0);
    }
}

#[test]
fn protocol_mapping_identities_bind_their_current_semantics_without_changing_the_wire_abi() {
    for (protocol, name, path, version) in [
        (
            ModelProviderWireProtocol::OpenAiResponses,
            "openai.responses/v1",
            "/v1/responses",
            "responses-v1",
        ),
        (
            ModelProviderWireProtocol::AnthropicMessages,
            "anthropic.messages/2023-06-01",
            "/v1/messages",
            "2023-06-01",
        ),
    ] {
        let mut declaration = serde_json::json!({
            "schema_version": 1,
            "kind": "insight.model-provider-adapter-contract/v1",
            "protocol": protocol,
            "qualified_name": name,
            "canonical_request_abi": 1,
            "canonical_response_abi": 1,
            "normalized_stream_abi": 1,
            "provider_wire_request_abi": 2,
            "endpoint_path": path,
            "protocol_version": version,
            "wire_mapping_semantics": match protocol {
                ModelProviderWireProtocol::OpenAiResponses => 4,
                ModelProviderWireProtocol::AnthropicMessages => 2,
            },
        });
        assert_eq!(
            canonical_digest(&declaration).unwrap(),
            protocol.adapter_contract_digest().as_str()
        );
        declaration["wire_mapping_semantics"] = serde_json::json!(match protocol {
            ModelProviderWireProtocol::OpenAiResponses => 3,
            ModelProviderWireProtocol::AnthropicMessages => 1,
        });
        let old_digest: Sha256Digest = canonical_digest(&declaration).unwrap().parse().unwrap();
        assert_ne!(old_digest, protocol.adapter_contract_digest());
        let fixture = wire_fixture(protocol.qualified_name());
        let connector = Arc::new(FixtureWireConnector::new(vec![]));
        let mut descriptor = fixture.descriptor;
        descriptor.adapter_contract_digest = old_digest;
        let rejected = match protocol {
            ModelProviderWireProtocol::OpenAiResponses => {
                OpenAiResponsesAdapter::new(descriptor, connector.clone()).is_err()
            }
            ModelProviderWireProtocol::AnthropicMessages => {
                AnthropicMessagesAdapter::new(descriptor, connector.clone()).is_err()
            }
        };
        assert!(rejected);
        assert_eq!(connector.calls.load(Ordering::SeqCst), 0);
    }
}

#[async_trait]
impl ModelProviderWireConnector for FixtureWireConnector {
    async fn open(
        &self,
        request: ModelProviderWireRequest,
    ) -> Result<ModelProviderWireStream, ModelAdapterFailure> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        let mut captured = self.request.lock().unwrap();
        if captured.replace(request).is_some() {
            return Err(rejected("fixture_duplicate_wire_request"));
        }
        let events = self
            .events
            .lock()
            .unwrap()
            .take()
            .ok_or_else(|| rejected("fixture_missing_wire_stream"))?;
        Ok(stream::iter(events).boxed())
    }

    async fn cancel(
        &self,
        _protocol: ModelProviderWireProtocol,
        _request: ModelAdapterCancelRequest,
    ) -> Result<ModelAdapterCancelOutcome, ModelAdapterFailure> {
        Ok(ModelAdapterCancelOutcome::Unsupported)
    }
}

struct FixtureEgressBroker {
    calls: AtomicUsize,
    request: Mutex<Option<ModelProviderWireRequest>>,
    status_code: u16,
    content_type: String,
    chunks: Mutex<Option<Vec<Vec<u8>>>>,
}

impl FixtureEgressBroker {
    fn from_events(events: Vec<ModelProviderWireEvent>) -> Self {
        let mut encoded = Vec::new();
        for event in events {
            encoded.extend_from_slice(b"event: ");
            encoded.extend_from_slice(event.event_name.as_bytes());
            encoded.extend_from_slice(b"\ndata: ");
            encoded.extend_from_slice(&serde_json::to_vec(&event.data).unwrap());
            encoded.extend_from_slice(b"\n\n");
        }
        let split = encoded.len() / 2;
        Self::raw(
            200,
            "text/event-stream; charset=utf-8",
            vec![encoded[..split].to_vec(), encoded[split..].to_vec()],
        )
    }

    fn raw(status_code: u16, content_type: &str, chunks: Vec<Vec<u8>>) -> Self {
        Self {
            calls: AtomicUsize::new(0),
            request: Mutex::new(None),
            status_code,
            content_type: content_type.to_owned(),
            chunks: Mutex::new(Some(chunks)),
        }
    }
}

#[async_trait]
impl ModelProviderEgressBroker for FixtureEgressBroker {
    async fn open(
        &self,
        request: ModelProviderWireRequest,
    ) -> Result<ModelProviderEgressResponse, ModelAdapterFailure> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        let mut captured = self.request.lock().unwrap();
        if captured.replace(request).is_some() {
            return Err(rejected("fixture_duplicate_egress_request"));
        }
        let chunks = self
            .chunks
            .lock()
            .unwrap()
            .take()
            .ok_or_else(|| rejected("fixture_missing_egress_stream"))?;
        Ok(ModelProviderEgressResponse {
            status_code: self.status_code,
            content_type: self.content_type.clone(),
            body: stream::iter(chunks.into_iter().map(Ok)).boxed(),
        })
    }

    async fn cancel(
        &self,
        _protocol: ModelProviderWireProtocol,
        _request: ModelAdapterCancelRequest,
    ) -> Result<ModelAdapterCancelOutcome, ModelAdapterFailure> {
        Ok(ModelAdapterCancelOutcome::Unsupported)
    }
}

fn event(event_name: &str, data: Value) -> ModelProviderWireEvent {
    assert_eq!(data.get("type").and_then(Value::as_str), Some(event_name));
    ModelProviderWireEvent {
        event_name: event_name.to_owned(),
        data,
    }
}

fn wire_fixture(adapter_name: &str) -> Fixture {
    let mut fixture = fixture(adapter_name, '9', 'a');
    fixture.request.request.messages.push(CanonicalMessage {
        role: CanonicalMessageRole::User,
        parts: vec![CanonicalMessagePart::Text("Say hello.".to_owned())],
        classification: DataClassification::Internal,
        source: ModelContentSource {
            source_kind: "user_input".to_owned(),
            source_id: "input-fixture".to_owned(),
            source_digest: sha('3'),
            content_digest: sha('3'),
            assembly_phase: insight_platform_models::PromptAssemblyPhase::UserInput,
            ordinal: 0,
            byte_budget: 1_024,
            token_budget: 256,
            trusted_instruction: false,
        },
    });
    fixture.request.request_digest = canonical_request_digest(&fixture.request.request).unwrap();
    fixture
}

fn enable_tool(fixture: &mut Fixture) {
    let schema = ClosedSchemaDocument::build(serde_json::json!({
        "$schema": "https://json-schema.org/draft/2020-12/schema",
        "type": "object",
        "properties": {"q": {
            "description": "Bounded fixture field.",
            "x-platform-classification": "internal",
            "type": "string",
            "minLength": 1,
            "maxLength": 256,
            "x-platform-max-bytes": 1_024
        }},
        "required": ["q"],
        "additionalProperties": false
    }))
    .unwrap();
    fixture.request.profile.tools = ModelToolContract {
        supported: true,
        parallel: false,
        maximum_tools: 1,
        maximum_calls_per_turn: 1,
        maximum_argument_bytes: 4_096,
    };
    fixture.request.profile.limits.maximum_tools = 1;
    fixture.request.profile.limits.maximum_parallel_tool_calls = 1;
    fixture.request.request.tools = vec![ModelToolProjection {
        projected_name: "lookup".to_owned(),
        capability_deployment: deployment(ResourceKind::CapabilityDeployment, 70, '1'),
        interface_revision: version(ResourceKind::CapabilityInterfaceRevision, 71, '2'),
        input_schema: schema,
        output_schema_digest: sha('3'),
        effect: Effect::ReadOnly,
    }];
    fixture.request.request.response_contract.allow_tool_intents = true;
    fixture.request.request_digest = canonical_request_digest(&fixture.request.request).unwrap();
}

fn enable_structured_output(fixture: &mut Fixture) {
    fixture.request.request.response_contract.structured_schema = Some(
        ClosedSchemaDocument::build(serde_json::json!({
            "$schema": "https://json-schema.org/draft/2020-12/schema",
            "type": "object",
            "properties": {"answer": {
                "description": "Bounded fixture field.",
                "x-platform-classification": "internal",
                "type": "string",
                "minLength": 1,
                "maxLength": 256,
                "x-platform-max-bytes": 1_024
            }},
            "required": ["answer"],
            "additionalProperties": false
        }))
        .unwrap(),
    );
    fixture
        .request
        .request
        .response_contract
        .output_schema_digest = fixture
        .request
        .request
        .response_contract
        .structured_schema
        .as_ref()
        .unwrap()
        .canonical_digest
        .clone();
    fixture.request.request_digest = canonical_request_digest(&fixture.request.request).unwrap();
}

async fn execute_wire_fixture(
    fixture: Fixture,
    events: Vec<ModelProviderWireEvent>,
) -> (ModelAdapterExecutionOutcome, ModelProviderWireRequest) {
    let (outcome, connector) = execute_wire_fixture_result(fixture, events).await;
    (outcome.unwrap(), connector.take_request())
}

async fn execute_wire_fixture_result(
    fixture: Fixture,
    events: Vec<ModelProviderWireEvent>,
) -> (
    Result<ModelAdapterExecutionOutcome, ModelAdapterHostError>,
    Arc<FixtureWireConnector>,
) {
    let connector = Arc::new(FixtureWireConnector::new(events));
    let adapter: Arc<dyn ModelProviderAdapter> = match fixture.descriptor.qualified_name.as_str() {
        OPENAI_RESPONSES_ADAPTER_NAME => Arc::new(
            OpenAiResponsesAdapter::new(fixture.descriptor.clone(), connector.clone()).unwrap(),
        ),
        ANTHROPIC_MESSAGES_ADAPTER_NAME => Arc::new(
            AnthropicMessagesAdapter::new(fixture.descriptor.clone(), connector.clone()).unwrap(),
        ),
        _ => panic!("unsupported wire fixture adapter"),
    };
    let mut registry = InstalledModelAdapterRegistry::default();
    registry.install(adapter).unwrap();
    let outcome = ModelAdapterHost::new(registry, Arc::new(DropModelLiveDeltas), limits())
        .execute(fixture.request)
        .await;
    (outcome, connector)
}

async fn execute_brokered_fixture(
    fixture: Fixture,
    broker: Arc<FixtureEgressBroker>,
) -> ModelAdapterExecutionOutcome {
    execute_brokered_fixture_result(fixture, broker)
        .await
        .unwrap()
}

async fn execute_brokered_fixture_result(
    fixture: Fixture,
    broker: Arc<FixtureEgressBroker>,
) -> Result<ModelAdapterExecutionOutcome, ModelAdapterHostError> {
    let connector: Arc<dyn ModelProviderWireConnector> =
        Arc::new(BrokeredModelProviderWireConnector::new(broker));
    let adapter: Arc<dyn ModelProviderAdapter> = match fixture.descriptor.qualified_name.as_str() {
        OPENAI_RESPONSES_ADAPTER_NAME => {
            Arc::new(OpenAiResponsesAdapter::new(fixture.descriptor.clone(), connector).unwrap())
        }
        ANTHROPIC_MESSAGES_ADAPTER_NAME => {
            Arc::new(AnthropicMessagesAdapter::new(fixture.descriptor.clone(), connector).unwrap())
        }
        _ => panic!("unsupported brokered fixture adapter"),
    };
    let mut registry = InstalledModelAdapterRegistry::default();
    registry.install(adapter).unwrap();
    ModelAdapterHost::new(registry, Arc::new(DropModelLiveDeltas), limits())
        .execute(fixture.request)
        .await
}

fn openai_text_events(text: &str) -> Vec<ModelProviderWireEvent> {
    vec![
        event(
            "response.created",
            serde_json::json!({"type": "response.created", "response": {}}),
        ),
        event(
            "response.output_text.delta",
            serde_json::json!({"type": "response.output_text.delta", "delta": text}),
        ),
        event(
            "response.completed",
            serde_json::json!({
                "type": "response.completed",
                "response": {
                    "status": "completed",
                    "model": "fixture-model-2026-08",
                    "system_fingerprint": "fixture-fingerprint",
                    "output": [{
                        "id": "msg_1",
                        "type": "message",
                        "status": "completed",
                        "role": "assistant",
                        "content": [{"type": "output_text", "text": text}]
                    }],
                    "usage": {"input_tokens": 50, "output_tokens": 10, "total_tokens": 60}
                }
            }),
        ),
    ]
}

fn anthropic_text_events(text: &str) -> Vec<ModelProviderWireEvent> {
    vec![
        event(
            "message_start",
            serde_json::json!({
                "type": "message_start",
                "message": {
                    "id": "msg_1",
                    "type": "message",
                    "role": "assistant",
                    "model": "fixture-model-2026-08",
                    "content": [],
                    "stop_reason": null,
                    "stop_sequence": null,
                    "usage": {"input_tokens": 50, "output_tokens": 0}
                }
            }),
        ),
        event(
            "content_block_start",
            serde_json::json!({
                "type": "content_block_start",
                "index": 0,
                "content_block": {"type": "text", "text": ""}
            }),
        ),
        event(
            "content_block_delta",
            serde_json::json!({
                "type": "content_block_delta",
                "index": 0,
                "delta": {"type": "text_delta", "text": text}
            }),
        ),
        event(
            "content_block_stop",
            serde_json::json!({"type": "content_block_stop", "index": 0}),
        ),
        event(
            "message_delta",
            serde_json::json!({
                "type": "message_delta",
                "delta": {"stop_reason": "end_turn", "stop_sequence": null},
                "usage": {"output_tokens": 10}
            }),
        ),
        event("message_stop", serde_json::json!({"type": "message_stop"})),
    ]
}

fn openai_tool_events() -> Vec<ModelProviderWireEvent> {
    vec![
        event(
            "response.output_item.added",
            serde_json::json!({
                "type": "response.output_item.added",
                "item": {
                    "id": "fc_1",
                    "type": "function_call",
                    "status": "in_progress",
                    "call_id": "call_1",
                    "name": "lookup",
                    "arguments": ""
                }
            }),
        ),
        event(
            "response.function_call_arguments.delta",
            serde_json::json!({
                "type": "response.function_call_arguments.delta",
                "item_id": "fc_1",
                "delta": "{\"q\":\"hello\"}"
            }),
        ),
        event(
            "response.completed",
            serde_json::json!({
                "type": "response.completed",
                "response": {
                    "status": "completed",
                    "model": "fixture-model-2026-08",
                    "output": [{
                        "id": "fc_1",
                        "type": "function_call",
                        "status": "completed",
                        "call_id": "call_1",
                        "name": "lookup",
                        "arguments": "{\"q\":\"hello\"}"
                    }],
                    "usage": {"input_tokens": 50, "output_tokens": 10, "total_tokens": 60}
                }
            }),
        ),
    ]
}

fn anthropic_tool_events() -> Vec<ModelProviderWireEvent> {
    vec![
        event(
            "message_start",
            serde_json::json!({
                "type": "message_start",
                "message": {
                    "id": "msg_1",
                    "type": "message",
                    "role": "assistant",
                    "model": "fixture-model-2026-08",
                    "content": [],
                    "stop_reason": null,
                    "stop_sequence": null,
                    "usage": {"input_tokens": 50, "output_tokens": 0}
                }
            }),
        ),
        event(
            "content_block_start",
            serde_json::json!({
                "type": "content_block_start",
                "index": 0,
                "content_block": {"type": "tool_use", "id": "call_1", "name": "lookup", "input": {}}
            }),
        ),
        event(
            "content_block_delta",
            serde_json::json!({
                "type": "content_block_delta",
                "index": 0,
                "delta": {"type": "input_json_delta", "partial_json": "{\"q\":\"hello\"}"}
            }),
        ),
        event(
            "content_block_stop",
            serde_json::json!({"type": "content_block_stop", "index": 0}),
        ),
        event(
            "message_delta",
            serde_json::json!({
                "type": "message_delta",
                "delta": {"stop_reason": "tool_use", "stop_sequence": null},
                "usage": {"output_tokens": 10}
            }),
        ),
        event("message_stop", serde_json::json!({"type": "message_stop"})),
    ]
}

#[tokio::test]
async fn undeclared_usage_guarantees_still_require_actual_protocol_measurements() {
    for adapter in [
        OPENAI_RESPONSES_ADAPTER_NAME,
        ANTHROPIC_MESSAGES_ADAPTER_NAME,
    ] {
        for declared in [false, true] {
            let events = if adapter == OPENAI_RESPONSES_ADAPTER_NAME {
                openai_text_events("hello")
            } else {
                anthropic_text_events("hello")
            };
            let mut fixture = wire_fixture(adapter);
            fixture.request.profile.usage.provider_reports_usage = declared;
            let (outcome, connector) = execute_wire_fixture_result(fixture, events).await;
            let ModelAdapterExecutionOutcome::Succeeded(success) = outcome.unwrap() else {
                panic!("an undeclared guarantee must not reject actual complete measurements");
            };
            assert!(connector.request.lock().unwrap().is_some());
            assert_eq!(
                success.response.usage.accounting_quality,
                AccountingQuality::ProviderReported
            );
            assert_eq!(success.response.usage.input_tokens, Some(50));
            assert_eq!(success.response.usage.output_tokens, Some(10));
        }
        for field in ["input_tokens", "output_tokens"] {
            for invalid in [
                None,
                Some(Value::Null),
                Some(serde_json::json!(-1)),
                Some(serde_json::json!(1.5)),
                Some(serde_json::json!("10")),
            ] {
                let mut events = if adapter == OPENAI_RESPONSES_ADAPTER_NAME {
                    openai_text_events("hello")
                } else {
                    anthropic_text_events("hello")
                };
                let usage = if adapter == OPENAI_RESPONSES_ADAPTER_NAME {
                    &mut events.last_mut().unwrap().data["response"]["usage"]
                } else if field == "input_tokens" {
                    &mut events[0].data["message"]["usage"]
                } else {
                    &mut events[4].data["usage"]
                }
                .as_object_mut()
                .unwrap();
                if let Some(value) = invalid {
                    usage.insert(field.into(), value);
                } else {
                    usage.remove(field);
                }
                let mut fixture = wire_fixture(adapter);
                fixture.request.profile.usage.provider_reports_usage = false;
                let (outcome, connector) = execute_wire_fixture_result(fixture, events).await;
                let ModelAdapterExecutionOutcome::Failed(failure) = outcome.unwrap() else {
                    panic!("incomplete or malformed measurements must not succeed");
                };
                assert!(connector.request.lock().unwrap().is_some());
                assert_eq!(failure.class, ModelAdapterFailureClass::Permanent);
                assert!(failure.request_sent);
            }
        }
    }
}

#[tokio::test]
async fn responses_optional_metadata_is_typed_in_diagnostics_and_model_execution() {
    let events_with = |metadata: Value| {
        let mut events = openai_text_events("hello");
        let response = events.last_mut().unwrap().data["response"]
            .as_object_mut()
            .unwrap();
        response.insert("id".into(), serde_json::json!("resp_fixture"));
        response.insert("object".into(), serde_json::json!("response"));
        response.extend(metadata.as_object().unwrap().clone());
        events
    };
    for metadata in [
        serde_json::json!({}),
        serde_json::json!({"completed_at":null,"frequency_penalty":null,"presence_penalty":null}),
        serde_json::json!({"completed_at":1788980000,"frequency_penalty":0.25,"presence_penalty":-0.25}),
        serde_json::json!({"completed_at":0,"frequency_penalty":-2,"presence_penalty":2}),
    ] {
        let events = events_with(metadata);
        assert!(model_connection_response(
            ModelProviderWireProtocol::OpenAiResponses,
            &serde_json::to_vec(&events.last().unwrap().data["response"]).unwrap()
        ));
        let (outcome, wire) =
            execute_wire_fixture(wire_fixture(OPENAI_RESPONSES_ADAPTER_NAME), events).await;
        let ModelAdapterExecutionOutcome::Succeeded(success) = outcome else {
            panic!("valid optional response metadata was rejected");
        };
        assert_eq!(
            success.response.message.as_ref().unwrap().parts,
            vec![CanonicalMessagePart::Text("hello".into())]
        );
        assert_eq!(success.response.usage.input_tokens, Some(50));
        assert_eq!(success.response.usage.output_tokens, Some(10));
        for field in ["completed_at", "frequency_penalty", "presence_penalty"] {
            assert!(wire.request_body.get(field).is_none());
        }
    }
    for metadata in [
        serde_json::json!({"completed_at":-1}),
        serde_json::json!({"completed_at":1.5}),
        serde_json::json!({"completed_at":"1788980000"}),
        serde_json::json!({"completed_at":false}),
        serde_json::json!({"completed_at":{}}),
        serde_json::json!({"completed_at":[]}),
        serde_json::json!({"frequency_penalty":-2.01}),
        serde_json::json!({"frequency_penalty":2.01}),
        serde_json::json!({"frequency_penalty":"0"}),
        serde_json::json!({"frequency_penalty":true}),
        serde_json::json!({"frequency_penalty":{}}),
        serde_json::json!({"presence_penalty":-2.01}),
        serde_json::json!({"presence_penalty":2.01}),
        serde_json::json!({"presence_penalty":"0"}),
        serde_json::json!({"presence_penalty":[]}),
        serde_json::json!({"unknown_provider_extension":null}),
    ] {
        let unknown = metadata.get("unknown_provider_extension").is_some();
        let events = events_with(metadata);
        assert!(!model_connection_response(
            ModelProviderWireProtocol::OpenAiResponses,
            &serde_json::to_vec(&events.last().unwrap().data["response"]).unwrap()
        ));
        let (outcome, _) =
            execute_wire_fixture(wire_fixture(OPENAI_RESPONSES_ADAPTER_NAME), events).await;
        let ModelAdapterExecutionOutcome::Failed(failure) = outcome else {
            panic!("invalid optional response metadata was accepted");
        };
        assert_eq!(failure.class, ModelAdapterFailureClass::Permanent);
        assert!(failure.request_sent);
        assert!(failure.safe_code.ends_with(if unknown {
            "unknown_field"
        } else {
            "invalid_response_metadata"
        }));
    }
    let events = events_with(serde_json::json!({"frequency_penalty":0}));
    let response = serde_json::to_string(&events.last().unwrap().data["response"])
        .unwrap()
        .replace("\"frequency_penalty\":0", "\"frequency_penalty\":1e999");
    assert!(!model_connection_response(
        ModelProviderWireProtocol::OpenAiResponses,
        response.as_bytes()
    ));
    let raw = format!("event: response.completed\ndata: {{\"type\":\"response.completed\",\"response\":{response}}}\n\n");
    let broker = Arc::new(FixtureEgressBroker::raw(
        200,
        "text/event-stream",
        vec![raw.into_bytes()],
    ));
    let outcome =
        execute_brokered_fixture(wire_fixture(OPENAI_RESPONSES_ADAPTER_NAME), broker).await;
    assert!(matches!(outcome, ModelAdapterExecutionOutcome::Failed(_)));
}

#[tokio::test]
async fn production_wire_adapters_share_text_stream_and_usage_contract() {
    for (adapter_name, protocol, events) in [
        (
            OPENAI_RESPONSES_ADAPTER_NAME,
            ModelProviderWireProtocol::OpenAiResponses,
            openai_text_events("hello"),
        ),
        (
            ANTHROPIC_MESSAGES_ADAPTER_NAME,
            ModelProviderWireProtocol::AnthropicMessages,
            anthropic_text_events("hello"),
        ),
    ] {
        let (outcome, wire) = execute_wire_fixture(wire_fixture(adapter_name), events).await;
        let ModelAdapterExecutionOutcome::Succeeded(success) = outcome else {
            panic!("wire adapter did not complete");
        };
        assert_eq!(
            success.response.message.as_ref().unwrap().parts,
            vec![CanonicalMessagePart::Text("hello".to_owned())]
        );
        assert_eq!(success.response.usage.input_tokens, Some(50));
        assert_eq!(success.response.usage.output_tokens, Some(10));
        assert_eq!(success.stream_evidence.accepted_delta_count, 1);
        assert_eq!(wire.protocol, protocol);
        assert_eq!(wire.schema_version, 2);
        assert_eq!(wire.job_id.kind(), ResourceKind::Job);
        assert_eq!(
            wire.worker_process_generation_id.kind(),
            ResourceKind::WorkerProcessGeneration
        );
        assert_eq!(wire.attempt_no, 1);
        assert_eq!(wire.lease_generation, 1);
        assert_eq!(wire.endpoint_path(), protocol.endpoint_path());
        assert_eq!(wire.protocol_version(), protocol.protocol_version());
        assert_eq!(wire.request_body.get("stream"), Some(&Value::Bool(true)));
        assert_eq!(
            wire.request_body_digest,
            canonical_digest(&wire.request_body)
                .unwrap()
                .parse()
                .unwrap()
        );
        let debug = format!("{wire:?}");
        assert!(!debug.contains("Answer safely"));
        assert!(!debug.contains("Say hello"));
    }
}

#[tokio::test]
async fn production_wire_adapters_share_tool_and_local_schema_contract() {
    for (adapter_name, events) in [
        (OPENAI_RESPONSES_ADAPTER_NAME, openai_tool_events()),
        (ANTHROPIC_MESSAGES_ADAPTER_NAME, anthropic_tool_events()),
    ] {
        let mut fixture = wire_fixture(adapter_name);
        enable_tool(&mut fixture);
        enable_structured_output(&mut fixture);
        let (outcome, wire) = execute_wire_fixture(fixture, events).await;
        let ModelAdapterExecutionOutcome::Succeeded(success) = outcome else {
            panic!("wire adapter did not produce a tool intent");
        };
        assert_eq!(
            success.response.finish_reason,
            CanonicalFinishReason::ToolUse
        );
        assert_eq!(success.response.tool_intents.len(), 1);
        assert_eq!(
            success.response.tool_intents[0].projected_tool_name,
            "lookup"
        );
        assert_eq!(
            success.response.tool_intents[0].arguments.value,
            serde_json::json!({"q": "hello"})
        );
        assert_eq!(success.stream_evidence.accepted_delta_count, 1);
        assert!(wire.request_body.get("tools").is_some());
    }
}

#[tokio::test]
async fn production_wire_adapters_share_native_structured_output_contract() {
    for (adapter_name, events) in [
        (
            OPENAI_RESPONSES_ADAPTER_NAME,
            openai_text_events("{\"answer\":\"hello\"}"),
        ),
        (
            ANTHROPIC_MESSAGES_ADAPTER_NAME,
            anthropic_text_events("{\"answer\":\"hello\"}"),
        ),
    ] {
        let mut fixture = wire_fixture(adapter_name);
        enable_structured_output(&mut fixture);
        let (outcome, wire) = execute_wire_fixture(fixture, events).await;
        let ModelAdapterExecutionOutcome::Succeeded(success) = outcome else {
            panic!("wire adapter did not produce structured output");
        };
        assert_eq!(
            success.response.structured_output.as_ref().unwrap().value,
            serde_json::json!({"answer": "hello"})
        );
        assert!(success.response.message.is_none());
        let format = if adapter_name == OPENAI_RESPONSES_ADAPTER_NAME {
            wire.request_body.pointer("/text/format")
        } else {
            wire.request_body.pointer("/output_config/format")
        };
        assert!(format.is_some());
    }
}

#[tokio::test]
async fn production_wire_adapters_support_explicit_textual_json_without_prompt_injection() {
    for (adapter_name, events) in [
        (
            OPENAI_RESPONSES_ADAPTER_NAME,
            openai_text_events("{\"answer\":\"hello\"}"),
        ),
        (
            ANTHROPIC_MESSAGES_ADAPTER_NAME,
            anthropic_text_events("{\"answer\":\"hello\"}"),
        ),
    ] {
        let mut fixture = wire_fixture(adapter_name);
        enable_structured_output(&mut fixture);
        fixture.request.profile.structured_output.native = false;
        let original_request_digest = fixture.request.request_digest.clone();
        let (outcome, connector) = execute_wire_fixture_result(fixture, events).await;
        let Ok(ModelAdapterExecutionOutcome::Succeeded(success)) = outcome else {
            panic!("explicit textual JSON output was rejected: {outcome:?}");
        };
        let wire = connector.take_request();
        assert_eq!(connector.calls.load(Ordering::SeqCst), 1);
        assert_eq!(
            success.response.structured_output.as_ref().unwrap().value,
            serde_json::json!({"answer": "hello"})
        );
        assert!(success.response.message.is_none());
        assert!(wire.request_body.get("text").is_none());
        assert!(wire.request_body.get("output_config").is_none());
        assert_eq!(wire.model_request_digest, original_request_digest);
        let (instruction, prompt) = if adapter_name == OPENAI_RESPONSES_ADAPTER_NAME {
            (
                wire.request_body.pointer("/input/0/content/0/text"),
                wire.request_body.pointer("/input/1/content/0/text"),
            )
        } else {
            (
                wire.request_body.pointer("/system/0/text"),
                wire.request_body.pointer("/messages/0/content/0/text"),
            )
        };
        assert_eq!(
            instruction,
            Some(&Value::String("Answer safely.".to_owned()))
        );
        assert_eq!(prompt, Some(&Value::String("Say hello.".to_owned())));
    }
}

#[tokio::test]
async fn textual_json_rejects_unsupported_modes_and_oversized_requests_before_dispatch() {
    for adapter_name in [
        OPENAI_RESPONSES_ADAPTER_NAME,
        ANTHROPIC_MESSAGES_ADAPTER_NAME,
    ] {
        for case in [
            "tools",
            "tool_intent",
            "mixed_output",
            "historical_tool",
            "schema",
            "request",
            "disabled",
        ] {
            let mut fixture = wire_fixture(adapter_name);
            enable_structured_output(&mut fixture);
            fixture.request.profile.structured_output.native = false;
            match case {
                "tools" | "tool_intent" | "mixed_output" => {
                    enable_tool(&mut fixture);
                    if case == "tools" {
                        fixture.request.request.response_contract.allow_tool_intents = false;
                    }
                    if case != "tools" {
                        fixture.request.request.tools.clear();
                    }
                    if case == "mixed_output" {
                        fixture
                            .request
                            .profile
                            .structured_output
                            .may_combine_with_tool_intent = true;
                        fixture
                            .request
                            .request
                            .response_contract
                            .allow_message_with_tool_intents = true;
                    }
                }
                "historical_tool" => {
                    let value = serde_json::json!({"result": "original tool output"});
                    let content_digest = canonical_digest(&value).unwrap().parse().unwrap();
                    let mut message = fixture.request.request.messages.last().unwrap().clone();
                    message.role = CanonicalMessageRole::Tool;
                    message.source.assembly_phase =
                        insight_platform_models::PromptAssemblyPhase::CapabilityToolResult;
                    message.parts = vec![CanonicalMessagePart::ToolResult(
                        insight_platform_models::ModelToolResult {
                            call_id: "past-call".to_owned(),
                            invocation_id: id(ResourceKind::CapabilityInvocation, 81),
                            output_value_id: id(ResourceKind::RunValue, 82),
                            output_schema_digest: sha('3'),
                            content_digest,
                            classification: DataClassification::Internal,
                            value: ValueRef::Inline { value },
                        },
                    )];
                    fixture.request.request.messages.push(message);
                }
                "schema" => {
                    fixture
                        .request
                        .profile
                        .structured_output
                        .maximum_schema_bytes = 1
                }
                "request" => {
                    fixture
                        .request
                        .provider
                        .request_limits
                        .maximum_request_bytes = 1
                }
                "disabled" => {
                    fixture
                        .request
                        .profile
                        .structured_output
                        .textual_json_fallback = false
                }
                _ => unreachable!(),
            }
            fixture.request.request_digest =
                canonical_request_digest(&fixture.request.request).unwrap();
            if !matches!(case, "request" | "disabled") {
                fixture.request.validate_at(Utc::now(), limits()).unwrap();
            }
            let (outcome, connector) = execute_wire_fixture_result(fixture, vec![]).await;
            match outcome {
                Ok(ModelAdapterExecutionOutcome::Failed(failure)) => {
                    assert_eq!(
                        failure.class,
                        ModelAdapterFailureClass::RejectedBeforeDispatch,
                        "{adapter_name}/{case}"
                    );
                    assert!(!failure.request_sent);
                }
                Err(_) if matches!(case, "request" | "disabled") => {}
                other => panic!("{adapter_name}/{case} accepted unsupported mode: {other:?}"),
            }
            assert_eq!(
                connector.calls.load(Ordering::SeqCst),
                0,
                "{adapter_name}/{case}"
            );
            assert!(connector.request.lock().unwrap().is_none());
        }
    }
}

#[tokio::test]
async fn structured_text_is_strict_json_and_exact_schema_without_repair_or_retry() {
    let outputs = [
        (
            "{\"answer\":\"secret-canary\",\"answer\":\"hello\"}",
            "model_structured_output_invalid_json",
        ),
        ("{\"answer\":NaN}", "model_structured_output_invalid_json"),
        ("{\"answer\":1e999}", "model_structured_output_invalid_json"),
        ("{\"answer\":42}", "model_structured_output_schema_mismatch"),
        (
            "{\"answer\":\"hello\",\"unexpected\":true}",
            "model_structured_output_schema_mismatch",
        ),
        (
            "{\"answer\":\"hello\"} trailing",
            "model_structured_output_invalid_json",
        ),
        (
            "{\"answer\":\"hello\"}{\"answer\":\"again\"}",
            "model_structured_output_invalid_json",
        ),
        (
            "```json\n{\"answer\":\"hello\"}\n```",
            "model_structured_output_invalid_json",
        ),
        (
            "prefix {\"answer\":\"hello\"}",
            "model_structured_output_invalid_json",
        ),
        ("[]", "model_structured_output_schema_mismatch"),
        (
            "{\"answer\":\"\\ud800\"}",
            "model_structured_output_invalid_json",
        ),
    ];
    for adapter_name in [
        OPENAI_RESPONSES_ADAPTER_NAME,
        ANTHROPIC_MESSAGES_ADAPTER_NAME,
    ] {
        for native in [false, true] {
            for (output, expected_code) in outputs {
                let mut fixture = wire_fixture(adapter_name);
                enable_structured_output(&mut fixture);
                fixture.request.profile.structured_output.native = native;
                let events = if adapter_name == OPENAI_RESPONSES_ADAPTER_NAME {
                    openai_text_events(output)
                } else {
                    anthropic_text_events(output)
                };
                let (outcome, connector) = execute_wire_fixture_result(fixture, events).await;
                let Ok(ModelAdapterExecutionOutcome::Failed(failure)) = outcome else {
                    panic!("{adapter_name}/native={native} accepted invalid structured text: {outcome:?}");
                };
                assert_eq!(failure.class, ModelAdapterFailureClass::Permanent);
                assert_eq!(failure.safe_code, expected_code);
                assert!(failure.request_sent);
                assert!(!format!("{failure:?}").contains("secret-canary"));
                assert_eq!(connector.calls.load(Ordering::SeqCst), 1);
                let wire = connector.take_request();
                let has_native = wire.request_body.get("text").is_some()
                    || wire.request_body.get("output_config").is_some();
                assert_eq!(has_native, native);
            }
        }
    }
}
#[tokio::test]
async fn textual_json_output_has_exact_byte_bound_and_real_incremental_sse_validation() {
    let raw = "{\"answer\":\"你好\\\"\\n\"}";
    let parsed = serde_json::from_str::<Value>(raw).unwrap();
    for adapter_name in [
        OPENAI_RESPONSES_ADAPTER_NAME,
        ANTHROPIC_MESSAGES_ADAPTER_NAME,
    ] {
        for fits in [false, true] {
            let mut fixture = wire_fixture(adapter_name);
            enable_structured_output(&mut fixture);
            fixture.request.profile.structured_output.native = false;
            fixture
                .request
                .profile
                .structured_output
                .maximum_output_bytes = u32::try_from(raw.len() - usize::from(!fits)).unwrap();
            let events = if adapter_name == OPENAI_RESPONSES_ADAPTER_NAME {
                openai_text_events(raw)
            } else {
                anthropic_text_events(raw)
            };
            let broker = Arc::new(FixtureEgressBroker::from_events(events));
            let outcome = execute_brokered_fixture(fixture, broker.clone()).await;
            match outcome {
                ModelAdapterExecutionOutcome::Succeeded(success) if fits => {
                    assert_eq!(success.response.structured_output.unwrap().value, parsed);
                }
                ModelAdapterExecutionOutcome::Failed(failure) if !fits => {
                    assert_eq!(failure.class, ModelAdapterFailureClass::Permanent);
                    assert_eq!(failure.safe_code, "model_structured_output_too_large");
                }
                other => panic!("{adapter_name}/fits={fits}: {other:?}"),
            }
            assert!(broker.request.lock().unwrap().is_some());
        }
    }
}

#[tokio::test]
async fn textual_json_rejects_tool_output_and_incomplete_streams() {
    for adapter_name in [
        OPENAI_RESPONSES_ADAPTER_NAME,
        ANTHROPIC_MESSAGES_ADAPTER_NAME,
    ] {
        for tool_output in [false, true] {
            let mut fixture = wire_fixture(adapter_name);
            enable_structured_output(&mut fixture);
            fixture.request.profile.structured_output.native = false;
            let mut events = match (adapter_name, tool_output) {
                (OPENAI_RESPONSES_ADAPTER_NAME, true) => openai_tool_events(),
                (OPENAI_RESPONSES_ADAPTER_NAME, false) => {
                    openai_text_events("{\"answer\":\"hello\"}")
                }
                (_, true) => anthropic_tool_events(),
                (_, false) => anthropic_text_events("{\"answer\":\"hello\"}"),
            };
            if !tool_output {
                events.pop();
            }
            let (outcome, connector) = execute_wire_fixture_result(fixture, events).await;
            let Ok(ModelAdapterExecutionOutcome::Failed(failure)) = outcome else {
                panic!("{adapter_name}/tools={tool_output}: {outcome:?}");
            };
            assert!(failure.request_sent);
            assert_eq!(connector.calls.load(Ordering::SeqCst), 1);
        }
    }
}

#[tokio::test]
async fn production_wire_adapters_fail_closed_on_unknown_provider_fields() {
    for (adapter_name, events) in [
        (
            OPENAI_RESPONSES_ADAPTER_NAME,
            vec![event(
                "response.output_text.delta",
                serde_json::json!({
                    "type": "response.output_text.delta",
                    "delta": "hello",
                    "future_field": true
                }),
            )],
        ),
        (
            ANTHROPIC_MESSAGES_ADAPTER_NAME,
            vec![event(
                "message_start",
                serde_json::json!({
                    "type": "message_start",
                    "future_field": true,
                    "message": {}
                }),
            )],
        ),
    ] {
        let (outcome, _) = execute_wire_fixture(wire_fixture(adapter_name), events).await;
        let ModelAdapterExecutionOutcome::Failed(failure) = outcome else {
            panic!("unknown Provider field was accepted");
        };
        assert_eq!(failure.class, ModelAdapterFailureClass::Permanent);
        assert!(failure.safe_code.ends_with("unknown_field"));
        assert!(failure.request_sent);
    }
}

#[tokio::test]
async fn brokered_connector_feeds_strict_incremental_sse_to_both_adapters() {
    for (adapter_name, events) in [
        (OPENAI_RESPONSES_ADAPTER_NAME, openai_text_events("hello")),
        (
            ANTHROPIC_MESSAGES_ADAPTER_NAME,
            anthropic_text_events("hello"),
        ),
    ] {
        let broker = Arc::new(FixtureEgressBroker::from_events(events));
        let outcome = execute_brokered_fixture(wire_fixture(adapter_name), broker.clone()).await;
        let ModelAdapterExecutionOutcome::Succeeded(success) = outcome else {
            panic!("brokered Provider stream did not complete");
        };
        assert_eq!(success.response.usage.output_tokens, Some(10));
        let request = broker.request.lock().unwrap();
        assert!(request.is_some());
        assert_eq!(request.as_ref().unwrap().secret_bindings.len(), 1);
        assert!(!format!("{:?}", request.as_ref().unwrap()).contains("api_key"));
    }
}

fn metadata_sse(events: &[ModelProviderWireEvent], newline: &str) -> Vec<u8> {
    let mut encoded = "\u{feff}".to_owned();
    for event in events {
        encoded.push_str(&format!(
            ": ignored{newline}id: transport-canary{newline}retry: 999999999999999999999999999999{newline}event: {}{newline}data: {}{newline}{newline}",
            event.event_name,
            serde_json::to_string(&event.data).unwrap()
        ));
    }
    encoded.into_bytes()
}

#[tokio::test]
async fn brokered_sse_metadata_preserves_exact_response_usage_and_request_without_reconnect() {
    for (adapter_name, events) in [
        (OPENAI_RESPONSES_ADAPTER_NAME, openai_text_events("hello")),
        (
            ANTHROPIC_MESSAGES_ADAPTER_NAME,
            anthropic_text_events("hello"),
        ),
    ] {
        let fixture = wire_fixture(adapter_name);
        let baseline_broker = Arc::new(FixtureEgressBroker::from_events(events.clone()));
        let baseline = execute_brokered_fixture(fixture.clone(), baseline_broker.clone()).await;
        let ModelAdapterExecutionOutcome::Succeeded(ref expected) = baseline else {
            panic!("baseline fixture failed");
        };
        assert_eq!(expected.response.usage.input_tokens, Some(50));
        assert_eq!(expected.response.usage.output_tokens, Some(10));
        for newline in ["\n", "\r", "\r\n"] {
            let encoded = metadata_sse(&events, newline);
            let broker = Arc::new(FixtureEgressBroker::raw(
                200,
                "text/event-stream",
                encoded.chunks(1).map(<[u8]>::to_vec).collect(),
            ));
            let outcome = execute_brokered_fixture(fixture.clone(), broker.clone()).await;
            // Includes the canonical output, provider-response digest, usage and stream evidence.
            assert_eq!(outcome, baseline);
            assert!(!format!("{outcome:?}").contains("transport-canary"));
            assert_eq!(broker.calls.load(Ordering::SeqCst), 1);
            let request = broker.request.lock().unwrap();
            let request = request.as_ref().unwrap();
            assert_eq!(request.model_request_digest, fixture.request.request_digest);
            assert_eq!(
                request.request_body_digest,
                baseline_broker
                    .request
                    .lock()
                    .unwrap()
                    .as_ref()
                    .unwrap()
                    .request_body_digest
            );
        }
        assert_eq!(baseline_broker.calls.load(Ordering::SeqCst), 1);
    }
}

#[tokio::test]
async fn brokered_sse_metadata_never_repairs_payload_or_invents_terminal_success() {
    for adapter_name in [
        OPENAI_RESPONSES_ADAPTER_NAME,
        ANTHROPIC_MESSAGES_ADAPTER_NAME,
    ] {
        for case in ["business_unknown", "truncated", "done_only"] {
            let mut events = if adapter_name == OPENAI_RESPONSES_ADAPTER_NAME {
                openai_text_events("hello")
            } else {
                anthropic_text_events("hello")
            };
            if case == "business_unknown" {
                events[0].data["unsupported_business_field"] = serde_json::json!("payload-canary");
            }
            let mut encoded = metadata_sse(&events, "\n");
            if case == "truncated" {
                encoded.pop();
            } else if case == "done_only" {
                encoded = b"id: transport-canary\nretry: 1\ndata: [DONE]\r\n\r\n".to_vec();
            }
            let broker = Arc::new(FixtureEgressBroker::raw(
                200,
                "text/event-stream",
                encoded.chunks(1).map(<[u8]>::to_vec).collect(),
            ));
            let outcome =
                execute_brokered_fixture(wire_fixture(adapter_name), broker.clone()).await;
            let ModelAdapterExecutionOutcome::Failed(failure) = outcome else {
                panic!("{adapter_name}/{case} invented success");
            };
            assert!(failure.request_sent);
            assert_eq!(
                failure.class,
                if case == "done_only" {
                    ModelAdapterFailureClass::RetryableAfterDispatch
                } else {
                    ModelAdapterFailureClass::Permanent
                }
            );
            match case {
                "business_unknown" => assert!(failure.safe_code.ends_with("unknown_field")),
                "truncated" => assert_eq!(failure.safe_code, "model_sse_incomplete_event"),
                "done_only" => assert!(failure.safe_code.ends_with("missing_terminal")),
                _ => unreachable!(),
            }
            assert!(!format!("{failure:?}").contains("canary"));
            assert_eq!(broker.calls.load(Ordering::SeqCst), 1);
        }
    }
}

#[tokio::test]
async fn brokered_connector_maps_status_content_type_and_duplicate_json_closed() {
    let retry_broker = Arc::new(FixtureEgressBroker::raw(429, "text/event-stream", vec![]));
    let outcome =
        execute_brokered_fixture(wire_fixture(OPENAI_RESPONSES_ADAPTER_NAME), retry_broker).await;
    let ModelAdapterExecutionOutcome::Failed(failure) = outcome else {
        panic!("retryable Provider status was accepted");
    };
    assert_eq!(
        failure.class,
        ModelAdapterFailureClass::RetryableAfterDispatch
    );

    let content_type_broker = Arc::new(FixtureEgressBroker::raw(200, "application/json", vec![]));
    let outcome = execute_brokered_fixture(
        wire_fixture(OPENAI_RESPONSES_ADAPTER_NAME),
        content_type_broker,
    )
    .await;
    let ModelAdapterExecutionOutcome::Failed(failure) = outcome else {
        panic!("invalid Provider content type was accepted");
    };
    assert_eq!(failure.safe_code, "model_provider_invalid_content_type");

    let duplicate_json_broker = Arc::new(FixtureEgressBroker::raw(
        200,
        "text/event-stream",
        vec![b"data: {\"type\":\"response.created\",\"type\":\"response.completed\"}\n\n".to_vec()],
    ));
    let outcome = execute_brokered_fixture(
        wire_fixture(OPENAI_RESPONSES_ADAPTER_NAME),
        duplicate_json_broker,
    )
    .await;
    let ModelAdapterExecutionOutcome::Failed(failure) = outcome else {
        panic!("duplicate Provider JSON key was accepted");
    };
    assert_eq!(failure.safe_code, "model_sse_invalid_json");
}

#[tokio::test]
async fn live_text_sequence_ignores_private_metadata_and_resets_per_attempt() {
    let fixture = fixture("fixture.responses/v1", '9', 'a');
    let sink = Arc::new(CapturingSink::default());
    let host = ModelAdapterHost::new(
        InstalledModelAdapterRegistry::default(),
        sink.clone(),
        limits(),
    );
    let deltas = vec![
        NormalizedModelDelta::Text("a".into()),
        NormalizedModelDelta::ProviderMetadataDigest(sha('a')),
        NormalizedModelDelta::Text("b".into()),
    ];
    let mut response = fixture.response.clone();
    response.observation.stream_delta_count = 3;
    response.observation.stream_bytes = deltas
        .iter()
        .map(|d| serde_json::to_vec(d).unwrap().len() as u64)
        .sum();
    let mut frames: Vec<_> = deltas
        .into_iter()
        .enumerate()
        .map(|(i, delta)| {
            Ok(NormalizedModelFrame {
                model_turn_id: fixture.request.model_turn_id.clone(),
                attempt_no: fixture.request.attempt_no,
                lease_generation: fixture.request.lease_generation,
                transport_sequence: i as u64 + 1,
                delta,
            })
        })
        .collect();
    frames.push(Ok(NormalizedModelFrame {
        model_turn_id: fixture.request.model_turn_id.clone(),
        attempt_no: fixture.request.attempt_no,
        lease_generation: fixture.request.lease_generation,
        transport_sequence: 4,
        delta: NormalizedModelDelta::Terminal(Box::new(response)),
    }));
    for _ in 0..2 {
        host.consume_stream(Box::pin(stream::iter(frames.clone())), &fixture.request)
            .await
            .unwrap();
    }
    assert_eq!(*sink.text_sequences.lock().unwrap(), vec![1, 1, 2, 1, 1, 2]);
}

#[tokio::test]
async fn structured_answer_exceeding_128_characters_is_schema_failure_without_truncation() {
    for adapter_name in [
        OPENAI_RESPONSES_ADAPTER_NAME,
        ANTHROPIC_MESSAGES_ADAPTER_NAME,
    ] {
        let mut fixture = wire_fixture(adapter_name);
        enable_structured_output(&mut fixture);
        let mut schema = fixture
            .request
            .request
            .response_contract
            .structured_schema
            .as_ref()
            .unwrap()
            .schema
            .clone();
        schema["properties"]["answer"]["maxLength"] = serde_json::json!(128);
        let schema = ClosedSchemaDocument::build(schema).unwrap();
        fixture
            .request
            .request
            .response_contract
            .output_schema_digest = schema.canonical_digest.clone();
        fixture.request.request.response_contract.structured_schema = Some(schema);
        fixture.request.request_digest =
            canonical_request_digest(&fixture.request.request).unwrap();
        let output =
            serde_json::to_string(&serde_json::json!({"answer": "a".repeat(500)})).unwrap();
        let events = if adapter_name == OPENAI_RESPONSES_ADAPTER_NAME {
            openai_text_events(&output)
        } else {
            anthropic_text_events(&output)
        };
        let (result, connector) = execute_wire_fixture_result(fixture, events).await;
        let Ok(ModelAdapterExecutionOutcome::Failed(failure)) = result else {
            panic!("overlong answer must fail")
        };
        assert_eq!(failure.safe_code, "model_structured_output_schema_mismatch");
        assert_eq!(failure.class, ModelAdapterFailureClass::Permanent);
        assert_eq!(connector.calls.load(Ordering::SeqCst), 1);
    }
}
