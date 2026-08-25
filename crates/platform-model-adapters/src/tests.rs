use super::*;
use chrono::Duration as ChronoDuration;
use futures::{stream, StreamExt};
use insight_platform_contracts::{
    checked_in_hard_limit_profile, ArtifactRef, AuthoringPackage, ClosedJsonValue, CommandOutcome,
    ContextWindowContract, DataClassification, DataRegion, Effect, ExactSecretBindingRef,
    ExactVersionRef, ExternalLeafFailureMutationIds, ExternalLeafResumeMutationIds,
    InstalledModelAdapter, ModelCatalogEvidence, ModelIdentityStability, ModelLimits,
    ModelModalities, ModelToolContract, ModelUsageContract, ProviderDataHandlingContract,
    ProviderModelIdentity, ProviderRequestLimits, ProviderTrainingPolicy, SecretPurpose,
    SecretResolutionPolicy, StructuredOutputContract, ValueRef,
};
use insight_platform_jobs::JobFence;
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

struct Fixture {
    request: ModelAdapterExecutionRequest,
    response: CanonicalModelResponse,
    descriptor: InstalledModelAdapterDescriptor,
}

fn fixture(adapter_name: &str, manifest: char, contract: char) -> Fixture {
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
        adapter_contract_digest: sha(contract),
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
            tokenizer_contract_digest: sha('c'),
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
            maximum_retention_milliseconds: 86_400_000,
            training: ProviderTrainingPolicy::Prohibited,
            subprocessor_set_digest: sha('e'),
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
            artifact: artifact(11, 'f'),
            source_digest: sha('1'),
            adapter_contract_digest: sha(contract),
            observed_at: now - ChronoDuration::minutes(1),
            expires_at: now + ChronoDuration::days(1),
        },
    };
    let provider_closure = ModelProviderDeploymentClosure {
        provider_revision,
        endpoint_identity_digest: sha('2'),
        secret_bindings: vec![exact_secret_binding(12)],
        protocol_policy: protocol_policy.clone(),
        network_policy: policy(13, '3'),
        tls_policy: policy(14, '4'),
        trust_policy: policy(15, '5'),
        data_policy: policy(16, '6'),
        region,
        conformance_evidence: artifact(17, '7'),
    };
    let model_closure = ModelDeploymentClosure {
        profile_revision: profile_revision.clone(),
        provider_deployment: provider_deployment.clone(),
        data_policy: policy(18, '8'),
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
}

#[async_trait]
impl ModelLiveDeltaSink for CapturingSink {
    async fn publish(
        &self,
        execution: &ModelAdapterExecutionRequest,
        frame: &NormalizedModelFrame,
    ) {
        assert_eq!(execution.model_turn_id, frame.model_turn_id);
        self.frames.lock().unwrap().push(frame.clone());
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
async fn exact_manifest_mismatch_fails_before_provider_dispatch() {
    let mut fixture = fixture("fixture.responses/v1", '9', 'a');
    let mut registry = InstalledModelAdapterRegistry::default();
    registry
        .install(Arc::new(StaticAdapter {
            descriptor: fixture.descriptor,
            response: fixture.response,
        }))
        .unwrap();
    fixture.request.worker_manifest_digest = sha('0');
    let host = ModelAdapterHost::new(registry, Arc::new(DropModelLiveDeltas), limits());
    assert_eq!(
        host.execute(fixture.request).await,
        Err(ModelAdapterHostError::InvalidExecutionContract)
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
        Ok(ModelOutputValue {
            value_id: id(ResourceKind::RunValue, 40),
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

struct CapturingAuthority {
    outcome: Mutex<Option<ModelDispatchOutcome>>,
    fence: Mutex<Option<JobFence>>,
    terminal_mutations: Mutex<Option<(bool, bool)>>,
}

#[async_trait]
impl ModelExecutionAuthority for Arc<CapturingAuthority> {
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
            continuation_node_execution_id: id(ResourceKind::NodeExecution, 50),
            continuation_job_id: id(ResourceKind::Job, 51),
            run_event_id: id(ResourceKind::Event, 52),
            run_outbox_id: id(ResourceKind::OutboxEvent, 53),
            leaf_node_event_id: id(ResourceKind::Event, 54),
            leaf_node_outbox_id: id(ResourceKind::OutboxEvent, 55),
            continuation_node_event_id: id(ResourceKind::Event, 56),
            continuation_node_outbox_id: id(ResourceKind::OutboxEvent, 57),
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
    let authority = Arc::new(CapturingAuthority {
        outcome: Mutex::new(None),
        fence: Mutex::new(None),
        terminal_mutations: Mutex::new(None),
    });
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
    let authority = Arc::new(CapturingAuthority {
        outcome: Mutex::new(None),
        fence: Mutex::new(None),
        terminal_mutations: Mutex::new(None),
    });
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
    let authority = Arc::new(CapturingAuthority {
        outcome: Mutex::new(None),
        fence: Mutex::new(None),
        terminal_mutations: Mutex::new(None),
    });
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
        let authority = Arc::new(CapturingAuthority {
            outcome: Mutex::new(None),
            fence: Mutex::new(None),
            terminal_mutations: Mutex::new(None),
        });
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

struct FixtureWireConnector {
    request: Mutex<Option<ModelProviderWireRequest>>,
    events: Mutex<Option<Vec<Result<ModelProviderWireEvent, ModelAdapterFailure>>>>,
}

impl FixtureWireConnector {
    fn new(events: Vec<ModelProviderWireEvent>) -> Self {
        Self {
            request: Mutex::new(None),
            events: Mutex::new(Some(events.into_iter().map(Ok).collect())),
        }
    }

    fn take_request(&self) -> ModelProviderWireRequest {
        self.request.lock().unwrap().take().unwrap()
    }
}

#[async_trait]
impl ModelProviderWireConnector for FixtureWireConnector {
    async fn open(
        &self,
        request: ModelProviderWireRequest,
    ) -> Result<ModelProviderWireStream, ModelAdapterFailure> {
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
    fixture.request.request_digest = canonical_request_digest(&fixture.request.request).unwrap();
}

async fn execute_wire_fixture(
    fixture: Fixture,
    events: Vec<ModelProviderWireEvent>,
) -> (ModelAdapterExecutionOutcome, ModelProviderWireRequest) {
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
        .await
        .unwrap();
    (outcome, connector.take_request())
}

async fn execute_brokered_fixture(
    fixture: Fixture,
    broker: Arc<FixtureEgressBroker>,
) -> ModelAdapterExecutionOutcome {
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
        .unwrap()
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
