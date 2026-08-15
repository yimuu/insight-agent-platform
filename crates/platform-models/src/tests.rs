use crate::*;
use chrono::{Duration, Utc};
use insight_platform_contracts::{
    AgentDeploymentClosure, ArtifactRef, AuthoringPackage, ClosedJsonValue, CommandAudit,
    ContextWindowContract, DataClassification, DataRegion, DecimalMoney, ExactDeploymentRef,
    ExactSecretBindingRef, ExactVersionRef, FrozenSlotBinding, FrozenSlotTarget,
    InstalledModelAdapter, ModelArtifactDeliveryContract, ModelCatalogEvidence,
    ModelDeploymentClosure, ModelIdentityStability, ModelLimits, ModelModalities,
    ModelProfileResourceSpec, ModelProviderDeploymentClosure, ModelProviderResourceSpec,
    ModelToolContract, ModelUsageContract, Permission, PermissionSet, PrincipalKind,
    PrincipalSnapshot, ProviderDataHandlingContract, ProviderModelIdentity, ProviderRequestLimits,
    ProviderTrainingPolicy, ResourceId, ResourceKind, RunBindingsSnapshot, SecretPurpose,
    SecretResolutionPolicy, Sha256Digest, StructuredOutputContract, ValueRef,
};
use insight_platform_jobs::{JobFence, LeasePolicy};
use serde_json::json;

struct Fixture {
    now: chrono::DateTime<Utc>,
    limits: ModelTurnLimits,
    command: CreateModelTurn,
    facts: ModelAdmissionFacts,
    request: CanonicalModelRequest,
    argument_schema: ClosedSchemaDocument,
}

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

fn artifact(suffix: u16, character: char, purpose: &str) -> ArtifactRef {
    ArtifactRef::new(
        id(ResourceKind::Artifact, suffix),
        sha(character),
        16,
        "application/json",
        DataClassification::Internal,
        Some(format!("{purpose}.json")),
    )
    .unwrap()
}

fn authoring(suffix: u16, character: char) -> AuthoringPackage {
    AuthoringPackage {
        artifact: artifact(suffix, character, "authoring"),
        manifest_digest: sha(character),
    }
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

fn audit(
    tenant_id: &ResourceId,
    principal_id: &ResourceId,
    suffix: u16,
    now: chrono::DateTime<Utc>,
) -> CommandAudit {
    CommandAudit {
        tenant_id: tenant_id.clone(),
        principal_id: principal_id.clone(),
        principal_kind: PrincipalKind::AgentRunner,
        receipt_id: id(ResourceKind::Receipt, suffix),
        event_id: id(ResourceKind::Event, suffix.wrapping_add(1)),
        outbox_id: id(ResourceKind::OutboxEvent, suffix.wrapping_add(2)),
        idempotency_key_digest: sha('a'),
        request_digest: sha('b'),
        receipt_expires_at: now + Duration::minutes(10),
    }
}

fn closed_object_schema(property: &str) -> ClosedSchemaDocument {
    ClosedSchemaDocument::build(json!({
        "$schema": "https://json-schema.org/draft/2020-12/schema",
        "type": "object",
        "additionalProperties": false,
        "properties": {
            property: {
                "description": "Bounded fixture field.",
                "x-platform-classification": "internal",
                "type": "string",
                "minLength": 1,
                "maxLength": 256,
                "x-platform-max-bytes": 1024
            }
        },
        "required": [property]
    }))
    .unwrap()
}

fn fixture() -> Fixture {
    let now = Utc::now();
    let limits = ModelTurnLimits::contract_fixture();
    let tenant_id = id(ResourceKind::Tenant, 1);
    let principal_id = id(ResourceKind::Principal, 2);
    let run_id = id(ResourceKind::Run, 3);
    let node_id = id(ResourceKind::NodeExecution, 4);
    let scope_id = id(ResourceKind::ScopeInstance, 5);
    let model_turn_id = id(ResourceKind::ModelTurn, 6);
    let profile_revision = version(ResourceKind::ModelProfileRevision, 10, '1');
    let provider_revision = version(ResourceKind::ModelProviderRevision, 11, '2');
    let model_deployment = deployment(ResourceKind::ModelDeployment, 12, '3');
    let provider_deployment = deployment(ResourceKind::ModelProviderDeployment, 13, '4');
    let protocol_policy = policy(30, '5');
    let parameter_schema_digest = sha('6');
    let region: DataRegion = "cn-east-1".parse().unwrap();
    let provider = ModelProviderResourceSpec {
        authoring_package: authoring(40, '7'),
        contract_digest: sha('8'),
        dependency_versions: vec![protocol_policy.clone()],
        policy_versions: vec![protocol_policy.clone()],
        installed_adapter: InstalledModelAdapter {
            qualified_name: "fixture.responses/v1".to_owned(),
            worker_manifest_digest: sha('9'),
            adapter_contract_digest: sha('a'),
        },
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
        authoring_package: authoring(41, 'b'),
        contract_digest: sha('c'),
        dependency_versions: vec![provider_revision.clone()],
        policy_versions: vec![policy(31, 'd')],
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
            tokenizer_contract_digest: sha('e'),
            estimator_contract_digest: sha('f'),
        },
        tools: ModelToolContract {
            supported: true,
            parallel: true,
            maximum_tools: 8,
            maximum_calls_per_turn: 8,
            maximum_argument_bytes: 16_384,
        },
        structured_output: StructuredOutputContract {
            native: true,
            textual_json_fallback: true,
            may_combine_with_tool_intent: false,
            maximum_schema_bytes: 65_536,
            maximum_output_bytes: 1_048_576,
        },
        parameter_schema_digest: parameter_schema_digest.clone(),
        artifact_delivery: ModelArtifactDeliveryContract {
            supported_modalities: vec![],
            provider_file_upload: false,
            maximum_artifacts: 0,
            maximum_single_artifact_bytes: 0,
            maximum_total_artifact_bytes: 0,
            remote_retention_milliseconds: 0,
        },
        usage: ModelUsageContract {
            provider_reports_usage: true,
            reports_cached_input_tokens: false,
            reports_reasoning_tokens: false,
            reports_cost: true,
            cost_currency: Some("USD".to_owned()),
            estimator_contract_digest: sha('f'),
        },
        data_handling: ProviderDataHandlingContract {
            maximum_classification: DataClassification::Confidential,
            allowed_regions: vec![region.clone()],
            maximum_retention_milliseconds: 86_400_000,
            training: ProviderTrainingPolicy::Prohibited,
            subprocessor_set_digest: sha('1'),
        },
        limits: ModelLimits {
            maximum_messages: 16,
            maximum_parts: 32,
            maximum_text_bytes: 32_768,
            maximum_artifacts: 0,
            maximum_tools: 8,
            maximum_parallel_tool_calls: 8,
            maximum_rounds: 8,
            maximum_input_tokens: 3_000,
            maximum_output_tokens: 512,
        },
        catalog_evidence: ModelCatalogEvidence {
            artifact: artifact(42, '2', "catalog"),
            source_digest: sha('3'),
            adapter_contract_digest: sha('a'),
            observed_at: now - Duration::minutes(1),
            expires_at: now + Duration::days(1),
        },
    };
    let provider_closure = ModelProviderDeploymentClosure {
        provider_revision: provider_revision.clone(),
        endpoint_identity_digest: sha('4'),
        secret_bindings: vec![exact_secret_binding(43)],
        protocol_policy: protocol_policy.clone(),
        network_policy: policy(32, '5'),
        tls_policy: policy(33, '6'),
        trust_policy: policy(34, '7'),
        data_policy: policy(35, '8'),
        region,
        conformance_evidence: artifact(44, '9', "conformance"),
    };
    let model_closure = ModelDeploymentClosure {
        profile_revision: profile_revision.clone(),
        provider_deployment: provider_deployment.clone(),
        data_policy: policy(36, 'a'),
        budget_policy: policy(37, 'b'),
        public_projection_policy: policy(38, 'c'),
        generation_defaults: ClosedJsonValue::build(
            parameter_schema_digest.clone(),
            json!({"temperature": 0}),
        )
        .unwrap(),
    };
    let principal = PrincipalSnapshot::build(
        tenant_id.clone(),
        principal_id.clone(),
        PrincipalKind::AgentRunner,
        PermissionSet::new(vec![Permission::ModelInvoke, Permission::RuntimeControl]).unwrap(),
        1,
        1,
        1,
    )
    .unwrap();
    let selection_policy = policy(39, 'd');
    let agent_closure = AgentDeploymentClosure {
        interface: version(ResourceKind::AgentInterfaceRevision, 50, 'e'),
        plan: version(ResourceKind::AgentPlanRevision, 51, 'f'),
        slots: vec![FrozenSlotBinding {
            slot_id: "primary_model".to_owned(),
            requirement_digest: sha('1'),
            target: FrozenSlotTarget::Model {
                candidates: vec![model_deployment.clone()],
                selection_policy,
            },
            binding_digest: sha('2'),
        }],
        policies: vec![policy(52, '3')],
        execution_profile: policy(53, '4'),
    };
    let run_bindings = RunBindingsSnapshot::build(
        deployment(ResourceKind::AgentDeployment, 54, '5'),
        principal.clone(),
        &agent_closure,
    )
    .unwrap();
    let argument_schema = closed_object_schema("query");
    let output_schema = closed_object_schema("answer");
    let request = CanonicalModelRequest {
        schema_version: 1,
        model_turn_id: model_turn_id.clone(),
        messages: vec![CanonicalMessage {
            role: CanonicalMessageRole::Platform,
            parts: vec![CanonicalMessagePart::Text("Answer safely.".to_owned())],
            classification: DataClassification::Internal,
            source: ModelContentSource {
                source_kind: "agent_contract".to_owned(),
                source_digest: sha('6'),
                trusted_instruction: true,
            },
        }],
        tools: vec![ModelToolProjection {
            projected_name: "search".to_owned(),
            capability_deployment: deployment(ResourceKind::CapabilityDeployment, 55, '7'),
            interface_revision: version(ResourceKind::CapabilityInterfaceRevision, 56, '8'),
            input_schema: argument_schema.clone(),
            output_schema_digest: sha('9'),
            effect: insight_platform_contracts::Effect::ReadOnly,
        }],
        response_contract: ModelResponseContract {
            output_schema_digest: output_schema.canonical_digest.clone(),
            structured_schema: Some(output_schema),
            allow_tool_intents: true,
            allow_message_with_tool_intents: false,
        },
        artifact_inputs: vec![],
        generation_parameters: ClosedJsonValue::build(
            parameter_schema_digest,
            json!({"temperature": 0}),
        )
        .unwrap(),
        max_output_tokens: 100,
        input_token_estimate: 100,
        estimator_contract_digest: sha('f'),
        source_map_digest: sha('a'),
        truncation_policy: policy(57, 'b'),
        classification: DataClassification::Internal,
        deadline: now + Duration::minutes(5),
        trace_context: SafeTraceContext {
            trace_id_digest: sha('c'),
            parent_span_id_digest: sha('d'),
        },
    };
    let request_json = serde_json::to_value(&request).unwrap();
    let request_digest = crate::types::digest(&request).unwrap();
    let command = CreateModelTurn {
        audit: audit(&tenant_id, &principal_id, 60, now),
        model_turn_id,
        run_id: run_id.clone(),
        node_execution_id: node_id,
        scope_instance_id: scope_id.clone(),
        expected_run_version: 1,
        expected_node_version: 1,
        round_ordinal: 1,
        slot_id: "primary_model".to_owned(),
        selected_candidate_ordinal: 0,
        selector_input_digest: sha('e'),
        request: ModelRequestValue {
            value_id: id(ResourceKind::RunValue, 61),
            classification: DataClassification::Internal,
            schema_digest: sha('f'),
            content_digest: request_digest,
            value: ValueRef::Inline {
                value: request_json,
            },
            artifact_link_id: None,
            request: request.clone(),
        },
        requested_attempt_limit: 3,
        cost_ceiling_microunits: 10_000,
    };
    let facts = ModelAdmissionFacts {
        run_state: insight_platform_contracts::RunState::Running,
        run_version: 1,
        run_pause_requested: false,
        run_cancel_requested: false,
        run_timeout_requested: false,
        run_deadline: request.deadline,
        run_bindings,
        node_state: insight_platform_contracts::NodeExecutionState::Running,
        node_version: 1,
        node_kind: insight_platform_contracts::PlanNodeKind::ModelLoop,
        node_scope_instance_id: scope_id,
        node_deadline: request.deadline,
        model_deployment,
        model_closure,
        model_gate_enabled: true,
        profile_revision,
        profile,
        provider_deployment,
        provider_closure,
        provider_gate_enabled: true,
        provider_revision,
        provider,
        principal,
        database_now: now,
    };
    Fixture {
        now,
        limits,
        command,
        facts,
        request,
        argument_schema,
    }
}

fn started(fixture: Fixture) -> (Fixture, PreparedModelDispatch, ResourceId, JobFence) {
    let turn = decide_model_turn_admission(&fixture.command, fixture.facts.clone(), fixture.limits)
        .unwrap();
    let prepared = decide_prepare_model_dispatch(
        &turn,
        &PrepareModelDispatch {
            audit: audit(
                &turn.tenant_id,
                &turn.payload.admission.principal.principal_id,
                70,
                fixture.now,
            ),
            model_turn_id: turn.model_turn_id.clone(),
            expected_turn_version: turn.version,
            job_id: id(ResourceKind::Job, 71),
            scheduled_at: fixture.now,
        },
        fixture.now,
        fixture.limits,
    )
    .unwrap();
    let worker = id(ResourceKind::WorkerProcessGeneration, 72);
    let reservation = id(ResourceKind::UsageReservation, 73);
    let token = sha('1');
    let started = decide_start_model_dispatch(
        &prepared.turn,
        &prepared.job,
        &prepared.job_payload,
        worker.clone(),
        token.clone(),
        reservation.clone(),
        LeasePolicy {
            requested_milliseconds: 30_000,
            hard_maximum_milliseconds: 60_000,
        },
        fixture.now,
        fixture.limits,
    )
    .unwrap();
    let fence = JobFence {
        expected_version: started.job.version,
        worker_process_generation_id: worker,
        lease_generation: started.job.lease_generation,
        token_digest: token,
    };
    (fixture, started, reservation, fence)
}

fn tool_response(fixture: &Fixture) -> CanonicalModelResponse {
    CanonicalModelResponse {
        schema_version: 1,
        message: None,
        structured_output: None,
        tool_intents: vec![ModelToolIntent {
            call_id: "call_1".to_owned(),
            projected_tool_name: "search".to_owned(),
            arguments: ClosedJsonValue::build(
                fixture.argument_schema.canonical_digest.clone(),
                json!({"query": "rust"}),
            )
            .unwrap(),
        }],
        finish_reason: CanonicalFinishReason::ToolUse,
        usage: ModelUsage {
            input_tokens: Some(50),
            output_tokens: Some(20),
            cached_input_tokens: None,
            reasoning_tokens: None,
            provider_reported_cost: Some(DecimalMoney::new("USD", 123, 6).unwrap()),
            accounting_quality: AccountingQuality::ProviderReported,
        },
        observation: ModelObservation {
            request_sent: true,
            provider_response_digest: Some(sha('2')),
            actual_model_identity: Some("fixture-model-2026-08".to_owned()),
            model_fingerprint: Some("fixture-fingerprint".to_owned()),
            possible_duplicate_charge: false,
            stream_delta_count: 0,
            stream_bytes: 0,
        },
    }
}

fn output(fixture: &Fixture, response: CanonicalModelResponse) -> ModelOutputValue {
    let value = serde_json::to_value(&response).unwrap();
    ModelOutputValue {
        value_id: id(ResourceKind::RunValue, 80),
        classification: DataClassification::Internal,
        schema_digest: fixture
            .request
            .response_contract
            .output_schema_digest
            .clone(),
        content_digest: crate::types::digest(&response).unwrap(),
        value: ValueRef::Inline { value },
        artifact_link_id: None,
        artifact_outputs: Vec::new(),
        response,
        validation_evidence_digest: sha('3'),
    }
}

#[test]
fn claimed_model_input_binds_exact_inline_bytes() {
    let fixture = fixture();
    let exact = fixture
        .command
        .request
        .exact_for(
            &fixture.command.run_id,
            &fixture.command.node_execution_id,
            fixture.limits,
        )
        .unwrap();
    let mut input = ModelExecutionInput {
        exact,
        material: ModelExecutionInputMaterial::Inline {
            value: serde_json::to_value(&fixture.request).unwrap(),
        },
    };
    input.validate().unwrap();

    input.material = ModelExecutionInputMaterial::Inline {
        value: json!({"messages": []}),
    };
    assert_eq!(input.validate(), Err(ModelTurnError::InvalidRequestValue));
}

#[test]
fn admission_dispatch_tool_intent_and_usage_are_closed() {
    let (fixture, started, reservation, fence) = started(fixture());
    let response = tool_response(&fixture);
    let decision = decide_model_outcome(
        &started.turn,
        &started.job,
        &started.job_payload,
        &fence,
        &reservation,
        &fixture.request,
        &ModelDispatchOutcome::Succeeded(Box::new(output(&fixture, response))),
        fixture.now + Duration::seconds(1),
        fixture.limits,
    )
    .unwrap();
    assert_eq!(
        decision.turn.state,
        insight_platform_contracts::ModelTurnState::Succeeded
    );
    assert_eq!(
        decision.job.state,
        insight_platform_contracts::JobState::Succeeded
    );
    assert_eq!(decision.turn.payload.attempts.len(), 1);
    assert_eq!(decision.settlement.requests_used, 1);
    assert_eq!(decision.settlement.tokens_used, 70);
    assert_eq!(decision.settlement.cost_microunits_used, 123);
    assert_eq!(decision.turn.payload.result.unwrap().tool_intent_count, 1);
}

#[test]
fn invalid_tool_arguments_and_usage_ceiling_fail_closed() {
    let (fixture, started, reservation, fence) = started(fixture());
    let mut invalid = tool_response(&fixture);
    invalid.tool_intents[0].arguments = ClosedJsonValue::build(
        fixture.argument_schema.canonical_digest.clone(),
        json!({"query": 42}),
    )
    .unwrap();
    let failure = decide_model_outcome(
        &started.turn,
        &started.job,
        &started.job_payload,
        &fence,
        &reservation,
        &fixture.request,
        &ModelDispatchOutcome::Succeeded(Box::new(output(&fixture, invalid))),
        fixture.now + Duration::seconds(1),
        fixture.limits,
    )
    .unwrap_err();
    assert_eq!(failure, ModelTurnError::SchemaValidationFailed);

    let mut excessive = tool_response(&fixture);
    excessive.usage.input_tokens = Some(180);
    excessive.usage.output_tokens = Some(40);
    let failure = decide_model_outcome(
        &started.turn,
        &started.job,
        &started.job_payload,
        &fence,
        &reservation,
        &fixture.request,
        &ModelDispatchOutcome::Succeeded(Box::new(output(&fixture, excessive))),
        fixture.now + Duration::seconds(1),
        fixture.limits,
    )
    .unwrap_err();
    assert_eq!(failure, ModelTurnError::UsageCeilingExceeded);
}

#[test]
fn retry_consumes_a_new_physical_attempt_and_reservation() {
    let (fixture, started, reservation, fence) = started(fixture());
    let measurement = ModelAttemptMeasurement {
        usage: Some(ModelUsage {
            input_tokens: Some(10),
            output_tokens: Some(0),
            cached_input_tokens: None,
            reasoning_tokens: None,
            provider_reported_cost: Some(DecimalMoney::new("USD", 10, 6).unwrap()),
            accounting_quality: AccountingQuality::ProviderReported,
        }),
        observation: ModelObservation {
            request_sent: true,
            provider_response_digest: Some(sha('4')),
            actual_model_identity: Some("fixture-model-2026-08".to_owned()),
            model_fingerprint: None,
            possible_duplicate_charge: true,
            stream_delta_count: 0,
            stream_bytes: 0,
        },
    };
    let retry = decide_model_outcome(
        &started.turn,
        &started.job,
        &started.job_payload,
        &fence,
        &reservation,
        &fixture.request,
        &ModelDispatchOutcome::RetryableFailure {
            failure: model_failure(
                insight_platform_contracts::FailureClass::External,
                insight_platform_contracts::Retryability::SafeWithinPolicy,
            ),
            retry_at: fixture.now + Duration::seconds(2),
            measurement,
        },
        fixture.now + Duration::seconds(1),
        fixture.limits,
    )
    .unwrap();
    let second_reservation = id(ResourceKind::UsageReservation, 90);
    let second = decide_start_model_dispatch(
        &retry.turn,
        &retry.job,
        &retry.job_payload,
        id(ResourceKind::WorkerProcessGeneration, 91),
        sha('5'),
        second_reservation.clone(),
        LeasePolicy {
            requested_milliseconds: 30_000,
            hard_maximum_milliseconds: 60_000,
        },
        fixture.now + Duration::seconds(2),
        fixture.limits,
    )
    .unwrap();
    assert_eq!(second.job.attempt_count, 2);
    assert_eq!(
        second.job_payload.active_usage_reservation_id,
        Some(second_reservation)
    );
    assert_eq!(
        second.turn.payload.attempts[0].usage_reservation_id,
        reservation
    );
}

#[test]
fn cancel_wins_against_late_completion_and_stream_is_fenced() {
    let (fixture, started, reservation, fence) = started(fixture());
    let controlled = decide_model_control(
        &started.turn,
        Some(&started.job),
        Some(&started.job_payload),
        ModelControlKind::Cancel,
        fixture.now + Duration::milliseconds(500),
        fixture.limits,
    )
    .unwrap();
    let late = decide_model_outcome(
        &controlled.turn,
        controlled.job.as_ref().unwrap(),
        controlled.job_payload.as_ref().unwrap(),
        &fence,
        &reservation,
        &fixture.request,
        &ModelDispatchOutcome::Succeeded(Box::new(output(&fixture, tool_response(&fixture)))),
        fixture.now + Duration::seconds(1),
        fixture.limits,
    )
    .unwrap_err();
    assert_eq!(late, ModelTurnError::FirstWinnerLost);
    let cancelling_job = controlled.job.as_ref().unwrap();
    let cancellation_fence = JobFence {
        expected_version: cancelling_job.version,
        worker_process_generation_id: fence.worker_process_generation_id.clone(),
        lease_generation: fence.lease_generation,
        token_digest: fence.token_digest.clone(),
    };
    let conservative = ModelAttemptMeasurement::conservative_dispatched(
        &controlled.turn.payload.admission,
        fixture.limits,
    )
    .unwrap();
    assert!(conservative.observation.request_sent);
    assert!(conservative.observation.possible_duplicate_charge);
    assert_eq!(
        conservative.usage.as_ref().unwrap().accounting_quality,
        AccountingQuality::Reconciled
    );
    let cancelled = decide_model_cancellation_outcome(
        &controlled.turn,
        cancelling_job,
        controlled.job_payload.as_ref().unwrap(),
        Some(&cancellation_fence),
        &reservation,
        &conservative,
        fixture.now + Duration::seconds(1),
        fixture.limits,
    )
    .unwrap();
    assert_eq!(
        cancelled.turn.state,
        insight_platform_contracts::ModelTurnState::Cancelled
    );

    let response = tool_response(&fixture);
    let mut stream = ModelStreamAccumulator::new(
        started.turn.model_turn_id.clone(),
        started.job.attempt_count,
        started.job.lease_generation,
        fixture.limits,
    )
    .unwrap();
    let accepted = stream
        .accept(NormalizedModelFrame {
            model_turn_id: started.turn.model_turn_id.clone(),
            attempt_no: started.job.attempt_count,
            lease_generation: started.job.lease_generation,
            transport_sequence: 1,
            delta: NormalizedModelDelta::Text("partial".to_owned()),
        })
        .unwrap();
    let ModelStreamAcceptance::Live {
        accepted_delta_count,
        accepted_delta_bytes,
        ..
    } = accepted
    else {
        panic!("expected live frame")
    };
    let mut terminal = response;
    terminal.observation.stream_delta_count = accepted_delta_count;
    terminal.observation.stream_bytes = accepted_delta_bytes;
    assert!(matches!(
        stream
            .accept(NormalizedModelFrame {
                model_turn_id: started.turn.model_turn_id.clone(),
                attempt_no: started.job.attempt_count,
                lease_generation: started.job.lease_generation,
                transport_sequence: 2,
                delta: NormalizedModelDelta::Terminal(Box::new(terminal)),
            })
            .unwrap(),
        ModelStreamAcceptance::Terminal { .. }
    ));
    assert_eq!(
        stream
            .accept(NormalizedModelFrame {
                model_turn_id: started.turn.model_turn_id,
                attempt_no: started.job.attempt_count,
                lease_generation: started.job.lease_generation,
                transport_sequence: 3,
                delta: NormalizedModelDelta::Text("late".to_owned()),
            })
            .unwrap_err(),
        ModelTurnError::StreamAlreadyTerminal
    );
}

#[test]
fn live_text_delta_is_closed_and_fence_bound() {
    let fixture = fixture();
    let delta = ModelLiveTextDelta {
        schema_version: 1,
        tenant_id: fixture.command.audit.tenant_id.clone(),
        run_id: fixture.command.run_id.clone(),
        model_turn_id: fixture.command.model_turn_id.clone(),
        job_id: id(ResourceKind::Job, 0x601),
        worker_process_generation_id: id(ResourceKind::WorkerProcessGeneration, 0x602),
        attempt_no: 1,
        lease_generation: 1,
        transport_sequence: 1,
        request_digest: crate::types::digest(&fixture.request).unwrap(),
        classification: fixture.request.classification,
        text: "bounded live text".to_owned(),
    };
    delta.validate(fixture.limits).unwrap();

    let mut wrong_run = delta.clone();
    wrong_run.run_id = id(ResourceKind::Job, 0x603);
    assert_eq!(
        wrong_run.validate(fixture.limits),
        Err(ModelTurnError::InvalidStream)
    );
    let mut private_delta = delta;
    private_delta.text = "contains\0nul".to_owned();
    assert_eq!(
        private_delta.validate(fixture.limits),
        Err(ModelTurnError::InvalidStream)
    );
}
