use crate::{
    id::{ResourceKind, RESOURCE_KIND_DESCRIPTORS},
    json::{canonical_digest, MAX_SAFE_JSON_INTEGER},
    nominal::{canonical_schema_digest, nominal_schema_files, pinned_nominal_reference},
    registry::{
        AgentAuthoringMode, ApiProblemCode, ArtifactGrantOperation, ArtifactPurpose,
        ArtifactReferenceKind, ArtifactWorkloadAudience, AuthnStrength, BlobIntegrityState,
        CapabilityBackendKind, CapabilityCancellationKind, CapabilityIdempotencyKind,
        CapabilityProgressDurability, CapabilityProgressMode, CodeTrustClass, ContextBackendKind,
        ContextBackendOutcomeKind, ContextCitationStrength, ContextConsistencyMode, CursorPurpose,
        DataClassification, DependencySlotKind, Effect, EventDurability, FailureClass,
        FailureSource, InteractionKind, LockRank, ManagementOperationKind,
        McpAuthorizationPrincipalKind, McpOAuthClientAuthenticationKind, McpTransportKind,
        ModelIdentityStability, ModelModality, Permission, PlanNodeKind, PlatformFailureCode,
        PolicyKind, PolicyReferenceRole, PrincipalKind, PublicRunEventSourceKind,
        PublicRunEventType, QuotaAccountingMode, QuotaDimension, QuotaScopeKind, QuotaWindowKind,
        Retryability, SandboxAbiVersion, SandboxCleanupPolicy, SandboxEntrypointKind,
        SandboxIsolationClass, SandboxRuntimeFamily, SchedulerPriority, ScopeKind, ServiceClass,
        SkillInstructionAudience, SkillInstructionPhase, SkillPackageEntryKind,
        SkillRequirementKind, SkillSelectionMode, WakeContractKind, WorkClass,
    },
    schema::{ALLOWED_SCHEMA_KEYWORDS, CLOSED_SCHEMA_PROFILE_ID},
    state::{all_state_machines, AttemptCommitDisposition},
    types::{MAX_ARTIFACT_BYTES, MAX_PUBLIC_EVENT_SAFE_SUMMARY_BYTES},
    MAX_CANDIDATE_COMPONENT_IMAGES, MAX_CANDIDATE_WORKER_MANIFESTS,
};
use serde::Serialize;
use serde_json::{json, Value};
use std::{
    collections::BTreeMap,
    error::Error,
    fmt, fs,
    path::{Path, PathBuf},
};

pub const CONTRACT_ROOT: &str = "contracts/platform-v1";

const PLATFORM_V1_OPENAPI: &str = r##"openapi: 3.1.0
info:
  title: Insight Platform API
  version: 1.0.0-implementing
  description: >-
    Target insight.platform/v1 contract. Operations remain implementing-not-current until
    qualification and clean replacement are complete.
x-insight-contract-status: implementing-not-current
servers:
  - url: /v1
paths:
  /mcp/oauth/callback:
    get:
      operationId: completeMcpOAuthCallback
      summary: Complete an MCP OAuth authorization redirect
      description: >-
        Fixed public redirect authenticated by the encrypted one-time state value. Exactly one of
        code or error is required. Duplicate and unknown query fields are rejected. The response
        never reflects callback values or internal failure details.
      tags:
        - MCP OAuth Callback
      security: []
      x-insight-authentication: oauth_callback_state
      x-insight-permission: none
      x-insight-idempotency: callback_receipt
      x-insight-rate-class: internal_callback
      x-insight-audit: callback_receipt_event_outbox
      x-insight-maximum-raw-query-bytes: 8192
      x-insight-exactly-one-query-parameter:
        - code
        - error
      parameters:
        - name: state
          in: query
          required: true
          schema:
            type: string
            minLength: 1
            maxLength: 4096
        - name: code
          in: query
          required: false
          schema:
            type: string
            minLength: 1
            maxLength: 8192
        - name: error
          in: query
          required: false
          schema:
            type: string
            minLength: 1
            maxLength: 128
        - name: iss
          in: query
          required: false
          schema:
            type: string
            minLength: 1
            maxLength: 2048
      responses:
        "200":
          description: The callback has one durable authorized or declined winner.
          headers:
            Cache-Control:
              $ref: "#/components/headers/NoStore"
            Referrer-Policy:
              $ref: "#/components/headers/NoReferrer"
          content:
            text/plain:
              schema:
                type: string
                enum:
                  - MCP authorization response received. You may close this window.
        "202":
          description: External preparation or database commit is uncertain; durable reconciliation applies.
          headers:
            Cache-Control:
              $ref: "#/components/headers/NoStore"
            Referrer-Policy:
              $ref: "#/components/headers/NoReferrer"
          content:
            text/plain:
              schema:
                type: string
                enum:
                  - The MCP authorization response is being processed.
        "400":
          description: The bounded callback query or authenticated binding was rejected.
          headers:
            Cache-Control:
              $ref: "#/components/headers/NoStore"
            Referrer-Policy:
              $ref: "#/components/headers/NoReferrer"
          content:
            text/plain:
              schema:
                type: string
                enum:
                  - The MCP authorization response could not be accepted.
        "405":
          description: Only GET is accepted and the request body must be empty.
          headers:
            Allow:
              schema:
                type: string
                const: GET
            Cache-Control:
              $ref: "#/components/headers/NoStore"
          content:
            text/plain:
              schema:
                type: string
                enum:
                  - The MCP authorization response could not be accepted.
        "500":
          description: A fail-closed internal invariant prevented a safe callback projection.
          headers:
            Cache-Control:
              $ref: "#/components/headers/NoStore"
          content:
            text/plain:
              schema:
                type: string
                enum:
                  - The MCP authorization response could not be processed.
        "503":
          description: A required callback dependency is temporarily unavailable before a durable winner.
          headers:
            Cache-Control:
              $ref: "#/components/headers/NoStore"
            Retry-After:
              schema:
                type: string
                const: "1"
          content:
            text/plain:
              schema:
                type: string
                enum:
                  - The MCP authorization service is temporarily unavailable.
components:
  headers:
    NoStore:
      schema:
        type: string
        const: no-store
    NoReferrer:
      schema:
        type: string
        const: no-referrer
  schemas:
    PlatformResourceId:
      $ref: ./schemas/resource-id.schema.json
    Digest:
      $ref: ./schemas/nominal/digest.schema.json
    UtcTimestamp:
      $ref: ./schemas/nominal/utc-timestamp.schema.json
    DecimalMoney:
      $ref: ./schemas/nominal/decimal-money.schema.json
    ArtifactRef:
      $ref: ./schemas/nominal/artifact-ref.schema.json
    Failure:
      $ref: ./schemas/nominal/failure.schema.json
    ApiProblem:
      $ref: ./schemas/nominal/api-problem.schema.json
    OpaqueListCursor:
      $ref: ./schemas/nominal/opaque-list-cursor.schema.json
    OpaqueRunEventCursor:
      $ref: ./schemas/nominal/opaque-run-event-cursor.schema.json
    DurablePublicRunEventPayload:
      $ref: ./events/public-run-payloads.schema.json
"##;

pub const EXECUTION_WORK_OWNER_PAIRS: &[(WorkClass, ResourceKind)] = &[
    (
        WorkClass::RegistryValidation,
        ResourceKind::ManagementOperation,
    ),
    (WorkClass::Orchestration, ResourceKind::NodeExecution),
    (WorkClass::Model, ResourceKind::ModelTurn),
    (
        WorkClass::CapabilityNative,
        ResourceKind::CapabilityInvocation,
    ),
    (
        WorkClass::CapabilityRemote,
        ResourceKind::CapabilityInvocation,
    ),
    (WorkClass::Mcp, ResourceKind::McpOperation),
    (WorkClass::Context, ResourceKind::ContextQuery),
    (WorkClass::Sandbox, ResourceKind::SandboxJob),
    (WorkClass::Interaction, ResourceKind::Interaction),
    (WorkClass::Artifact, ResourceKind::ManagementOperation),
    (WorkClass::Artifact, ResourceKind::InternalBlob),
    (WorkClass::Recovery, ResourceKind::Run),
    (WorkClass::Recovery, ResourceKind::NodeExecution),
    (WorkClass::Recovery, ResourceKind::CapabilityInvocation),
    (WorkClass::Recovery, ResourceKind::ContextQuery),
    (WorkClass::Recovery, ResourceKind::McpOperation),
    (WorkClass::Recovery, ResourceKind::ModelTurn),
    (WorkClass::Recovery, ResourceKind::SandboxJob),
    (WorkClass::Recovery, ResourceKind::ManagementOperation),
];

pub const fn is_execution_work_owner_pair(work_class: WorkClass, owner_kind: ResourceKind) -> bool {
    let mut index = 0;
    while index < EXECUTION_WORK_OWNER_PAIRS.len() {
        let candidate = EXECUTION_WORK_OWNER_PAIRS[index];
        if candidate.0 as u8 == work_class as u8 && candidate.1 as u8 == owner_kind as u8 {
            return true;
        }
        index += 1;
    }
    false
}

pub const CONTRACT_MANIFEST_INPUTS: &[&str] = &[
    "contracts/platform-v1/openapi.yaml",
    "contracts/platform-v1/registries.json",
    "contracts/platform-v1/errors.json",
    "contracts/platform-v1/events/public-run-events.json",
    "contracts/platform-v1/events/public-run-payloads.schema.json",
    "contracts/platform-v1/schemas/closed-schema-profile.json",
    "contracts/platform-v1/schemas/resource-id.schema.json",
    "contracts/platform-v1/schemas/states.json",
    "contracts/platform-v1/schemas/nominal-types.json",
    "contracts/platform-v1/schemas/frozen-slot-binding.schema.json",
    "contracts/platform-v1/schemas/worker-manifest.schema.json",
    "contracts/platform-v1/schemas/candidate-manifest.schema.json",
    "contracts/platform-v1/schemas/policies/artifact-retention-policy.schema.json",
    "contracts/platform-v1/schemas/policies/model-output-artifact-io-policy.schema.json",
    "contracts/platform-v1/schemas/policies/scheduling-policy.schema.json",
    "contracts/platform-v1/schemas/nominal/api-problem.schema.json",
    "contracts/platform-v1/schemas/nominal/artifact-ref.schema.json",
    "contracts/platform-v1/schemas/nominal/decimal-money.schema.json",
    "contracts/platform-v1/schemas/nominal/digest.schema.json",
    "contracts/platform-v1/schemas/nominal/failure.schema.json",
    "contracts/platform-v1/schemas/nominal/opaque-list-cursor.schema.json",
    "contracts/platform-v1/schemas/nominal/opaque-run-event-cursor.schema.json",
    "contracts/platform-v1/schemas/nominal/utc-timestamp.schema.json",
    "contracts/platform-v1/schemas/nominal/uuid-v7-id.schema.json",
    "contracts/platform-v1/limits/hard-limit-profile.schema.json",
    "contracts/platform-v1/limits/q1-50.json",
    "contracts/platform-v1/fixtures/manifest.json",
    "contracts/platform-v1/examples/foundation-scalars.json",
    "contracts/platform-v1/examples/q1-orchestration-worker-manifest.json",
    "proto/insight/platform/v1/foundation.proto",
];

fn wire_values<T>(items: &[T], to_wire: impl Fn(&T) -> &'static str) -> Vec<&'static str> {
    items.iter().map(to_wire).collect()
}

pub fn generated_contracts() -> BTreeMap<&'static str, Vec<u8>> {
    let registries = json!({
        "profile": "insight.platform/v1",
        "resource_kinds": RESOURCE_KIND_DESCRIPTORS,
        "revision_prefixes": crate::id::ResourceKind::ALL.iter().filter(|kind| kind.is_revision()).map(|kind| kind.descriptor().prefix).collect::<Vec<_>>(),
        "deployment_prefixes": crate::id::ResourceKind::ALL.iter().filter(|kind| kind.is_deployment()).map(|kind| kind.descriptor().prefix).collect::<Vec<_>>(),
        "exact_registry_projection_kinds": crate::id::ResourceKind::ALL.iter()
            .filter(|kind| kind.supports_exact_registry_projection())
            .map(|kind| {
                let descriptor = kind.descriptor();
                json!({"resource_kind": descriptor.name, "prefix": descriptor.prefix})
            })
            .collect::<Vec<_>>(),
        "principal_kinds": wire_values(PrincipalKind::ALL, |value| value.as_str()),
        "authentication_strengths": wire_values(AuthnStrength::ALL, |value| value.as_str()),
        "policy_kinds": wire_values(PolicyKind::ALL, |value| value.as_str()),
        "policy_reference_roles": PolicyReferenceRole::ALL.iter().map(|role| {
            json!({
                "role": role.as_str(),
                "expected_policy_kind": role.expected_kind().as_str(),
                "required_revision_prefix": "prev"
            })
        }).collect::<Vec<_>>(),
        "permissions": wire_values(Permission::ALL, |value| value.as_str()),
        "effects": wire_values(Effect::ALL, |value| value.as_str()),
        "data_classifications": wire_values(DataClassification::ALL, |value| value.as_str()),
        "code_trust_classes": wire_values(CodeTrustClass::ALL, |value| value.as_str()),
        "cursor_purposes": wire_values(CursorPurpose::ALL, |value| value.as_str()),
        "lock_ranks": LockRank::ALL.iter().map(|rank| {
            json!({"name": rank.as_str(), "ordinal": rank.ordinal()})
        }).collect::<Vec<_>>(),
        "work_classes": wire_values(WorkClass::ALL, |value| value.as_str()),
        "artifact_purposes": wire_values(ArtifactPurpose::ALL, |value| value.as_str()),
        "artifact_reference_kinds": wire_values(ArtifactReferenceKind::ALL, |value| value.as_str()),
        "artifact_grant_operations": wire_values(ArtifactGrantOperation::ALL, |value| value.as_str()),
        "artifact_workload_audiences": wire_values(ArtifactWorkloadAudience::ALL, |value| value.as_str()),
        "blob_integrity_states": wire_values(BlobIntegrityState::ALL, |value| value.as_str()),
        "management_operation_kinds": wire_values(ManagementOperationKind::ALL, |value| value.as_str()),
        "plan_node_kinds": wire_values(PlanNodeKind::ALL, |value| value.as_str()),
        "scope_kinds": wire_values(ScopeKind::ALL, |value| value.as_str()),
        "wake_contract_kinds": wire_values(WakeContractKind::ALL, |value| value.as_str()),
        "interaction_kinds": wire_values(InteractionKind::ALL, |value| value.as_str()),
        "scheduler_priorities": wire_values(SchedulerPriority::ALL, |value| value.as_str()),
        "service_classes": wire_values(ServiceClass::ALL, |value| value.as_str()),
        "execution_work_owner_pairs": EXECUTION_WORK_OWNER_PAIRS.iter().map(|(work_class, owner_kind)| {
            json!({"work_class": work_class.as_str(), "owner_kind": owner_kind.descriptor().name})
        }).collect::<Vec<_>>(),
        "agent_authoring_modes": wire_values(AgentAuthoringMode::ALL, |value| value.as_str()),
        "dependency_slot_kinds": wire_values(DependencySlotKind::ALL, |value| value.as_str()),
        "capability_backend_kinds": wire_values(CapabilityBackendKind::ALL, |value| value.as_str()),
        "capability_idempotency_kinds": wire_values(CapabilityIdempotencyKind::ALL, |value| value.as_str()),
        "capability_cancellation_kinds": wire_values(CapabilityCancellationKind::ALL, |value| value.as_str()),
        "capability_progress_modes": wire_values(CapabilityProgressMode::ALL, |value| value.as_str()),
        "capability_progress_durabilities": wire_values(CapabilityProgressDurability::ALL, |value| value.as_str()),
        "skill_instruction_phases": wire_values(SkillInstructionPhase::ALL, |value| value.as_str()),
        "skill_instruction_audiences": wire_values(SkillInstructionAudience::ALL, |value| value.as_str()),
        "skill_requirement_kinds": wire_values(SkillRequirementKind::ALL, |value| value.as_str()),
        "skill_package_entry_kinds": wire_values(SkillPackageEntryKind::ALL, |value| value.as_str()),
        "skill_selection_modes": wire_values(SkillSelectionMode::ALL, |value| value.as_str()),
        "context_backend_kinds": wire_values(ContextBackendKind::ALL, |value| value.as_str()),
        "context_consistency_modes": wire_values(ContextConsistencyMode::ALL, |value| value.as_str()),
        "context_citation_strengths": wire_values(ContextCitationStrength::ALL, |value| value.as_str()),
        "context_backend_outcome_kinds": wire_values(ContextBackendOutcomeKind::ALL, |value| value.as_str()),
        "mcp_transport_kinds": wire_values(McpTransportKind::ALL, |value| value.as_str()),
        "mcp_authorization_principal_kinds": wire_values(McpAuthorizationPrincipalKind::ALL, |value| value.as_str()),
        "mcp_oauth_client_authentication_kinds": wire_values(McpOAuthClientAuthenticationKind::ALL, |value| value.as_str()),
        "model_identity_stabilities": wire_values(ModelIdentityStability::ALL, |value| value.as_str()),
        "model_modalities": wire_values(ModelModality::ALL, |value| value.as_str()),
        "sandbox_runtime_families": wire_values(SandboxRuntimeFamily::ALL, |value| value.as_str()),
        "sandbox_isolation_classes": SandboxIsolationClass::ALL.iter().map(|value| {
            json!({"name": value.as_str(), "security_rank": value.security_rank()})
        }).collect::<Vec<_>>(),
        "sandbox_abi_versions": wire_values(SandboxAbiVersion::ALL, |value| value.as_str()),
        "sandbox_cleanup_policies": wire_values(SandboxCleanupPolicy::ALL, |value| value.as_str()),
        "sandbox_entrypoint_kinds": wire_values(SandboxEntrypointKind::ALL, |value| value.as_str()),
        "quota_accounting_modes": wire_values(QuotaAccountingMode::ALL, |value| value.as_str()),
        "quota_scope_kinds": wire_values(QuotaScopeKind::ALL, |value| value.as_str()),
        "quota_window_kinds": wire_values(QuotaWindowKind::ALL, |value| value.as_str()),
        "quota_dimensions": QuotaDimension::ALL.iter().map(|dimension| {
            json!({
                "accounting_mode": dimension.accounting_mode().as_str(),
                "hard_limit_path": dimension.as_str(),
                "unit": dimension.unit()
            })
        }).collect::<Vec<_>>(),
        "active_head_targets": [
            {"owner_prefix": "agt", "target_prefix": "adep"},
            {"owner_prefix": "skl", "target_prefix": "srev"},
            {"owner_prefix": "cap", "target_prefix": "cdep"},
            {"owner_prefix": "ctx", "target_prefix": "xdep"},
            {"owner_prefix": "dset", "target_prefix": "dgen"},
            {"owner_prefix": "mcp", "target_prefix": "mcdep"},
            {"owner_prefix": "mpr", "target_prefix": "mpdep"},
            {"owner_prefix": "mdl", "target_prefix": "mdep"},
            {"owner_prefix": "pol", "target_prefix": "prev"},
            {"owner_prefix": "srt", "target_prefix": "srrev"},
            {"owner_prefix": "spk", "target_prefix": "sprev"},
            {"owner_prefix": "sxp", "target_prefix": "sxrev"}
        ],
        "slot_target_prefixes": {
            "model": ["mdep"],
            "capability": ["cdep"],
            "context": ["xcb"],
            "child_agent": ["adep"],
            "skill": ["srev"]
        }
    });
    let errors = json!({
        "profile": "insight.platform/v1",
        "failure": {
            "classes": wire_values(FailureClass::ALL, |value| value.as_str()),
            "platform_codes": wire_values(PlatformFailureCode::ALL, |value| value.as_str()),
            "retryability": wire_values(Retryability::ALL, |value| value.as_str()),
            "sources": wire_values(FailureSource::ALL, |value| value.as_str()),
            "declared_code_pattern": "^[a-z][a-z0-9_]{0,63}$",
            "declared_codes_must_not_shadow_platform_codes": true
        },
        "api_problem": {
            "codes": wire_values(ApiProblemCode::ALL, |value| value.as_str()),
            "media_type": "application/problem+json"
        }
    });
    let events = json!({
        "profile": "insight.platform/v1",
        "durability": wire_values(EventDurability::ALL, |value| value.as_str()),
        "durable_source_kinds": wire_values(PublicRunEventSourceKind::ALL, |value| value.as_str()),
        "event_types": PublicRunEventType::ALL.iter().map(|event_type| {
            json!({
                "type": event_type.as_str(),
                "allowed_durability": event_type.allowed_durability().iter().map(|value| value.as_str()).collect::<Vec<_>>(),
                "durable_source_kind": event_type.durable_source_kind().map(|value| value.as_str())
            })
        }).collect::<Vec<_>>(),
        "envelope_invariants": {
            "snapshot": {"event_id": "absent", "sequence": "absent", "cursor": "high_water"},
            "durable": {"event_id": "required", "sequence": "required", "cursor": "required"},
            "live_only": {"event_id": "absent", "sequence": "absent", "cursor": "absent"}
        }
    });
    let schema_profile = json!({
        "profile_id": CLOSED_SCHEMA_PROFILE_ID,
        "json_schema_dialect": "https://json-schema.org/draft/2020-12/schema",
        "allowed_keywords": ALLOWED_SCHEMA_KEYWORDS,
        "root_type": "object",
        "object_additional_properties": false,
        "maximum_interoperable_integer": crate::json::MAX_SAFE_JSON_INTEGER,
        "references": ["acyclic_local_defs", "digest_pinned_platform_nominal"],
        "forbidden_features": [
            "remote_ref", "recursive_ref", "default", "coercion", "pattern", "open_format",
            "anyOf", "allOf", "not", "conditionals", "unevaluated", "unknown_keyword"
        ],
        "nominal_types": [
            "ArtifactRef", "Message", "Citation", "Failure", "UtcTimestamp", "UuidV7Id", "DecimalMoney"
        ]
    });
    let resource_id_schema = json!({
        "$schema": "https://json-schema.org/draft/2020-12/schema",
        "$id": "urn:insight:platform:v1:resource-id",
        "title": "PlatformResourceId",
        "type": "string",
        "description": "Known resource prefix followed by one canonical lowercase RFC 9562 UUIDv7. Field kind validation is additional and mandatory.",
        "pattern": format!(
            "^({})_[0-9a-f]{{8}}-[0-9a-f]{{4}}-7[0-9a-f]{{3}}-[89ab][0-9a-f]{{3}}-[0-9a-f]{{12}}$",
            RESOURCE_KIND_DESCRIPTORS.iter().map(|item| item.prefix).collect::<Vec<_>>().join("|")
        )
    });
    let frozen_slot_binding_schema = frozen_slot_binding_schema();
    let worker_manifest_schema = worker_manifest_schema();
    let candidate_manifest_schema = candidate_manifest_schema();
    let artifact_retention_policy_schema = artifact_retention_policy_schema();
    let scheduling_policy_schema = scheduling_policy_schema();
    let public_run_payload_schema = durable_public_run_payload_schema();
    let states = json!({
        "attempt_commit_dispositions": AttemptCommitDisposition::ALL
            .iter()
            .map(|value| value.as_str())
            .collect::<Vec<_>>(),
        "state_machines": all_state_machines()
    });
    let nominal_types = json!({
        "profile": "insight.platform/v1",
        "reference_form": "urn:insight:platform:v1:nominal:<name>@sha256:<canonical-schema-digest>",
        "parameterized_types": [
            {
                "name": "ValueRef",
                "reason": "the inline branch is parameterized by the owning closed payload schema; the artifact branch uses ArtifactRef"
            }
        ],
        "schemas": nominal_schema_files().iter().map(|(name, (path, schema))| {
            json!({
                "name": name,
                "path": path,
                "canonical_digest": canonical_schema_digest(schema),
                "pinned_reference": pinned_nominal_reference(name).expect("registered nominal schema")
            })
        }).collect::<Vec<_>>()
    });

    let mut contracts = BTreeMap::from([
        ("openapi.yaml", PLATFORM_V1_OPENAPI.as_bytes().to_vec()),
        ("registries.json", pretty(&registries)),
        ("errors.json", pretty(&errors)),
        ("events/public-run-events.json", pretty(&events)),
        (
            "events/public-run-payloads.schema.json",
            pretty(&public_run_payload_schema),
        ),
        ("schemas/nominal-types.json", pretty(&nominal_types)),
        (
            "schemas/closed-schema-profile.json",
            pretty(&schema_profile),
        ),
        (
            "schemas/resource-id.schema.json",
            pretty(&resource_id_schema),
        ),
        (
            "schemas/frozen-slot-binding.schema.json",
            pretty(&frozen_slot_binding_schema),
        ),
        (
            "schemas/worker-manifest.schema.json",
            pretty(&worker_manifest_schema),
        ),
        (
            "schemas/candidate-manifest.schema.json",
            pretty(&candidate_manifest_schema),
        ),
        (
            "schemas/policies/artifact-retention-policy.schema.json",
            pretty(&artifact_retention_policy_schema),
        ),
        (
            "schemas/policies/model-output-artifact-io-policy.schema.json",
            pretty(&model_output_artifact_io_policy_schema()),
        ),
        (
            "schemas/policies/scheduling-policy.schema.json",
            pretty(&scheduling_policy_schema),
        ),
        ("schemas/states.json", pretty(&states)),
    ]);
    for (_, (path, schema)) in nominal_schema_files() {
        contracts.insert(path, pretty(&schema));
    }
    contracts
}

fn artifact_retention_policy_schema() -> Value {
    json!({
        "$schema": "https://json-schema.org/draft/2020-12/schema",
        "$id": "urn:insight:platform:v1:artifact-retention-policy",
        "title": "ArtifactRetentionPolicy",
        "description": "Closed Artifact retention and two-phase deletion policy. Legal holds and live references remain non-overridable database predicates.",
        "type": "object",
        "additionalProperties": false,
        "required": [
            "version",
            "minimum_retention_seconds",
            "gc_grace_seconds",
            "tombstone_retention_seconds",
            "retain_provenance_sources",
            "delete_requires_approval"
        ],
        "properties": {
            "version": {"type": "integer", "const": 1},
            "minimum_retention_seconds": {
                "type": "integer", "minimum": 0, "maximum": 3155760000_u64
            },
            "gc_grace_seconds": {
                "type": "integer", "minimum": 1, "maximum": 31536000_u64
            },
            "tombstone_retention_seconds": {
                "type": "integer", "minimum": 1, "maximum": 3155760000_u64
            },
            "retain_provenance_sources": {"type": "boolean"},
            "delete_requires_approval": {"type": "boolean"}
        }
    })
}

fn model_output_artifact_io_policy_schema() -> Value {
    json!({
        "$schema": "https://json-schema.org/draft/2020-12/schema",
        "$id": "urn:insight:platform:v1:model-output-artifact-io-policy",
        "title": "ModelOutputArtifactIoPolicyDocument",
        "description": "Closed Artifact I/O policy for canonical Artifact-backed Model responses. Candidate storage and effective HardLimit facts are validated separately at admission.",
        "type": "object",
        "additionalProperties": false,
        "required": [
            "schema_version",
            "staging_grace_seconds",
            "verified_media_type",
            "classification_ceiling",
            "maximum_materialized_bytes",
            "storage_binding_digest",
            "encryption_domain_id",
            "content_validation_profile_digest"
        ],
        "properties": {
            "schema_version": {"type": "integer", "const": 1},
            "staging_grace_seconds": {
                "type": "integer", "minimum": 1, "maximum": MAX_SAFE_JSON_INTEGER
            },
            "verified_media_type": {"type": "string", "const": "application/json"},
            "classification_ceiling": {
                "type": "string",
                "enum": wire_values(DataClassification::ALL, |value| value.as_str())
            },
            "maximum_materialized_bytes": {
                "type": "integer", "minimum": 1, "maximum": MAX_ARTIFACT_BYTES
            },
            "storage_binding_digest": {
                "type": "string", "pattern": "^sha256:[0-9a-f]{64}$"
            },
            "encryption_domain_id": {
                "type": "string",
                "pattern": "^enc_[0-9a-f]{8}-[0-9a-f]{4}-7[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$"
            },
            "content_validation_profile_digest": {
                "type": "string", "pattern": "^sha256:[0-9a-f]{64}$"
            }
        }
    })
}

fn worker_manifest_schema() -> Value {
    json!({
        "$schema": "https://json-schema.org/draft/2020-12/schema",
        "$id": "urn:insight:platform:v1:worker-manifest",
        "title": "WorkerManifest",
        "description": "Immutable per-role local worker capacity. It is referenced by CandidateManifest and is not a durable quota authority.",
        "type": "object",
        "additionalProperties": false,
        "required": [
            "manifest_version",
            "worker_role",
            "work_class",
            "adapter_runtime_digest",
            "protocol_version",
            "max_concurrency",
            "critical_control_reserved_slots"
        ],
        "properties": {
            "manifest_version": {"type": "integer", "const": 1},
            "worker_role": {
                "type": "string",
                "minLength": 1,
                "maxLength": 128,
                "pattern": "^[a-z][a-z0-9_.-]{0,127}$",
                "x-platform-max-bytes": 128
            },
            "work_class": {
                "type": "string",
                "enum": wire_values(WorkClass::ALL, |value| value.as_str())
            },
            "adapter_runtime_digest": {
                "type": "string",
                "pattern": "^sha256:[0-9a-f]{64}$"
            },
            "protocol_version": {"type": "integer", "const": 1},
            "max_concurrency": {"type": "integer", "minimum": 1, "maximum": 65535},
            "critical_control_reserved_slots": {
                "type": "integer",
                "minimum": 1,
                "maximum": 65535
            }
        }
    })
}

fn candidate_manifest_schema() -> Value {
    let digest = json!({
        "type": "string",
        "pattern": "^sha256:[0-9a-f]{64}$"
    });
    json!({
        "$schema": "https://json-schema.org/draft/2020-12/schema",
        "$id": "urn:insight:platform:v1:candidate-manifest",
        "title": "CandidateManifest",
        "description": "Immutable digest closure that binds every Gate A-G result to one exact source, contract, schema, image, worker, configuration, limit, policy and qualification profile set.",
        "type": "object",
        "additionalProperties": false,
        "required": [
            "candidate_id",
            "git_commit",
            "contract_digest",
            "database_schema_version",
            "component_images",
            "worker_manifests",
            "deployment_config_digest",
            "hard_limit_profile_digest",
            "policy_baseline_digest",
            "qualification_profile",
            "created_at"
        ],
        "properties": {
            "candidate_id": {
                "type": "string",
                "pattern": "^cand_[0-9a-f]{8}-[0-9a-f]{4}-7[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$"
            },
            "git_commit": {
                "type": "string",
                "pattern": "^(sha1:[0-9a-f]{40}|sha256:[0-9a-f]{64})$"
            },
            "contract_digest": digest.clone(),
            "database_schema_version": {
                "type": "integer",
                "minimum": 1,
                "maximum": u32::MAX
            },
            "component_images": {
                "type": "object",
                "minProperties": 1,
                "maxProperties": MAX_CANDIDATE_COMPONENT_IMAGES,
                "propertyNames": {
                    "pattern": "^[a-z][a-z0-9_.-]{0,127}$"
                },
                "additionalProperties": digest.clone()
            },
            "worker_manifests": {
                "type": "array",
                "minItems": 1,
                "maxItems": MAX_CANDIDATE_WORKER_MANIFESTS,
                "uniqueItems": true,
                "description": "Canonical ascending digest set; Rust validation additionally enforces ordering and exact installed WorkerManifest closure.",
                "items": digest.clone()
            },
            "deployment_config_digest": digest.clone(),
            "hard_limit_profile_digest": digest.clone(),
            "policy_baseline_digest": digest,
            "qualification_profile": {
                "type": "string",
                "pattern": "^qpr_[0-9a-f]{8}-[0-9a-f]{4}-7[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$"
            },
            "created_at": {
                "type": "string",
                "format": "date-time",
                "pattern": "^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}\\.[0-9]{6}Z$"
            }
        }
    })
}

fn scheduling_policy_schema() -> Value {
    json!({
        "$schema": "https://json-schema.org/draft/2020-12/schema",
        "$id": "urn:insight:platform:v1:scheduling-policy",
        "title": "SchedulingPolicy",
        "description": "Closed tenant/work-class fairness policy. Runtime hard limits may only tighten these machine maxima.",
        "type": "object",
        "additionalProperties": false,
        "required": ["version", "weight", "burst", "aging_rounds"],
        "properties": {
            "version": {"type": "integer", "const": 1},
            "weight": {"type": "integer", "minimum": 1, "maximum": 65535},
            "burst": {"type": "integer", "minimum": 1, "maximum": 65535},
            "aging_rounds": {"type": "integer", "minimum": 1, "maximum": 65535}
        }
    })
}

fn durable_public_run_payload_schema() -> Value {
    let data_schema = |source_kind: Option<PublicRunEventSourceKind>| {
        let source_kind_schema = source_kind.map_or_else(
            || {
                json!({
                    "type": "string",
                    "enum": wire_values(PublicRunEventSourceKind::ALL, |value| value.as_str()),
                    "minLength": 1,
                    "maxLength": 32,
                    "x-platform-max-bytes": 32
                })
            },
            |source_kind| {
                json!({
                    "type": "string",
                    "const": source_kind.as_str(),
                    "minLength": 1,
                    "maxLength": 32,
                    "x-platform-max-bytes": 32
                })
            },
        );
        json!({
            "type": "object",
            "additionalProperties": false,
            "required": ["source_kind", "source_id", "source_projection_version", "safe_summary"],
            "properties": {
                "source_kind": source_kind_schema,
                "source_id": {
                    "type": "string",
                    "minLength": 40,
                    "maxLength": 43,
                    "x-platform-max-bytes": 43
                },
                "source_projection_version": {
                    "type": "integer",
                    "minimum": 1,
                    "maximum": MAX_SAFE_JSON_INTEGER
                },
                "safe_summary": {
                    "oneOf": [
                        {
                            "type": "string",
                            "minLength": 1,
                            "maxLength": MAX_PUBLIC_EVENT_SAFE_SUMMARY_BYTES,
                            "x-platform-max-bytes": MAX_PUBLIC_EVENT_SAFE_SUMMARY_BYTES
                        },
                        {"type": "null"}
                    ]
                }
            }
        })
    };
    let durable_events = PublicRunEventType::ALL
        .iter()
        .filter_map(|event_type| {
            event_type.durable_source_kind().map(|source_kind| {
                json!({
                    "type": "object",
                    "additionalProperties": false,
                    "required": ["event_type", "data"],
                    "properties": {
                        "event_type": {
                            "type": "string",
                            "const": event_type.as_str(),
                            "minLength": 1,
                            "maxLength": 64,
                            "x-platform-max-bytes": 64
                        },
                        "data": data_schema(Some(source_kind))
                    }
                })
            })
        })
        .collect::<Vec<_>>();
    let durable_event_types = PublicRunEventType::ALL
        .iter()
        .filter(|event_type| event_type.durable_source_kind().is_some())
        .map(|event_type| event_type.as_str())
        .collect::<Vec<_>>();
    json!({
        "$schema": "https://json-schema.org/draft/2020-12/schema",
        "$id": "urn:insight:platform:v1:durable-public-run-event-payload",
        "title": "DurablePublicRunEventPayload",
        "description": "Closed safe source projection shared by Rust, SQL, OpenAPI and F-EVENT. Snapshot and live-only payloads are not durable projections.",
        "type": "object",
        "additionalProperties": false,
        "required": ["event_type", "data"],
        "properties": {
            "event_type": {
                "type": "string",
                "enum": durable_event_types,
                "minLength": 1,
                "maxLength": 64,
                "x-platform-max-bytes": 64
            },
            "data": data_schema(None)
        },
        "oneOf": durable_events
    })
}

fn frozen_slot_binding_schema() -> Value {
    let digest = json!({
        "type": "string",
        "pattern": "^sha256:[0-9a-f]{64}$"
    });
    let exact_deployment = |resource_kind: &str, prefix: &str| {
        json!({
            "type": "object",
            "additionalProperties": false,
            "required": ["deployment_id", "resource_kind", "deployment_digest"],
            "properties": {
                "deployment_id": {
                    "type": "string",
                    "pattern": format!("^{prefix}_[0-9a-f]{{8}}-[0-9a-f]{{4}}-7[0-9a-f]{{3}}-[89ab][0-9a-f]{{3}}-[0-9a-f]{{12}}$")
                },
                "resource_kind": {"const": resource_kind},
                "deployment_digest": digest.clone()
            }
        })
    };
    let exact_revision = |resource_kind: &str, prefix: &str| {
        json!({
            "type": "object",
            "additionalProperties": false,
            "required": ["revision_id", "resource_kind", "semantic_digest"],
            "properties": {
                "revision_id": {
                    "type": "string",
                    "pattern": format!("^{prefix}_[0-9a-f]{{8}}-[0-9a-f]{{4}}-7[0-9a-f]{{3}}-[89ab][0-9a-f]{{3}}-[0-9a-f]{{12}}$")
                },
                "resource_kind": {"const": resource_kind},
                "semantic_digest": digest.clone()
            }
        })
    };
    let selection_policy = exact_revision("policy_revision", "prev");
    let dataset_id = json!({
        "type": "string",
        "pattern": "^dset_[0-9a-f]{8}-[0-9a-f]{4}-7[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$"
    });
    let generation_id = json!({
        "type": "string",
        "pattern": "^dgen_[0-9a-f]{8}-[0-9a-f]{4}-7[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$"
    });
    let consistency = json!({
        "oneOf": [
            {
                "type": "object",
                "additionalProperties": false,
                "required": ["mode", "generation"],
                "properties": {
                    "mode": {"const": "pinned_generation"},
                    "generation": {
                        "type": "object",
                        "additionalProperties": false,
                        "required": ["dataset_id", "generation_id", "generation_digest"],
                        "properties": {
                            "dataset_id": dataset_id.clone(),
                            "generation_id": generation_id,
                            "generation_digest": digest.clone()
                        }
                    }
                }
            },
            {
                "type": "object",
                "additionalProperties": false,
                "required": ["mode", "dataset_id"],
                "properties": {
                    "mode": {"const": "pin_at_run_admission"},
                    "dataset_id": dataset_id.clone()
                }
            },
            {
                "type": "object",
                "additionalProperties": false,
                "required": ["mode", "dataset_id"],
                "properties": {
                    "mode": {"const": "latest_at_query_start"},
                    "dataset_id": dataset_id
                }
            },
            {
                "type": "object",
                "additionalProperties": false,
                "required": ["mode"],
                "properties": {"mode": {"const": "external_observation"}}
            }
        ]
    });
    let context_binding = json!({
        "type": "object",
        "additionalProperties": false,
        "required": [
            "schema_version", "context_binding_id", "owner_agent_deployment_id",
            "context_deployment", "consistency", "allowed_projection",
            "authorization_policy", "ranking_policy", "binding_digest"
        ],
        "properties": {
            "schema_version": {"const": 1},
            "context_binding_id": {
                "type": "string",
                "pattern": "^xcb_[0-9a-f]{8}-[0-9a-f]{4}-7[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$"
            },
            "owner_agent_deployment_id": {
                "type": "string",
                "pattern": "^adep_[0-9a-f]{8}-[0-9a-f]{4}-7[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$"
            },
            "context_deployment": exact_deployment("context_deployment", "xdep"),
            "consistency": consistency,
            "allowed_projection": {
                "type": "array",
                "maxItems": 256,
                "uniqueItems": true,
                "items": {
                    "type": "string",
                    "minLength": 1,
                    "maxLength": 128,
                    "pattern": "^[A-Za-z0-9_.-]+$"
                }
            },
            "authorization_policy": selection_policy.clone(),
            "ranking_policy": selection_policy.clone(),
            "binding_digest": digest.clone()
        }
    });
    let target = json!({
        "oneOf": [
            {
                "type": "object",
                "additionalProperties": false,
                "required": ["kind", "candidates", "selection_policy"],
                "properties": {
                    "kind": {"const": "model"},
                    "candidates": {
                        "type": "array", "minItems": 1, "maxItems": 512,
                        "uniqueItems": true,
                        "items": exact_deployment("model_deployment", "mdep")
                    },
                    "selection_policy": selection_policy.clone()
                }
            },
            {
                "type": "object",
                "additionalProperties": false,
                "required": ["kind", "candidates", "selection_policy"],
                "properties": {
                    "kind": {"const": "capability"},
                    "candidates": {
                        "type": "array", "minItems": 1, "maxItems": 512,
                        "uniqueItems": true,
                        "items": exact_deployment("capability_deployment", "cdep")
                    },
                    "selection_policy": selection_policy.clone(),
                    "tool_alias": {
                        "type": ["string", "null"], "minLength": 1, "maxLength": 128
                    }
                }
            },
            {
                "type": "object",
                "additionalProperties": false,
                "required": ["kind", "binding"],
                "properties": {
                    "kind": {"const": "context"},
                    "binding": context_binding
                }
            },
            {
                "type": "object",
                "additionalProperties": false,
                "required": ["kind", "candidates", "selection_policy"],
                "properties": {
                    "kind": {"const": "child_agent"},
                    "candidates": {
                        "type": "array", "minItems": 1, "maxItems": 512,
                        "uniqueItems": true,
                        "items": exact_deployment("agent_deployment", "adep")
                    },
                    "selection_policy": selection_policy.clone()
                }
            },
            {
                "type": "object",
                "additionalProperties": false,
                "required": ["kind", "candidates", "selection_policy"],
                "properties": {
                    "kind": {"const": "skill"},
                    "candidates": {
                        "type": "array", "minItems": 1, "maxItems": 512,
                        "uniqueItems": true,
                        "items": exact_revision("skill_revision", "srev")
                    },
                    "selection_policy": selection_policy
                }
            }
        ]
    });
    json!({
        "$schema": "https://json-schema.org/draft/2020-12/schema",
        "$id": "urn:insight:platform:v1:frozen-slot-binding",
        "title": "FrozenSlotBinding",
        "description": "The only Agent Deployment and RunBindings dependency-slot wire schema.",
        "type": "object",
        "additionalProperties": false,
        "required": ["slot_id", "requirement_digest", "target", "binding_digest"],
        "properties": {
            "slot_id": {"type": "string", "minLength": 1, "maxLength": 128},
            "requirement_digest": digest.clone(),
            "target": target,
            "binding_digest": digest
        }
    })
}

fn pretty(value: &Value) -> Vec<u8> {
    // `serde_json/preserve_order` can be enabled by an unrelated workspace
    // dependency through Cargo feature unification.  Machine-contract bytes
    // must not depend on that ambient feature, so recursively insert object
    // keys in lexical order before pretty serialization.
    let mut bytes =
        serde_json::to_vec_pretty(&sorted_json(value)).expect("machine contract serializes");
    bytes.push(b'\n');
    bytes
}

fn sorted_json(value: &Value) -> Value {
    match value {
        Value::Array(items) => Value::Array(items.iter().map(sorted_json).collect()),
        Value::Object(object) => {
            let mut entries = object.iter().collect::<Vec<_>>();
            entries.sort_by(|(left, _), (right, _)| left.cmp(right));
            Value::Object(
                entries
                    .into_iter()
                    .map(|(key, value)| (key.clone(), sorted_json(value)))
                    .collect(),
            )
        }
        scalar => scalar.clone(),
    }
}

pub fn generated_root_manifest(repository_root: &Path) -> Result<Vec<u8>, std::io::Error> {
    let mut files = Vec::with_capacity(CONTRACT_MANIFEST_INPUTS.len());
    for relative in CONTRACT_MANIFEST_INPUTS {
        let raw = fs::read(repository_root.join(relative))?;
        files.push(json!({
            "bytes": raw.len(),
            "media_type": media_type(relative),
            "path": relative,
            "sha256": lowercase_sha256(&raw)
        }));
    }
    let manifest = json!({
        "contract_digest": canonical_digest(&Value::Array(files.clone()))
            .expect("manifest file metadata is canonicalizable"),
        "contract_profile": "insight.platform/v1",
        "files": files,
        "manifest_version": 1,
        "status": "implementing_not_current"
    });
    Ok(pretty(&manifest))
}

fn media_type(path: &str) -> &'static str {
    if path.ends_with(".json") {
        "application/json"
    } else if path.ends_with(".yaml") {
        "application/yaml"
    } else if path.ends_with(".proto") {
        "text/x-protobuf"
    } else {
        "application/octet-stream"
    }
}

fn lowercase_sha256(bytes: &[u8]) -> String {
    use sha2::{Digest as _, Sha256};

    Sha256::digest(bytes)
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect()
}

#[derive(Debug)]
pub struct ContractTreeMismatch {
    pub failures: Vec<String>,
}

impl fmt::Display for ContractTreeMismatch {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "machine contract tree mismatch:\n{}",
            self.failures.join("\n")
        )
    }
}

impl Error for ContractTreeMismatch {}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ContractChangeImpact {
    Identical,
    NonBreaking,
    QualificationInvalidating,
    Breaking,
}

impl fmt::Display for ContractChangeImpact {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::Identical => "identical",
            Self::NonBreaking => "non-breaking",
            Self::QualificationInvalidating => "qualification-invalidating",
            Self::Breaking => "breaking",
        })
    }
}

pub fn classify_contract_change(
    relative_path: &str,
    before: &[u8],
    after: &[u8],
) -> ContractChangeImpact {
    if before == after {
        return ContractChangeImpact::Identical;
    }
    if relative_path.starts_with("examples/") {
        ContractChangeImpact::NonBreaking
    } else if relative_path.starts_with("limits/") || relative_path.starts_with("fixtures/") {
        ContractChangeImpact::QualificationInvalidating
    } else {
        ContractChangeImpact::Breaking
    }
}

pub fn check_contract_tree(repository_root: &Path) -> Result<(), ContractTreeMismatch> {
    let root = repository_root.join(CONTRACT_ROOT);
    let mut failures = Vec::new();
    for (relative, expected) in generated_contracts() {
        let path = root.join(relative);
        match fs::read(&path) {
            Ok(actual) if actual == expected => {}
            Ok(actual) => failures.push(format!(
                "{} differs from generated contract ({})",
                path.display(),
                classify_contract_change(relative, &actual, &expected)
            )),
            Err(error) => failures.push(format!("{} cannot be read: {error}", path.display())),
        }
    }
    let manifest_path = repository_root.join(CONTRACT_ROOT).join("manifest.json");
    match generated_root_manifest(repository_root) {
        Ok(expected) => match fs::read(&manifest_path) {
            Ok(actual) if actual == expected => {}
            Ok(actual) => failures.push(format!(
                "{} differs from generated contract ({})",
                manifest_path.display(),
                classify_contract_change("manifest.json", &actual, &expected)
            )),
            Err(error) => failures.push(format!(
                "{} cannot be read: {error}",
                manifest_path.display()
            )),
        },
        Err(error) => failures.push(format!(
            "root manifest inputs cannot be read while generating {}: {error}",
            manifest_path.display()
        )),
    }
    if failures.is_empty() {
        Ok(())
    } else {
        Err(ContractTreeMismatch { failures })
    }
}

pub fn repository_root_from_manifest() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .and_then(Path::parent)
        .expect("contract crate is two levels below repository root")
        .to_owned()
}

pub fn serialize_pretty<T: Serialize>(value: &T) -> Vec<u8> {
    let mut bytes = serde_json::to_vec_pretty(value).expect("contract value serializes");
    bytes.push(b'\n');
    bytes
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeSet;

    #[test]
    fn execution_work_owner_pairs_are_unique_and_scheduler_only() {
        let mut pairs = BTreeSet::new();
        for (work_class, owner_kind) in EXECUTION_WORK_OWNER_PAIRS {
            assert!(pairs.insert((work_class.as_str(), owner_kind.descriptor().name)));
            assert!(is_execution_work_owner_pair(*work_class, *owner_kind));
        }

        assert!(SchedulerPriority::ALL
            .iter()
            .any(|priority| priority.as_str() == "critical_control"));
        assert!(ServiceClass::ALL
            .iter()
            .all(|class| class.as_str() != "critical_control"));
    }

    #[test]
    fn drift_is_classified_by_contract_surface() {
        assert_eq!(
            classify_contract_change("registries.json", b"old", b"new"),
            ContractChangeImpact::Breaking
        );
        assert_eq!(
            classify_contract_change("examples/foundation-scalars.json", b"old", b"new"),
            ContractChangeImpact::NonBreaking
        );
        assert_eq!(
            classify_contract_change("limits/q1-50.json", b"old", b"new"),
            ContractChangeImpact::QualificationInvalidating
        );
        assert_eq!(
            classify_contract_change("errors.json", b"same", b"same"),
            ContractChangeImpact::Identical
        );
    }
}
