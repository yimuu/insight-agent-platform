//! Machine-readable foundations for the clean-cut `insight.platform/v1` contract.
//!
//! This crate deliberately has no dependency on the current runtime or API crates. It is the
//! producer for closed registries and fixtures consumed by later implementation phases; its
//! presence does not make the target API a current behavior.

#![recursion_limit = "256"]

pub mod capability;
pub mod command;
pub mod context;
pub mod id;
pub mod json;
pub mod limits;
pub mod machine;
pub mod mcp;
pub mod model;
pub mod nominal;
pub mod operation;
pub mod qualification;
pub mod registry;
pub mod resource;
pub mod sandbox_policy;
pub mod schema;
pub mod security;
pub mod state;
pub mod trace;
pub mod types;
pub mod worker;

pub use capability::*;
pub use command::{
    CommandAudit, CommandContractError, CommandOutcome, ExternalLeafFailureMutationIds,
    ExternalLeafResumeMutationIds,
};
pub use context::*;
pub use id::{ResourceId, ResourceIdError, ResourceKind};
pub use json::{
    canonical_digest, canonical_json, parse_strict_json, JsonLimits, StrictJsonError,
    MAX_SAFE_JSON_INTEGER,
};
pub use limits::{
    checked_in_hard_limit_profile, HardLimitProfile, Limit, LimitProfileError, LimitUnit,
    OverflowOutcome, HARD_LIMIT_PROFILE_VERSION, Q1_SANDBOX_RUNTIME_BUNDLE_BYTES,
};
pub use machine::{
    is_execution_work_owner_pair, is_job_kind_work_owner_triple, EXECUTION_WORK_OWNER_PAIRS,
    JOB_KIND_WORK_OWNER_TRIPLES,
};
pub use mcp::*;
pub use model::*;
pub use nominal::{
    canonical_schema_digest, is_known_pinned_nominal_reference, nominal_schemas,
    pinned_nominal_reference,
};
pub use operation::*;
pub use qualification::{
    CandidateManifest, CandidateManifestError, CapacityPool, CapacityPoolKind, CapacityProfile,
    ComponentRole, GitCommit, HpaTarget, LeaseTarget, NewCandidateManifest,
    QualificationArtifactLink, QualificationEnvironmentClass, QualificationEvidenceManifest,
    QualificationGate, QualificationGateEvidence, QualificationLayer, QualificationManifestError,
    QualificationOutcome, QualificationProfile, QueueTarget, RecoveryTarget, ReplicaTarget,
    SafetyScanTarget, SloIndicator, SloTarget, CAPACITY_PROFILE_VERSION,
    MAX_CANDIDATE_COMPONENT_IMAGES, MAX_CANDIDATE_WORKER_MANIFESTS, MAX_QUALIFICATION_ARTIFACTS,
    MAX_QUALIFICATION_NAME_BYTES, MAX_QUALIFICATION_TOOL_VERSIONS, MAX_QUALIFICATION_VERSION_BYTES,
    QUALIFICATION_EVIDENCE_VERSION, QUALIFICATION_PROFILE_VERSION,
};
pub use registry::{
    require_cursor_purpose, validate_public_event_envelope, AgentAuthoringMode, ApiProblemCode,
    ArtifactGrantOperation, ArtifactPurpose, ArtifactReferenceKind, ArtifactWorkloadAudience,
    AuthnStrength, BlobIntegrityState, CapabilityBackendKind, CapabilityCancellationKind,
    CapabilityIdempotencyKind, CapabilityProgressDurability, CapabilityProgressMode,
    CodeTrustClass, ContextBackendKind, ContextBackendOutcomeKind, ContextCitationStrength,
    ContextConsistencyMode, CursorPurpose, CursorPurposeMismatch, DataClassification,
    DependencySlotKind, Effect, EventDurability, EventEnvelopeError, FailureClass, FailureSource,
    InteractionKind, JobKind, LockRank, McpAuthorizationPrincipalKind,
    McpOAuthClientAuthenticationKind, McpTransportKind, ModelIdentityStability, ModelModality,
    Permission, PlanNodeKind, PlatformFailureCode, PolicyKind, PolicyReferenceRole, PrincipalKind,
    PublicJobKind, PublicRunEventSourceKind, PublicRunEventType, QuotaAccountingMode,
    QuotaDimension, QuotaScopeKind, QuotaWindowKind, Retryability, SandboxAbiVersion,
    SandboxCleanupPolicy, SandboxEntrypointKind, SandboxIsolationClass, SandboxRuntimeFamily,
    SchedulerPriority, ScopeKind, ServiceClass, SkillInstructionAudience, SkillInstructionPhase,
    SkillPackageEntryKind, SkillRequirementKind, SkillSelectionMode, UnknownRegistryValue,
    WakeContractKind, WorkClass,
};
pub use resource::*;
pub use sandbox_policy::*;
pub use schema::{
    validate_capability_interface_schema, validate_closed_schema, ClosedJsonSchema,
    ClosedSchemaDocument, InteractionSchemaDocument, SchemaProfileError,
    CLOSED_SCHEMA_DOCUMENT_VERSION, CLOSED_SCHEMA_PROFILE_ID, MAX_CLOSED_SCHEMA_BYTES,
    MCP_FORM_SCHEMA_PROFILE_ID,
};
pub use security::{
    authorize, exact_secret_binding_purposes_match, resolution_policy_digest, AuthorizationError,
    AuthorizationRequest, ExactSecretBindingRef, InstallationPrincipalBinding, PermissionSet,
    PrincipalBindingsPayload, PrincipalContext, PrincipalScope, PrincipalSnapshot,
    SecretBindingPayload, SecretPurpose, SecretResolutionPolicy, TenantConfig,
    TenantPrincipalPayload,
};
pub use state::{
    AdministrativeGate, ApprovalState, ArtifactState, AttemptCommitDisposition,
    AttemptObservationState, ContextQueryState, EntityLifecycle, InteractionState, InvocationState,
    JobState, McpAuthorizationState, McpSessionState, ModelTurnState, NodeExecutionState,
    PrincipalBindingState, PrincipalIdentityState, RunState, SandboxJobState, ScopeState,
    SecretBindingState, WakeContractState,
};
pub use trace::{
    SpanId, TraceContractError, TraceFlags, TraceId, TraceIdentityV1, W3cTraceParent,
    SPAN_ID_HEX_LENGTH, TRACE_ID_HEX_LENGTH,
};
pub use types::{
    ApiProblem, ArtifactRef, DecimalMoney, DeclaredFailureCode, DurablePublicRunEventData, Failure,
    FailureCode, FieldError, NominalTypeError, OpaqueListCursor, OpaqueRunEventCursor,
    PublicRunEvent, Sha256Digest, UtcTimestamp, ValueRef, MAX_ARTIFACT_BYTES, MAX_FIELD_ERRORS,
    MAX_OPAQUE_CURSOR_BYTES, MAX_PUBLIC_EVENT_SAFE_SUMMARY_BYTES, MAX_SAFE_TEXT_BYTES,
};
pub use worker::{
    WorkerManifest, WorkerManifestError, WORKER_MANIFEST_VERSION, WORKER_PROTOCOL_VERSION,
};
