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
        FailureSource, InteractionKind, LockRank, McpAuthorizationPrincipalKind,
        McpOAuthClientAuthenticationKind, McpTransportKind, ModelIdentityStability, ModelModality,
        Permission, PlanNodeKind, PlatformFailureCode, PolicyKind, PolicyReferenceRole,
        PrincipalKind, PublicJobKind, PublicRunEventSourceKind, PublicRunEventType,
        QuotaAccountingMode, QuotaDimension, QuotaScopeKind, QuotaWindowKind, Retryability,
        SandboxAbiVersion, SandboxCleanupPolicy, SandboxEntrypointKind, SandboxIsolationClass,
        SandboxRuntimeFamily, SchedulerPriority, ScopeKind, ServiceClass, SkillInstructionAudience,
        SkillInstructionPhase, SkillPackageEntryKind, SkillRequirementKind, SkillSelectionMode,
        WakeContractKind, WorkClass,
    },
    schema::{ALLOWED_SCHEMA_KEYWORDS, CLOSED_SCHEMA_PROFILE_ID},
    state::{all_state_machines, AttemptCommitDisposition},
    types::MAX_PUBLIC_EVENT_SAFE_SUMMARY_BYTES,
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
  /{resource_noun}:
    post:
      operationId: createManagedResource
      summary: Create one typed public management Resource with its editable Draft
      tags: [Resources]
      x-insight-authentication: oidc_or_workload_credential
      x-insight-permission: resource.write
      x-insight-idempotency: tenant_principal_resource_collection_receipt
      parameters:
        - $ref: "#/components/parameters/ResourceNoun"
        - $ref: "#/components/parameters/IdempotencyKey"
      requestBody:
        required: true
        content:
          application/json:
            schema: {$ref: "#/components/schemas/CreateResourceRequestV1"}
      responses:
        "201": {$ref: "#/components/responses/ResourceCreatedResponse"}
        "400": {$ref: "#/components/responses/ApiProblem"}
        "401": {$ref: "#/components/responses/ApiProblem"}
        "403": {$ref: "#/components/responses/ApiProblem"}
        "409": {$ref: "#/components/responses/ApiProblem"}
        "503": {$ref: "#/components/responses/ApiProblem"}
  /{resource_noun}/{resource_id}:
    get:
      operationId: getManagedResource
      summary: Read the current typed Resource and editable Draft projection
      tags: [Resources]
      x-insight-permission: resource.read
      parameters:
        - $ref: "#/components/parameters/ResourceNoun"
        - $ref: "#/components/parameters/ResourceId"
      responses:
        "200": {$ref: "#/components/responses/ResourceResponse"}
        "401": {$ref: "#/components/responses/ApiProblem"}
        "403": {$ref: "#/components/responses/ApiProblem"}
        "404": {$ref: "#/components/responses/ApiProblem"}
        "503": {$ref: "#/components/responses/ApiProblem"}
  /{resource_noun}/{resource_id}/draft:
    put:
      operationId: updateManagedResourceDraft
      summary: Replace the current editable Draft under a strong Resource fence
      tags: [Resources]
      x-insight-permission: resource.write
      x-insight-idempotency: resource_scoped_receipt
      parameters:
        - $ref: "#/components/parameters/ResourceNoun"
        - $ref: "#/components/parameters/ResourceId"
        - $ref: "#/components/parameters/IfMatch"
        - $ref: "#/components/parameters/IdempotencyKey"
      requestBody:
        required: true
        content:
          application/json:
            schema: {$ref: "#/components/schemas/ResourceDraftPayload"}
      responses:
        "200": {$ref: "#/components/responses/ResourceResponse"}
        "400": {$ref: "#/components/responses/ApiProblem"}
        "401": {$ref: "#/components/responses/ApiProblem"}
        "403": {$ref: "#/components/responses/ApiProblem"}
        "404": {$ref: "#/components/responses/ApiProblem"}
        "409": {$ref: "#/components/responses/ApiProblem"}
        "412": {$ref: "#/components/responses/ApiProblem"}
        "428": {$ref: "#/components/responses/ApiProblem"}
  /{resource_noun}/{resource_id}/draft:validate:
    post:
      operationId: validateManagedResourceDraft
      summary: Create a shared validation Job for the exact current Draft generation
      tags: [Resources]
      x-insight-permission: resource.write
      x-insight-idempotency: resource_scoped_receipt
      x-insight-empty-body: required
      parameters:
        - $ref: "#/components/parameters/ResourceNoun"
        - $ref: "#/components/parameters/ResourceId"
        - $ref: "#/components/parameters/IfMatch"
        - $ref: "#/components/parameters/IdempotencyKey"
      responses:
        "202": {$ref: "#/components/responses/OperationAcceptedResponse"}
        "400": {$ref: "#/components/responses/ApiProblem"}
        "401": {$ref: "#/components/responses/ApiProblem"}
        "403": {$ref: "#/components/responses/ApiProblem"}
        "404": {$ref: "#/components/responses/ApiProblem"}
        "409": {$ref: "#/components/responses/ApiProblem"}
        "412": {$ref: "#/components/responses/ApiProblem"}
        "428": {$ref: "#/components/responses/ApiProblem"}
  /{resource_noun}/{resource_id}/draft:publish:
    post:
      operationId: publishManagedResourceDraft
      summary: Publish immutable Version records from one validated Draft generation
      tags: [Resources]
      x-insight-permission: resource.write
      x-insight-idempotency: resource_scoped_receipt
      parameters:
        - $ref: "#/components/parameters/ResourceNoun"
        - $ref: "#/components/parameters/ResourceId"
        - $ref: "#/components/parameters/IfMatch"
        - $ref: "#/components/parameters/IdempotencyKey"
      requestBody:
        required: true
        content:
          application/json:
            schema: {$ref: "#/components/schemas/PublishResourceDraftRequestV1"}
      responses:
        "200": {$ref: "#/components/responses/PublishResourceResponse"}
        "400": {$ref: "#/components/responses/ApiProblem"}
        "401": {$ref: "#/components/responses/ApiProblem"}
        "403": {$ref: "#/components/responses/ApiProblem"}
        "404": {$ref: "#/components/responses/ApiProblem"}
        "409": {$ref: "#/components/responses/ApiProblem"}
        "412": {$ref: "#/components/responses/ApiProblem"}
        "428": {$ref: "#/components/responses/ApiProblem"}
  /{resource_noun}/{resource_id}/versions/{version_id}:
    get:
      operationId: getManagedResourceVersion
      summary: Read one immutable published Version
      tags: [Resources]
      x-insight-permission: resource.read
      parameters:
        - $ref: "#/components/parameters/ResourceNoun"
        - $ref: "#/components/parameters/ResourceId"
        - $ref: "#/components/parameters/ResourceVersionId"
      responses:
        "200": {$ref: "#/components/responses/ResourceVersionResponse"}
        "401": {$ref: "#/components/responses/ApiProblem"}
        "403": {$ref: "#/components/responses/ApiProblem"}
        "404": {$ref: "#/components/responses/ApiProblem"}
        "503": {$ref: "#/components/responses/ApiProblem"}
  /{resource_noun}/{resource_id}/deployments:
    post:
      operationId: createManagedResourceDeployment
      summary: Create one immutable exact Deployment closure
      tags: [Resources]
      x-insight-permission: resource.write
      x-insight-idempotency: resource_scoped_receipt
      parameters:
        - $ref: "#/components/parameters/ResourceNoun"
        - $ref: "#/components/parameters/ResourceId"
        - $ref: "#/components/parameters/IfMatch"
        - $ref: "#/components/parameters/IdempotencyKey"
      requestBody:
        required: true
        content:
          application/json:
            schema: {$ref: "#/components/schemas/CreateDeploymentRequestV1"}
      responses:
        "201": {$ref: "#/components/responses/DeploymentCreatedResponse"}
        "400": {$ref: "#/components/responses/ApiProblem"}
        "401": {$ref: "#/components/responses/ApiProblem"}
        "403": {$ref: "#/components/responses/ApiProblem"}
        "404": {$ref: "#/components/responses/ApiProblem"}
        "409": {$ref: "#/components/responses/ApiProblem"}
        "412": {$ref: "#/components/responses/ApiProblem"}
        "428": {$ref: "#/components/responses/ApiProblem"}
  /{resource_noun}/{resource_id}/deployments/{deployment_id}:
    get:
      operationId: getManagedResourceDeployment
      summary: Read one immutable exact Deployment closure
      tags: [Resources]
      x-insight-permission: resource.read
      parameters:
        - $ref: "#/components/parameters/ResourceNoun"
        - $ref: "#/components/parameters/ResourceId"
        - $ref: "#/components/parameters/DeploymentId"
      responses:
        "200": {$ref: "#/components/responses/DeploymentResponse"}
        "401": {$ref: "#/components/responses/ApiProblem"}
        "403": {$ref: "#/components/responses/ApiProblem"}
        "404": {$ref: "#/components/responses/ApiProblem"}
        "503": {$ref: "#/components/responses/ApiProblem"}
  /{resource_noun}/{resource_id}/deployments/{deployment_id}:activate:
    post:
      operationId: activateManagedResourceDeployment
      summary: Set the exact path Deployment as the enabled future binding
      tags: [Resources]
      x-insight-permission: resource.write
      x-insight-idempotency: resource_scoped_receipt
      x-insight-empty-body: required
      parameters:
        - $ref: "#/components/parameters/ResourceNoun"
        - $ref: "#/components/parameters/ResourceId"
        - $ref: "#/components/parameters/DeploymentId"
        - $ref: "#/components/parameters/IfMatch"
        - $ref: "#/components/parameters/IdempotencyKey"
      responses:
        "200": {$ref: "#/components/responses/ResourceResponse"}
        "400": {$ref: "#/components/responses/ApiProblem"}
        "401": {$ref: "#/components/responses/ApiProblem"}
        "403": {$ref: "#/components/responses/ApiProblem"}
        "404": {$ref: "#/components/responses/ApiProblem"}
        "409": {$ref: "#/components/responses/ApiProblem"}
        "412": {$ref: "#/components/responses/ApiProblem"}
        "428": {$ref: "#/components/responses/ApiProblem"}
  /{resource_noun}/{resource_id}/deployments/{deployment_id}:suspend:
    post:
      operationId: suspendManagedResourceDeployment
      summary: Suspend the exact currently active future binding
      tags: [Resources]
      x-insight-permission: resource.write
      x-insight-idempotency: resource_scoped_receipt
      x-insight-empty-body: required
      parameters:
        - $ref: "#/components/parameters/ResourceNoun"
        - $ref: "#/components/parameters/ResourceId"
        - $ref: "#/components/parameters/DeploymentId"
        - $ref: "#/components/parameters/IfMatch"
        - $ref: "#/components/parameters/IdempotencyKey"
      responses:
        "200": {$ref: "#/components/responses/ResourceResponse"}
        "400": {$ref: "#/components/responses/ApiProblem"}
        "401": {$ref: "#/components/responses/ApiProblem"}
        "403": {$ref: "#/components/responses/ApiProblem"}
        "404": {$ref: "#/components/responses/ApiProblem"}
        "409": {$ref: "#/components/responses/ApiProblem"}
        "412": {$ref: "#/components/responses/ApiProblem"}
        "428": {$ref: "#/components/responses/ApiProblem"}
  /runs:
    post:
      operationId: createRun
      summary: Admit a root Run from one enabled Agent binding
      tags: [Runs]
      x-insight-authentication: oidc_or_workload_credential
      x-insight-permission: agent.run
      x-insight-idempotency: tenant_principal_agent_collection_receipt
      parameters:
        - $ref: "#/components/parameters/IdempotencyKey"
      requestBody:
        required: true
        content:
          application/json:
            schema: {$ref: "#/components/schemas/CreateRunRequestV1"}
      responses:
        "201":
          description: The root Run and its initial Node, Job, Receipt, Event, and Outbox committed atomically.
          headers:
            Location: {schema: {type: string, pattern: "^/v1/runs/run_"}}
            ETag: {schema: {type: string, minLength: 1, maxLength: 128}}
            Cache-Control: {$ref: "#/components/headers/PrivateNoStore"}
          content:
            application/json:
              schema: {$ref: "#/components/schemas/RunViewV1"}
        "400": {$ref: "#/components/responses/ApiProblem"}
        "401": {$ref: "#/components/responses/ApiProblem"}
        "403": {$ref: "#/components/responses/ApiProblem"}
        "404": {$ref: "#/components/responses/ApiProblem"}
        "409": {$ref: "#/components/responses/ApiProblem"}
        "503": {$ref: "#/components/responses/ApiProblem"}
  /runs/{run_id}:
    get:
      operationId: getRun
      summary: Read the current safe Run projection
      tags: [Runs]
      x-insight-permission: runtime.read
      parameters:
        - $ref: "#/components/parameters/RunId"
      responses:
        "200":
          description: Current Run projection with a strong version ETag.
          headers:
            ETag: {schema: {type: string, minLength: 1, maxLength: 128}}
            Cache-Control: {$ref: "#/components/headers/PrivateNoStore"}
          content:
            application/json:
              schema: {$ref: "#/components/schemas/RunViewV1"}
        "401": {$ref: "#/components/responses/ApiProblem"}
        "403": {$ref: "#/components/responses/ApiProblem"}
        "404": {$ref: "#/components/responses/ApiProblem"}
        "503": {$ref: "#/components/responses/ApiProblem"}
  /runs/{run_id}/result:
    get:
      operationId: getRunResult
      summary: Read the typed output of a terminal Run
      tags: [Runs]
      x-insight-permission: runtime.read
      parameters:
        - $ref: "#/components/parameters/RunId"
      responses:
        "200":
          description: Inline output or an exact Ready Artifact reference.
          headers:
            Cache-Control: {$ref: "#/components/headers/PrivateNoStore"}
          content:
            application/json:
              schema: {$ref: "#/components/schemas/RunResultViewV1"}
        "401": {$ref: "#/components/responses/ApiProblem"}
        "403": {$ref: "#/components/responses/ApiProblem"}
        "404": {$ref: "#/components/responses/ApiProblem"}
        "409": {$ref: "#/components/responses/ApiProblem"}
        "503": {$ref: "#/components/responses/ApiProblem"}
  /runs/{run_id}/events:
    get:
      operationId: streamRunEvents
      summary: Read the next bounded page of durable public Run events as SSE
      description: >-
        Returns at most 128 ordered durable events and then closes the stream. Reconnect with the
        last received opaque SSE id in Last-Event-ID. The signed cursor is scoped to the current
        principal binding and Run, expires after 15 minutes, and is never an event authority.
      tags: [Runs]
      x-insight-permission: runtime.read
      x-insight-idempotency: read_only
      x-insight-event-authority: postgres
      x-insight-maximum-events-per-response: 128
      parameters:
        - $ref: "#/components/parameters/RunId"
        - name: Last-Event-ID
          in: header
          required: false
          description: Opaque signed cursor emitted as the id of the last accepted SSE event.
          schema: {$ref: "#/components/schemas/OpaqueRunEventCursor"}
      responses:
        "200":
          description: A finite ordered SSE page containing only closed public event projections.
          headers:
            Cache-Control: {$ref: "#/components/headers/PrivateNoStore"}
          content:
            text/event-stream:
              schema:
                type: string
              x-insight-json-event-data:
                envelope: PublicRunEvent
                durable-payload-projection:
                  $ref: "#/components/schemas/DurablePublicRunEventPayload"
        "400": {$ref: "#/components/responses/ApiProblem"}
        "401": {$ref: "#/components/responses/ApiProblem"}
        "403": {$ref: "#/components/responses/ApiProblem"}
        "404": {$ref: "#/components/responses/ApiProblem"}
        "503": {$ref: "#/components/responses/ApiProblem"}
  /runs/{run_id}:pause:
    post:
      operationId: pauseRun
      summary: Durably request Run pause
      tags: [Runs]
      x-insight-permission: agent.run
      x-insight-idempotency: run_scoped_receipt
      parameters:
        - $ref: "#/components/parameters/RunId"
        - $ref: "#/components/parameters/IfMatch"
        - $ref: "#/components/parameters/IdempotencyKey"
      responses:
        "200": {$ref: "#/components/responses/RunControlResponse"}
        "400": {$ref: "#/components/responses/ApiProblem"}
        "401": {$ref: "#/components/responses/ApiProblem"}
        "403": {$ref: "#/components/responses/ApiProblem"}
        "404": {$ref: "#/components/responses/ApiProblem"}
        "409": {$ref: "#/components/responses/ApiProblem"}
  /runs/{run_id}:resume:
    post:
      operationId: resumeRun
      summary: Durably clear a Run pause request
      tags: [Runs]
      x-insight-permission: agent.run
      x-insight-idempotency: run_scoped_receipt
      parameters:
        - $ref: "#/components/parameters/RunId"
        - $ref: "#/components/parameters/IfMatch"
        - $ref: "#/components/parameters/IdempotencyKey"
      responses:
        "200": {$ref: "#/components/responses/RunControlResponse"}
        "400": {$ref: "#/components/responses/ApiProblem"}
        "401": {$ref: "#/components/responses/ApiProblem"}
        "403": {$ref: "#/components/responses/ApiProblem"}
        "404": {$ref: "#/components/responses/ApiProblem"}
        "409": {$ref: "#/components/responses/ApiProblem"}
  /runs/{run_id}:cancel:
    post:
      operationId: cancelRun
      summary: Durably request Run cancellation
      tags: [Runs]
      x-insight-permission: agent.run
      x-insight-idempotency: run_scoped_receipt
      parameters:
        - $ref: "#/components/parameters/RunId"
        - $ref: "#/components/parameters/IfMatch"
        - $ref: "#/components/parameters/IdempotencyKey"
      responses:
        "200": {$ref: "#/components/responses/RunControlResponse"}
        "400": {$ref: "#/components/responses/ApiProblem"}
        "401": {$ref: "#/components/responses/ApiProblem"}
        "403": {$ref: "#/components/responses/ApiProblem"}
        "404": {$ref: "#/components/responses/ApiProblem"}
        "409": {$ref: "#/components/responses/ApiProblem"}
  /tasks/{task_id}:
    get:
      operationId: getTask
      summary: Read a safe authorized Task projection
      tags: [Tasks]
      parameters:
        - $ref: "#/components/parameters/TaskId"
      responses:
        "200":
          description: Safe Task prompt metadata and owner link.
          headers:
            ETag: {schema: {type: string, minLength: 1, maxLength: 128}}
            Cache-Control: {$ref: "#/components/headers/PrivateNoStore"}
          content:
            application/json:
              schema: {$ref: "#/components/schemas/TaskViewV1"}
        "401": {$ref: "#/components/responses/ApiProblem"}
        "403": {$ref: "#/components/responses/ApiProblem"}
        "404": {$ref: "#/components/responses/ApiProblem"}
  /tasks/{task_id}:submit-input:
    post:
      operationId: submitTaskInput
      summary: Submit a typed Task response and atomically wake its owner
      tags: [Tasks]
      parameters:
        - $ref: "#/components/parameters/TaskId"
        - $ref: "#/components/parameters/IfMatch"
        - $ref: "#/components/parameters/IdempotencyKey"
      requestBody:
        required: true
        content:
          application/json:
            schema: {$ref: "#/components/schemas/SubmitTaskInputV1"}
      responses:
        "200": {$ref: "#/components/responses/TaskControlResponse"}
        "400": {$ref: "#/components/responses/ApiProblem"}
        "401": {$ref: "#/components/responses/ApiProblem"}
        "403": {$ref: "#/components/responses/ApiProblem"}
        "409": {$ref: "#/components/responses/ApiProblem"}
  /tasks/{task_id}:approve:
    post:
      operationId: approveTask
      summary: Approve an exact pending approval Task
      tags: [Tasks]
      parameters:
        - $ref: "#/components/parameters/TaskId"
        - $ref: "#/components/parameters/IfMatch"
        - $ref: "#/components/parameters/IdempotencyKey"
      responses:
        "200": {$ref: "#/components/responses/TaskControlResponse"}
        "400": {$ref: "#/components/responses/ApiProblem"}
        "401": {$ref: "#/components/responses/ApiProblem"}
        "403": {$ref: "#/components/responses/ApiProblem"}
        "409": {$ref: "#/components/responses/ApiProblem"}
  /tasks/{task_id}:reject:
    post:
      operationId: rejectTask
      summary: Reject or decline an exact pending Task
      tags: [Tasks]
      parameters:
        - $ref: "#/components/parameters/TaskId"
        - $ref: "#/components/parameters/IfMatch"
        - $ref: "#/components/parameters/IdempotencyKey"
      responses:
        "200": {$ref: "#/components/responses/TaskControlResponse"}
        "400": {$ref: "#/components/responses/ApiProblem"}
        "401": {$ref: "#/components/responses/ApiProblem"}
        "403": {$ref: "#/components/responses/ApiProblem"}
        "409": {$ref: "#/components/responses/ApiProblem"}
  /tasks/{task_id}:cancel:
    post:
      operationId: cancelTask
      summary: Cancel an interaction Task through its owner adapter
      tags: [Tasks]
      parameters:
        - $ref: "#/components/parameters/TaskId"
        - $ref: "#/components/parameters/IfMatch"
        - $ref: "#/components/parameters/IdempotencyKey"
      responses:
        "200": {$ref: "#/components/responses/TaskControlResponse"}
        "400": {$ref: "#/components/responses/ApiProblem"}
        "401": {$ref: "#/components/responses/ApiProblem"}
        "403": {$ref: "#/components/responses/ApiProblem"}
        "409": {$ref: "#/components/responses/ApiProblem"}
  /artifacts:prepare-upload:
    post:
      operationId: prepareArtifactUpload
      summary: Prepare a server-owned staging Artifact and short-lived upload target
      tags: [Artifacts]
      x-insight-authentication: oidc_or_workload_credential
      x-insight-permission: artifact.write
      x-insight-idempotency: tenant_principal_artifact_collection_receipt
      parameters:
        - $ref: "#/components/parameters/IdempotencyKey"
      requestBody:
        required: true
        content:
          application/json:
            schema: {$ref: "#/components/schemas/PrepareArtifactUploadRequestV1"}
      responses:
        "201":
          description: Staging Artifact, shared verification Job, Grant, and secret-bearing target.
          headers:
            Location: {schema: {type: string, pattern: "^/v1/artifacts/art_"}}
            ETag: {schema: {type: string, minLength: 1, maxLength: 128}}
            Cache-Control: {$ref: "#/components/headers/PrivateNoStore"}
          content:
            application/json:
              schema: {$ref: "#/components/schemas/PrepareArtifactUploadResponseV1"}
        "400": {$ref: "#/components/responses/ApiProblem"}
        "401": {$ref: "#/components/responses/ApiProblem"}
        "403": {$ref: "#/components/responses/ApiProblem"}
        "409": {$ref: "#/components/responses/ApiProblem"}
        "413": {$ref: "#/components/responses/ApiProblem"}
        "503": {$ref: "#/components/responses/ApiProblem"}
  /artifacts/{artifact_id}:
    get:
      operationId: getArtifact
      summary: Read safe Artifact metadata and an exact Ready content reference
      tags: [Artifacts]
      x-insight-authentication: oidc_or_workload_credential
      x-insight-permission: artifact.read
      x-insight-idempotency: read_only
      x-insight-rate-class: control_read
      x-insight-audit: access_log_only
      parameters:
        - $ref: "#/components/parameters/ArtifactId"
      responses:
        "200":
          description: Current Artifact projection; content is present only while the Artifact is Ready.
          headers:
            ETag: {schema: {type: string, minLength: 1, maxLength: 128}}
            Cache-Control: {$ref: "#/components/headers/PrivateNoStore"}
          content:
            application/json:
              schema: {$ref: "#/components/schemas/ArtifactViewV1"}
        "401": {$ref: "#/components/responses/ApiProblem"}
        "403": {$ref: "#/components/responses/ApiProblem"}
        "404": {$ref: "#/components/responses/ApiProblem"}
        "503": {$ref: "#/components/responses/ApiProblem"}
  /artifacts/{artifact_id}:complete-upload:
    post:
      operationId: completeArtifactUpload
      summary: Verify the current provider generation and schedule the frozen scan
      tags: [Artifacts]
      x-insight-authentication: oidc_or_workload_credential
      x-insight-permission: artifact.write
      x-insight-idempotency: artifact_scoped_receipt
      parameters:
        - $ref: "#/components/parameters/ArtifactId"
        - $ref: "#/components/parameters/IdempotencyKey"
        - $ref: "#/components/parameters/IfMatch"
      requestBody:
        required: true
        content:
          application/json:
            schema: {$ref: "#/components/schemas/CompleteArtifactUploadRequestV1"}
      responses:
        "202":
          description: Provider generation accepted and the shared verification Job made ready.
          headers:
            Location: {schema: {type: string, pattern: "^/v1/operations/job_"}}
            ETag: {schema: {type: string, minLength: 1, maxLength: 128}}
            Cache-Control: {$ref: "#/components/headers/PrivateNoStore"}
          content:
            application/json:
              schema: {$ref: "#/components/schemas/ArtifactMutationAcceptedV1"}
        "400": {$ref: "#/components/responses/ApiProblem"}
        "401": {$ref: "#/components/responses/ApiProblem"}
        "403": {$ref: "#/components/responses/ApiProblem"}
        "404": {$ref: "#/components/responses/ApiProblem"}
        "409": {$ref: "#/components/responses/ApiProblem"}
        "412": {$ref: "#/components/responses/ApiProblem"}
        "428": {$ref: "#/components/responses/ApiProblem"}
        "503": {$ref: "#/components/responses/ApiProblem"}
  /artifacts/{artifact_id}/content:
    get:
      operationId: downloadArtifactContent
      summary: Stream the current authorized Ready Artifact content
      tags: [Artifacts]
      x-insight-authentication: oidc_or_workload_credential
      x-insight-permission: artifact.read
      x-insight-idempotency: read_only
      x-insight-rate-class: artifact_download
      parameters:
        - $ref: "#/components/parameters/ArtifactId"
      responses:
        "200":
          description: Bounded verified attachment stream.
          headers:
            Content-Length: {required: true, schema: {type: integer, minimum: 1, maximum: 1073741824}}
            Content-Disposition: {required: true, schema: {const: attachment}}
            ETag: {required: true, schema: {type: string, minLength: 3, maxLength: 128}}
            Cache-Control: {$ref: "#/components/headers/PrivateNoStore"}
          content:
            "*/*":
              schema: {type: string, format: binary, maxLength: 1073741824}
        "401": {$ref: "#/components/responses/ApiProblem"}
        "403": {$ref: "#/components/responses/ApiProblem"}
        "404": {$ref: "#/components/responses/ApiProblem"}
        "413": {$ref: "#/components/responses/ApiProblem"}
        "503": {$ref: "#/components/responses/ApiProblem"}
  /artifacts/{artifact_id}:delete:
    post:
      operationId: deleteArtifact
      summary: Request policy-controlled deletion through a shared maintenance Job
      tags: [Artifacts]
      x-insight-authentication: oidc_or_workload_credential
      x-insight-permission: artifact.delete
      x-insight-idempotency: artifact_scoped_receipt
      x-insight-empty-body: required
      parameters:
        - $ref: "#/components/parameters/ArtifactId"
        - $ref: "#/components/parameters/IdempotencyKey"
        - $ref: "#/components/parameters/IfMatch"
      responses:
        "202":
          description: Deletion Job accepted or awaiting its exact policy approval.
          headers:
            Location: {schema: {type: string, pattern: "^/v1/operations/job_"}}
            ETag: {schema: {type: string, minLength: 1, maxLength: 128}}
            Cache-Control: {$ref: "#/components/headers/PrivateNoStore"}
          content:
            application/json:
              schema: {$ref: "#/components/schemas/ArtifactMutationAcceptedV1"}
        "400": {$ref: "#/components/responses/ApiProblem"}
        "401": {$ref: "#/components/responses/ApiProblem"}
        "403": {$ref: "#/components/responses/ApiProblem"}
        "404": {$ref: "#/components/responses/ApiProblem"}
        "409": {$ref: "#/components/responses/ApiProblem"}
        "412": {$ref: "#/components/responses/ApiProblem"}
        "428": {$ref: "#/components/responses/ApiProblem"}
        "503": {$ref: "#/components/responses/ApiProblem"}
  /operations/{operation_id}:
    get:
      operationId: getOperation
      summary: Read a safe projection of one shared Job
      tags:
        - Operations
      x-insight-authentication: oidc_or_workload_credential
      x-insight-permission: operation.read
      x-insight-idempotency: read_only
      x-insight-rate-class: control_read
      x-insight-audit: access_log_only
      parameters:
        - name: operation_id
          in: path
          required: true
          schema:
            $ref: "#/components/schemas/JobId"
      responses:
        "200":
          description: Safe Job projection. The ETag is derived directly from the Job version.
          headers:
            ETag:
              schema:
                type: string
                minLength: 1
                maxLength: 128
            Cache-Control:
              $ref: "#/components/headers/NoStore"
          content:
            application/json:
              schema:
                $ref: "#/components/schemas/OperationViewV1"
        "401":
          $ref: "#/components/responses/ApiProblem"
        "403":
          $ref: "#/components/responses/ApiProblem"
        "404":
          $ref: "#/components/responses/ApiProblem"
        "500":
          $ref: "#/components/responses/ApiProblem"
        "503":
          $ref: "#/components/responses/ApiProblem"
  /mcp-servers/{mcp_server_id}/deployments/{mcp_deployment_id}:discover:
    post:
      operationId: discoverMcpDeployment
      summary: Create a durable discovery Job for one exact MCP Deployment
      tags: [MCP Servers]
      x-insight-authentication: oidc_or_workload_credential
      x-insight-permission: mcp.write
      x-insight-idempotency: tenant_principal_mcp_deployment_receipt
      parameters:
        - $ref: "#/components/parameters/McpServerId"
        - $ref: "#/components/parameters/McpDeploymentId"
        - $ref: "#/components/parameters/IdempotencyKey"
      requestBody:
        required: true
        content:
          application/json:
            schema: {$ref: "#/components/schemas/DiscoverMcpDeploymentRequestV1"}
      responses:
        "202":
          description: The shared MCP discovery Job was accepted or replayed.
          headers:
            Location: {schema: {type: string, pattern: "^/v1/operations/job_"}}
            ETag: {schema: {type: string, minLength: 1, maxLength: 128}}
            Cache-Control: {$ref: "#/components/headers/PrivateNoStore"}
          content:
            application/json:
              schema: {$ref: "#/components/schemas/OperationViewV1"}
        "400": {$ref: "#/components/responses/ApiProblem"}
        "401": {$ref: "#/components/responses/ApiProblem"}
        "403": {$ref: "#/components/responses/ApiProblem"}
        "404": {$ref: "#/components/responses/ApiProblem"}
        "409": {$ref: "#/components/responses/ApiProblem"}
        "503": {$ref: "#/components/responses/ApiProblem"}
  /contexts/{context_id}/deployments/{context_deployment_id}:build-dataset:
    post:
      operationId: buildContextDataset
      summary: Create a durable Dataset Generation build Job
      tags: [Contexts]
      x-insight-authentication: oidc_or_workload_credential
      x-insight-permission: context.write
      x-insight-idempotency: tenant_principal_context_deployment_receipt
      parameters:
        - $ref: "#/components/parameters/ContextId"
        - $ref: "#/components/parameters/ContextDeploymentId"
        - $ref: "#/components/parameters/IdempotencyKey"
      requestBody:
        required: true
        content:
          application/json:
            schema: {$ref: "#/components/schemas/BuildContextDatasetRequestV1"}
      responses:
        "202":
          description: The shared build Job was accepted; its target is the reserved Dataset ID.
          headers:
            Location: {schema: {type: string, pattern: "^/v1/operations/job_"}}
            ETag: {schema: {type: string, minLength: 1, maxLength: 128}}
            Cache-Control: {$ref: "#/components/headers/PrivateNoStore"}
          content:
            application/json:
              schema: {$ref: "#/components/schemas/OperationViewV1"}
        "400": {$ref: "#/components/responses/ApiProblem"}
        "401": {$ref: "#/components/responses/ApiProblem"}
        "403": {$ref: "#/components/responses/ApiProblem"}
        "404": {$ref: "#/components/responses/ApiProblem"}
        "409": {$ref: "#/components/responses/ApiProblem"}
        "503": {$ref: "#/components/responses/ApiProblem"}
  /context-datasets/{dataset_id}/versions/{generation_id}:
    get:
      operationId: getContextDatasetGeneration
      summary: Read one immutable Context Dataset Generation
      tags: [Contexts]
      x-insight-authentication: oidc_or_workload_credential
      x-insight-permission: context.read
      x-insight-idempotency: read_only
      x-insight-rate-class: control_read
      x-insight-audit: access_log_only
      parameters:
        - $ref: "#/components/parameters/ContextDatasetId"
        - $ref: "#/components/parameters/DatasetGenerationId"
      responses:
        "200":
          description: One immutable generation from the typed Dataset root.
          headers:
            ETag: {schema: {type: string, minLength: 1, maxLength: 128}}
            Cache-Control: {$ref: "#/components/headers/PrivateNoStore"}
          content:
            application/json:
              schema: {$ref: "#/components/schemas/ContextDatasetGenerationViewV1"}
        "401": {$ref: "#/components/responses/ApiProblem"}
        "403": {$ref: "#/components/responses/ApiProblem"}
        "404": {$ref: "#/components/responses/ApiProblem"}
        "503": {$ref: "#/components/responses/ApiProblem"}
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
  parameters:
    ResourceNoun:
      name: resource_noun
      in: path
      required: true
      schema:
        type: string
        enum: [agents, skills, capabilities, contexts, models, mcp-servers, policies, sandboxes]
    ResourceId:
      name: resource_id
      in: path
      required: true
      schema: {$ref: "#/components/schemas/PublicManagementResourceId"}
    ResourceVersionId:
      name: version_id
      in: path
      required: true
      schema: {$ref: "#/components/schemas/PublicManagementVersionId"}
    DeploymentId:
      name: deployment_id
      in: path
      required: true
      schema: {$ref: "#/components/schemas/PublicManagementDeploymentId"}
    RunId:
      name: run_id
      in: path
      required: true
      schema: {$ref: "#/components/schemas/RunId"}
    TaskId:
      name: task_id
      in: path
      required: true
      schema: {$ref: "#/components/schemas/TaskId"}
    ArtifactId:
      name: artifact_id
      in: path
      required: true
      schema: {$ref: "#/components/schemas/ArtifactId"}
    McpServerId:
      name: mcp_server_id
      in: path
      required: true
      schema: {$ref: "#/components/schemas/McpServerId"}
    McpDeploymentId:
      name: mcp_deployment_id
      in: path
      required: true
      schema: {$ref: "#/components/schemas/McpDeploymentId"}
    ContextId:
      name: context_id
      in: path
      required: true
      schema: {$ref: "#/components/schemas/ContextId"}
    ContextDeploymentId:
      name: context_deployment_id
      in: path
      required: true
      schema: {$ref: "#/components/schemas/ContextDeploymentId"}
    ContextDatasetId:
      name: dataset_id
      in: path
      required: true
      schema: {$ref: "#/components/schemas/ContextDatasetId"}
    DatasetGenerationId:
      name: generation_id
      in: path
      required: true
      schema: {$ref: "#/components/schemas/DatasetGenerationId"}
    IfMatch:
      name: If-Match
      in: header
      required: true
      schema: {type: string, minLength: 1, maxLength: 128}
    IdempotencyKey:
      name: Idempotency-Key
      in: header
      required: true
      schema: {type: string, minLength: 1, maxLength: 255, pattern: "^[ -~]+$"}
  responses:
    ApiProblem:
      description: A bounded, stable public problem response.
      headers:
        Cache-Control:
          $ref: "#/components/headers/NoStore"
      content:
        application/json:
          schema:
            $ref: "#/components/schemas/ApiProblem"
    ResourceResponse:
      description: Current typed Resource projection with a strong aggregate ETag.
      headers:
        ETag: {schema: {type: string, minLength: 1, maxLength: 128}}
        Cache-Control: {$ref: "#/components/headers/PrivateNoStore"}
      content:
        application/json:
          schema: {$ref: "#/components/schemas/ResourceViewV1"}
    ResourceCreatedResponse:
      description: Resource and editable Draft committed atomically or replayed from its Receipt.
      headers:
        Location: {schema: {type: string, pattern: "^/v1/(agents|skills|capabilities|contexts|models|mcp-servers|policies|sandboxes)/"}}
        ETag: {schema: {type: string, minLength: 1, maxLength: 128}}
        Cache-Control: {$ref: "#/components/headers/PrivateNoStore"}
      content:
        application/json:
          schema: {$ref: "#/components/schemas/ResourceViewV1"}
    ResourceVersionResponse:
      description: One immutable typed published Version.
      headers:
        ETag: {schema: {type: string, minLength: 1, maxLength: 128}}
        Cache-Control: {$ref: "#/components/headers/PrivateNoStore"}
      content:
        application/json:
          schema: {$ref: "#/components/schemas/ResourceVersionViewV1"}
    DeploymentResponse:
      description: One immutable typed Deployment closure.
      headers:
        ETag: {schema: {type: string, minLength: 1, maxLength: 128}}
        Cache-Control: {$ref: "#/components/headers/PrivateNoStore"}
      content:
        application/json:
          schema: {$ref: "#/components/schemas/DeploymentViewV1"}
    DeploymentCreatedResponse:
      description: Immutable Deployment closure committed atomically or replayed from its Receipt.
      headers:
        Location: {schema: {type: string, pattern: "^/v1/(agents|skills|capabilities|contexts|models|mcp-servers|policies|sandboxes)/.+/deployments/"}}
        ETag: {schema: {type: string, minLength: 1, maxLength: 128}}
        Cache-Control: {$ref: "#/components/headers/PrivateNoStore"}
      content:
        application/json:
          schema: {$ref: "#/components/schemas/DeploymentViewV1"}
    PublishResourceResponse:
      description: Immutable published Version identities from the fenced Draft generation.
      headers:
        ETag: {schema: {type: string, minLength: 1, maxLength: 128}}
        Cache-Control: {$ref: "#/components/headers/PrivateNoStore"}
      content:
        application/json:
          schema: {$ref: "#/components/schemas/PublishResourceDraftResponseV1"}
    OperationAcceptedResponse:
      description: Shared durable Job accepted or replayed.
      headers:
        Location: {schema: {type: string, pattern: "^/v1/operations/job_"}}
        ETag: {schema: {type: string, minLength: 1, maxLength: 128}}
        Cache-Control: {$ref: "#/components/headers/PrivateNoStore"}
      content:
        application/json:
          schema: {$ref: "#/components/schemas/OperationViewV1"}
    RunControlResponse:
      description: The durable control intent winner and current Run projection.
      headers:
        ETag: {schema: {type: string, minLength: 1, maxLength: 128}}
        Cache-Control: {$ref: "#/components/headers/PrivateNoStore"}
      content:
        application/json:
          schema: {$ref: "#/components/schemas/RunViewV1"}
    TaskControlResponse:
      description: The durable Task first-winner projection.
      headers:
        ETag: {schema: {type: string, minLength: 1, maxLength: 128}}
        Cache-Control: {$ref: "#/components/headers/PrivateNoStore"}
      content:
        application/json:
          schema: {$ref: "#/components/schemas/TaskViewV1"}
  headers:
    NoStore:
      schema:
        type: string
        const: no-store
    PrivateNoStore:
      schema:
        type: string
        const: no-store, private, max-age=0
    NoReferrer:
      schema:
        type: string
        const: no-referrer
  schemas:
    PublicManagementResourceKind:
      type: string
      enum: [agent, skill, capability_interface, context_source_interface, model_profile, mcp_server, policy, sandbox_profile]
    PublicManagementResourceId:
      type: string
      pattern: "^(agt|skl|cap|ctx|mdl|mcp|pol|sxp)_[0-9a-f]{8}-[0-9a-f]{4}-7[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$"
    PublicManagementVersionId:
      type: string
      pattern: "^(aif|arev|srev|cirev|xirev|mdrev|mrev|prev|sxrev)_[0-9a-f]{8}-[0-9a-f]{4}-7[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$"
    PublicManagementDeploymentId:
      type: string
      pattern: "^(adep|skdep|cdep|xdep|mdep|mcdep|pdep|sxdep)_[0-9a-f]{8}-[0-9a-f]{4}-7[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$"
    CreateResourceRequestV1:
      type: object
      additionalProperties: false
      required: [display_name, document]
      properties:
        display_name: {type: string, minLength: 1, maxLength: 255}
        document: {$ref: "#/components/schemas/ResourceDocument"}
    ResourceDraftPayload:
      type: object
      additionalProperties: false
      required: [display_name, document, validation]
      properties:
        display_name: {type: string, minLength: 1, maxLength: 255}
        document: {$ref: "#/components/schemas/ResourceDocument"}
        validation:
          oneOf:
            - {$ref: "#/components/schemas/ValidationSummary"}
            - {type: "null"}
    ValidationSummary:
      type: object
      additionalProperties: false
      required: [validator_digest, validated_draft_digest, dependency_closure_digest, security_evidence_digest, warnings]
      properties:
        validator_digest: {$ref: "#/components/schemas/Digest"}
        validated_draft_digest: {$ref: "#/components/schemas/Digest"}
        dependency_closure_digest: {$ref: "#/components/schemas/Digest"}
        security_evidence_digest: {$ref: "#/components/schemas/Digest"}
        warnings:
          type: array
          maxItems: 256
          items:
            type: object
            additionalProperties: false
            required: [code, path]
            properties:
              code: {type: string, minLength: 1, maxLength: 128, pattern: "^[a-z][a-z0-9_.-]*$"}
              path: {type: string, maxLength: 512}
    PublishResourceDraftRequestV1:
      oneOf:
        - type: object
          additionalProperties: false
          required: [kind, revision_no, content_digest, artifact_id]
          properties:
            kind: {const: single}
            revision_no: {type: integer, minimum: 1}
            content_digest: {$ref: "#/components/schemas/Digest"}
            artifact_id: {oneOf: [{$ref: "#/components/schemas/ArtifactId"}, {type: "null"}]}
        - type: object
          additionalProperties: false
          required: [kind, revision_no, interface_content_digest, plan_content_digest, artifact_id]
          properties:
            kind: {const: agent}
            revision_no: {type: integer, minimum: 1}
            interface_content_digest: {$ref: "#/components/schemas/Digest"}
            plan_content_digest: {$ref: "#/components/schemas/Digest"}
            artifact_id: {oneOf: [{$ref: "#/components/schemas/ArtifactId"}, {type: "null"}]}
    CreateDeploymentRequestV1:
      type: object
      additionalProperties: false
      required: [resource_version_id, environment, closure]
      properties:
        resource_version_id: {$ref: "#/components/schemas/PublicManagementVersionId"}
        environment: {type: string, minLength: 1, maxLength: 128, pattern: "^[A-Za-z0-9_.-]+$"}
        closure: {$ref: "#/components/schemas/DeploymentClosure"}
    ResourceViewV1:
      type: object
      additionalProperties: false
      required: [schema_version, resource_id, resource_kind, lifecycle_state, gate_state, draft_generation, version, draft, etag]
      properties:
        schema_version: {const: 1}
        resource_id: {$ref: "#/components/schemas/PublicManagementResourceId"}
        resource_kind: {$ref: "#/components/schemas/PublicManagementResourceKind"}
        lifecycle_state: {type: string, enum: [active, archived, retired]}
        gate_state: {type: string, enum: [enabled, suspended]}
        draft_generation: {type: integer, minimum: 1}
        version: {type: integer, minimum: 1}
        draft: {$ref: "#/components/schemas/ResourceDraftPayload"}
        etag: {type: string, minLength: 1, maxLength: 128}
    ResourceVersionViewV1:
      type: object
      additionalProperties: false
      required: [schema_version, resource_id, resource_kind, resource_version_id, revision_no, content_digest, artifact_id, payload, created_at, etag]
      properties:
        schema_version: {const: 1}
        resource_id: {$ref: "#/components/schemas/PublicManagementResourceId"}
        resource_kind: {$ref: "#/components/schemas/PublicManagementResourceKind"}
        resource_version_id: {$ref: "#/components/schemas/PublicManagementVersionId"}
        revision_no: {type: integer, minimum: 1}
        content_digest: {$ref: "#/components/schemas/Digest"}
        artifact_id: {oneOf: [{$ref: "#/components/schemas/ArtifactId"}, {type: "null"}]}
        payload:
          type: object
          additionalProperties: false
          required: [document, validation]
          properties:
            document: {$ref: "#/components/schemas/ResourceDocument"}
            validation: {$ref: "#/components/schemas/ValidationSummary"}
        created_at: {$ref: "#/components/schemas/UtcTimestamp"}
        etag: {type: string, minLength: 1, maxLength: 128}
    PublishedResourceVersionSummaryV1:
      type: object
      additionalProperties: false
      required: [resource_version_id, revision_no, content_digest, artifact_id, etag]
      properties:
        resource_version_id: {$ref: "#/components/schemas/PublicManagementVersionId"}
        revision_no: {type: integer, minimum: 1}
        content_digest: {$ref: "#/components/schemas/Digest"}
        artifact_id: {oneOf: [{$ref: "#/components/schemas/ArtifactId"}, {type: "null"}]}
        etag: {type: string, minLength: 1, maxLength: 128}
    PublishResourceDraftResponseV1:
      type: object
      additionalProperties: false
      required: [schema_version, resource_id, resource_kind, draft_generation, version, published_versions, etag]
      properties:
        schema_version: {const: 1}
        resource_id: {$ref: "#/components/schemas/PublicManagementResourceId"}
        resource_kind: {$ref: "#/components/schemas/PublicManagementResourceKind"}
        draft_generation: {type: integer, minimum: 1}
        version: {type: integer, minimum: 1}
        published_versions: {type: array, minItems: 1, maxItems: 2, items: {$ref: "#/components/schemas/PublishedResourceVersionSummaryV1"}}
        etag: {type: string, minLength: 1, maxLength: 128}
    DeploymentViewV1:
      type: object
      additionalProperties: false
      required: [schema_version, deployment_id, resource_id, resource_kind, resource_version_id, environment, closure_digest, closure, created_at, etag]
      properties:
        schema_version: {const: 1}
        deployment_id: {$ref: "#/components/schemas/PublicManagementDeploymentId"}
        resource_id: {$ref: "#/components/schemas/PublicManagementResourceId"}
        resource_kind: {$ref: "#/components/schemas/PublicManagementResourceKind"}
        resource_version_id: {$ref: "#/components/schemas/PublicManagementVersionId"}
        environment: {type: string, minLength: 1, maxLength: 128}
        closure_digest: {$ref: "#/components/schemas/Digest"}
        closure: {$ref: "#/components/schemas/DeploymentClosure"}
        created_at: {$ref: "#/components/schemas/UtcTimestamp"}
        etag: {type: string, minLength: 1, maxLength: 128}
    ResourceDocument:
      description: >-
        Public projection of the Rust ResourceDocument owner union. Each spec is decoded by its
        nominal deny-unknown-fields Rust type and validated before any application command runs.
      x-insight-rust-owner: ResourceDocument
      x-insight-owner-validation-required: true
      oneOf:
        - {$ref: "#/components/schemas/AgentResourceDocument"}
        - {$ref: "#/components/schemas/SkillResourceDocument"}
        - {$ref: "#/components/schemas/CapabilityResourceDocument"}
        - {$ref: "#/components/schemas/ContextResourceDocument"}
        - {$ref: "#/components/schemas/ModelResourceDocument"}
        - {$ref: "#/components/schemas/McpResourceDocument"}
        - {$ref: "#/components/schemas/PolicyResourceDocument"}
        - {$ref: "#/components/schemas/SandboxResourceDocument"}
    AgentResourceDocument:
      type: object
      additionalProperties: false
      required: [resource_kind, spec]
      properties: {resource_kind: {const: agent}, spec: {type: object}}
    SkillResourceDocument:
      type: object
      additionalProperties: false
      required: [resource_kind, spec]
      properties: {resource_kind: {const: skill}, spec: {type: object}}
    CapabilityResourceDocument:
      type: object
      additionalProperties: false
      required: [resource_kind, spec]
      properties: {resource_kind: {const: capability_interface}, spec: {type: object}}
    ContextResourceDocument:
      type: object
      additionalProperties: false
      required: [resource_kind, spec]
      properties: {resource_kind: {const: context_source_interface}, spec: {type: object}}
    ModelResourceDocument:
      type: object
      additionalProperties: false
      required: [resource_kind, spec]
      properties: {resource_kind: {const: model_profile}, spec: {type: object}}
    McpResourceDocument:
      type: object
      additionalProperties: false
      required: [resource_kind, spec]
      properties: {resource_kind: {const: mcp_server}, spec: {type: object}}
    PolicyResourceDocument:
      type: object
      additionalProperties: false
      required: [resource_kind, spec]
      properties: {resource_kind: {const: policy}, spec: {type: object}}
    SandboxResourceDocument:
      type: object
      additionalProperties: false
      required: [resource_kind, spec]
      properties: {resource_kind: {const: sandbox_profile}, spec: {type: object}}
    DeploymentClosure:
      $ref: ./schemas/deployment-closure.schema.json
    RunId:
      type: string
      pattern: "^run_[0-9a-f]{8}-[0-9a-f]{4}-7[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$"
    RunValueId:
      type: string
      pattern: "^val_[0-9a-f]{8}-[0-9a-f]{4}-7[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$"
    AgentId:
      type: string
      pattern: "^agt_[0-9a-f]{8}-[0-9a-f]{4}-7[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$"
    AgentDeploymentId:
      type: string
      pattern: "^adep_[0-9a-f]{8}-[0-9a-f]{4}-7[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$"
    TaskId:
      type: string
      pattern: "^(int|apv)_[0-9a-f]{8}-[0-9a-f]{4}-7[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$"
    ArtifactId:
      type: string
      pattern: "^art_[0-9a-f]{8}-[0-9a-f]{4}-7[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$"
    McpServerId:
      type: string
      pattern: "^mcp_[0-9a-f]{8}-[0-9a-f]{4}-7[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$"
    McpDeploymentId:
      type: string
      pattern: "^mcdep_[0-9a-f]{8}-[0-9a-f]{4}-7[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$"
    McpAuthorizationBindingId:
      type: string
      pattern: "^mab_[0-9a-f]{8}-[0-9a-f]{4}-7[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$"
    ContextId:
      type: string
      pattern: "^ctx_[0-9a-f]{8}-[0-9a-f]{4}-7[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$"
    ContextDeploymentId:
      type: string
      pattern: "^xdep_[0-9a-f]{8}-[0-9a-f]{4}-7[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$"
    ContextDatasetId:
      type: string
      pattern: "^dset_[0-9a-f]{8}-[0-9a-f]{4}-7[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$"
    DatasetGenerationId:
      type: string
      pattern: "^dgen_[0-9a-f]{8}-[0-9a-f]{4}-7[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$"
    DataClassification:
      type: string
      enum: [public, internal, confidential, restricted]
    ValueRef:
      oneOf:
        - type: object
          additionalProperties: false
          required: [kind, value]
          properties:
            kind: {const: inline}
            value: {}
        - type: object
          additionalProperties: false
          required: [kind, artifact]
          properties:
            kind: {const: artifact}
            artifact: {$ref: "#/components/schemas/ArtifactRef"}
    CreateRunRequestV1:
      type: object
      additionalProperties: false
      required: [agent_id, input, deadline]
      properties:
        agent_id: {$ref: "#/components/schemas/AgentId"}
        input:
          type: object
          additionalProperties: false
          required: [classification, schema_digest, value]
          properties:
            classification: {$ref: "#/components/schemas/DataClassification"}
            schema_digest: {$ref: "#/components/schemas/Digest"}
            value: {$ref: "#/components/schemas/ValueRef"}
        deadline: {$ref: "#/components/schemas/UtcTimestamp"}
    DiscoverMcpDeploymentRequestV1:
      type: object
      additionalProperties: false
      required: [schema_version, authorization_binding_id, deadline]
      properties:
        schema_version: {const: 1}
        authorization_binding_id: {$ref: "#/components/schemas/McpAuthorizationBindingId"}
        deadline: {$ref: "#/components/schemas/UtcTimestamp"}
    BuildContextDatasetRequestV1:
      type: object
      additionalProperties: false
      required: [schema_version, dataset_id, deadline]
      properties:
        schema_version: {const: 1}
        dataset_id:
          oneOf:
            - {$ref: "#/components/schemas/ContextDatasetId"}
            - {type: "null"}
        deadline: {$ref: "#/components/schemas/UtcTimestamp"}
    RunViewV1:
      type: object
      additionalProperties: false
      required: [schema_version, run_id, agent_deployment_id, state, version, input_value_id, output_value_id, pause_generation, cancel_generation, deadline, started_at, terminal_at, created_at, updated_at, etag]
      properties:
        schema_version: {const: 1}
        run_id: {$ref: "#/components/schemas/RunId"}
        agent_deployment_id: {$ref: "#/components/schemas/AgentDeploymentId"}
        state: {type: string, enum: [queued, running, waiting, cancelling, succeeded, failed, cancelled, timed_out]}
        version: {type: integer, minimum: 1}
        input_value_id: {$ref: "#/components/schemas/RunValueId"}
        output_value_id:
          oneOf:
            - {$ref: "#/components/schemas/RunValueId"}
            - {type: "null"}
        pause_generation: {type: integer, minimum: 0}
        cancel_generation: {type: integer, minimum: 0}
        deadline: {$ref: "#/components/schemas/UtcTimestamp"}
        started_at: {oneOf: [{$ref: "#/components/schemas/UtcTimestamp"}, {type: "null"}]}
        terminal_at: {oneOf: [{$ref: "#/components/schemas/UtcTimestamp"}, {type: "null"}]}
        created_at: {$ref: "#/components/schemas/UtcTimestamp"}
        updated_at: {$ref: "#/components/schemas/UtcTimestamp"}
        etag: {type: string, minLength: 1, maxLength: 128}
    RunResultViewV1:
      type: object
      additionalProperties: false
      required: [schema_version, run_id, value_id, classification, schema_digest, content_digest, value]
      properties:
        schema_version: {const: 1}
        run_id: {$ref: "#/components/schemas/RunId"}
        value_id: {$ref: "#/components/schemas/RunValueId"}
        classification: {$ref: "#/components/schemas/DataClassification"}
        schema_digest: {$ref: "#/components/schemas/Digest"}
        content_digest: {$ref: "#/components/schemas/Digest"}
        value: {$ref: "#/components/schemas/ValueRef"}
    SubmitTaskInputV1:
      type: object
      additionalProperties: false
      required: [classification, schema_digest, value]
      properties:
        classification: {$ref: "#/components/schemas/DataClassification"}
        schema_digest: {$ref: "#/components/schemas/Digest"}
        value: {$ref: "#/components/schemas/ValueRef"}
    TaskViewV1:
      type: object
      additionalProperties: false
      required: [schema_version, task_id, task_kind, state, generation, version, safe_prompt_key, response_schema_digest, owner, deadline, responded_at, created_at, updated_at, etag]
      properties:
        schema_version: {const: 1}
        task_id: {$ref: "#/components/schemas/TaskId"}
        task_kind: {type: string, enum: [approval, interaction_form, interaction_url_consent, interaction_business_input, external_authorization, human_work]}
        state: {type: string, enum: [pending, responded, declined, approved, rejected, cancelled, expired]}
        generation: {type: integer, minimum: 1}
        version: {type: integer, minimum: 1}
        safe_prompt_key: {type: string, minLength: 1, maxLength: 128, pattern: "^[a-z][a-z0-9_.-]*$"}
        response_schema_digest: {oneOf: [{$ref: "#/components/schemas/Digest"}, {type: "null"}]}
        owner:
          oneOf:
            - type: object
              additionalProperties: false
              required: [kind, run_id]
              properties: {kind: {const: run}, run_id: {$ref: "#/components/schemas/RunId"}}
            - type: object
              additionalProperties: false
              required: [kind, invocation_id]
              properties: {kind: {const: invocation}, invocation_id: {$ref: "#/components/schemas/PlatformResourceId"}}
            - type: object
              additionalProperties: false
              required: [kind, artifact_id]
              properties: {kind: {const: artifact}, artifact_id: {$ref: "#/components/schemas/ArtifactId"}}
        deadline: {$ref: "#/components/schemas/UtcTimestamp"}
        responded_at: {oneOf: [{$ref: "#/components/schemas/UtcTimestamp"}, {type: "null"}]}
        created_at: {$ref: "#/components/schemas/UtcTimestamp"}
        updated_at: {$ref: "#/components/schemas/UtcTimestamp"}
        etag: {type: string, minLength: 1, maxLength: 128}
    PrepareArtifactUploadRequestV1:
      type: object
      additionalProperties: false
      required: [schema_version, purpose, classification, expected_size_bytes, expected_digest, declared_media_type, display_name]
      properties:
        schema_version: {const: 1}
        purpose: {type: string, enum: [authoring_document, interface_contract, typed_plan, package, sbom, backend_binding, model_generation_defaults, run_input, run_output, capability_input, capability_output, context_source, context_derived, mcp_resource, sandbox_input, sandbox_output, diagnostic, export]}
        classification: {$ref: "#/components/schemas/DataClassification"}
        expected_size_bytes: {type: integer, minimum: 1, maximum: 1073741824}
        expected_digest: {oneOf: [{$ref: "#/components/schemas/Digest"}, {type: "null"}]}
        declared_media_type: {oneOf: [{type: string, minLength: 3, maxLength: 255}, {type: "null"}]}
        display_name: {oneOf: [{type: string, minLength: 1, maxLength: 512}, {type: "null"}]}
    OpaqueUploadCompletionProof:
      type: string
      minLength: 1
      maxLength: 4096
      pattern: "^[A-Za-z0-9._~-]+$"
      writeOnly: true
      x-insight-secret-bearing: true
    SecretBearingUploadTargetV1:
      type: object
      additionalProperties: false
      required: [url, completion_proof]
      x-insight-secret-bearing: true
      properties:
        url: {type: string, format: uri, pattern: "^https://", maxLength: 8192, writeOnly: true}
        completion_proof: {$ref: "#/components/schemas/OpaqueUploadCompletionProof"}
    PrepareArtifactUploadResponseV1:
      type: object
      additionalProperties: false
      required: [schema_version, artifact_id, operation_id, upload_grant_id, artifact_etag, upload_target, upload_expires_at]
      properties:
        schema_version: {const: 1}
        artifact_id: {$ref: "#/components/schemas/ArtifactId"}
        operation_id: {$ref: "#/components/schemas/JobId"}
        upload_grant_id: {type: string, pattern: "^grt_[0-9a-f]{8}-[0-9a-f]{4}-7[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$", minLength: 40, maxLength: 40}
        artifact_etag: {type: string, minLength: 3, maxLength: 128}
        upload_target: {$ref: "#/components/schemas/SecretBearingUploadTargetV1"}
        upload_expires_at: {$ref: "#/components/schemas/UtcTimestamp"}
    CompleteArtifactUploadRequestV1:
      type: object
      additionalProperties: false
      required: [schema_version, completion_proof]
      properties:
        schema_version: {const: 1}
        completion_proof: {$ref: "#/components/schemas/OpaqueUploadCompletionProof"}
    ArtifactMutationAcceptedV1:
      type: object
      additionalProperties: false
      required: [schema_version, artifact_id, artifact_etag, operation_id]
      properties:
        schema_version: {const: 1}
        artifact_id: {$ref: "#/components/schemas/ArtifactId"}
        artifact_etag: {type: string, minLength: 3, maxLength: 128}
        operation_id: {$ref: "#/components/schemas/JobId"}
    ArtifactViewV1:
      type: object
      additionalProperties: false
      required: [schema_version, artifact_id, purpose, classification, state, version, expected_size_bytes, declared_media_type, verified_media_type, content, retain_until, created_at, updated_at, etag]
      properties:
        schema_version: {const: 1}
        artifact_id: {$ref: "#/components/schemas/ArtifactId"}
        purpose: {type: string, enum: [authoring_document, interface_contract, typed_plan, package, sbom, backend_binding, model_generation_defaults, run_input, run_output, capability_input, capability_output, context_source, context_derived, mcp_resource, sandbox_input, sandbox_output, diagnostic, export]}
        classification: {$ref: "#/components/schemas/DataClassification"}
        state: {type: string, enum: [staging, uploaded, verifying, verified, ready, quarantined, rejected, deleting, deleted, corrupt]}
        version: {type: integer, minimum: 1}
        expected_size_bytes: {type: integer, minimum: 1}
        declared_media_type: {oneOf: [{type: string, minLength: 1, maxLength: 255}, {type: "null"}]}
        verified_media_type: {oneOf: [{type: string, minLength: 1, maxLength: 255}, {type: "null"}]}
        content: {oneOf: [{$ref: "#/components/schemas/ArtifactRef"}, {type: "null"}]}
        retain_until: {$ref: "#/components/schemas/UtcTimestamp"}
        created_at: {$ref: "#/components/schemas/UtcTimestamp"}
        updated_at: {$ref: "#/components/schemas/UtcTimestamp"}
        etag: {type: string, minLength: 1, maxLength: 128}
    JobId:
      type: string
      pattern: "^job_[0-9a-f]{8}-[0-9a-f]{4}-7[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$"
      minLength: 40
      maxLength: 40
    PublicJobState:
      type: string
      enum:
        - queued
        - running
        - waiting
        - succeeded
        - failed
        - cancelled
        - timed_out
        - reconciliation_required
    PublicJobTarget:
      oneOf:
        - type: object
          additionalProperties: false
          required: [kind, resource_id, resource_version]
          properties:
            kind: {const: resource_version}
            resource_id: {$ref: "#/components/schemas/PlatformResourceId"}
            resource_version: {type: integer, minimum: 1}
        - type: object
          additionalProperties: false
          required: [kind, deployment_id]
          properties:
            kind: {const: deployment}
            deployment_id: {$ref: "#/components/schemas/PlatformResourceId"}
        - type: object
          additionalProperties: false
          required: [kind, context_dataset_id]
          properties:
            kind: {const: context_dataset}
            context_dataset_id: {$ref: "#/components/schemas/PlatformResourceId"}
        - type: object
          additionalProperties: false
          required: [kind, artifact_id]
          properties:
            kind: {const: artifact}
            artifact_id: {$ref: "#/components/schemas/PlatformResourceId"}
    OperationViewV1:
      type: object
      additionalProperties: false
      required:
        - operation_id
        - tenant_id
        - kind
        - target
        - state
        - progress
        - result
        - error
        - created_at
        - updated_at
        - etag
      properties:
        operation_id: {$ref: "#/components/schemas/JobId"}
        tenant_id: {$ref: "#/components/schemas/PlatformResourceId"}
        kind:
          type: string
          enum: [resource_validation, mcp_discovery, context_dataset_build, artifact_verify, artifact_delete]
        target: {$ref: "#/components/schemas/PublicJobTarget"}
        state: {$ref: "#/components/schemas/PublicJobState"}
        progress:
          oneOf:
            - type: object
              additionalProperties: false
              required: [completed_units, total_units]
              properties:
                completed_units: {type: integer, minimum: 0}
                total_units: {type: integer, minimum: 1}
            - type: "null"
        result:
          oneOf:
            - type: object
              additionalProperties: false
              required: [result_digest]
              properties:
                result_digest: {$ref: "#/components/schemas/Digest"}
            - type: "null"
        error:
          oneOf:
            - type: object
              additionalProperties: false
              required: [code, message]
              properties:
                code: {type: string, pattern: "^[a-z][a-z0-9_]{0,63}$", maxLength: 64}
                message: {type: string, minLength: 1, maxLength: 512}
            - type: "null"
        created_at: {$ref: "#/components/schemas/UtcTimestamp"}
        updated_at: {$ref: "#/components/schemas/UtcTimestamp"}
        etag: {type: string, minLength: 1, maxLength: 128}
    PolicyExactVersionRef:
      type: object
      additionalProperties: false
      required: [revision_id, resource_kind, semantic_digest]
      properties:
        revision_id:
          type: string
          pattern: "^polr_[0-9a-f]{8}-[0-9a-f]{4}-7[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$"
        resource_kind: {const: policy_revision}
        semantic_digest: {$ref: "#/components/schemas/Digest"}
    ContextExactDeploymentRef:
      type: object
      additionalProperties: false
      required: [deployment_id, resource_kind, deployment_digest]
      properties:
        deployment_id: {$ref: "#/components/schemas/ContextDeploymentId"}
        resource_kind: {const: context_deployment}
        deployment_digest: {$ref: "#/components/schemas/Digest"}
    ModelExactDeploymentRef:
      type: object
      additionalProperties: false
      required: [deployment_id, resource_kind, deployment_digest]
      properties:
        deployment_id:
          type: string
          pattern: "^mdep_[0-9a-f]{8}-[0-9a-f]{4}-7[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$"
        resource_kind: {const: model_deployment}
        deployment_digest: {$ref: "#/components/schemas/Digest"}
    ContextDatasetGenerationSpec:
      type: object
      additionalProperties: false
      required: [context_deployment, source_manifest_digest, parser_profile, chunker_profile, embedding_model_deployment, ranking_profile, index_manifest, validation_evidence, created_by_operation_id]
      properties:
        context_deployment: {$ref: "#/components/schemas/ContextExactDeploymentRef"}
        source_manifest_digest: {$ref: "#/components/schemas/Digest"}
        parser_profile: {$ref: "#/components/schemas/PolicyExactVersionRef"}
        chunker_profile: {$ref: "#/components/schemas/PolicyExactVersionRef"}
        embedding_model_deployment:
          oneOf:
            - {$ref: "#/components/schemas/ModelExactDeploymentRef"}
            - {type: "null"}
        ranking_profile: {$ref: "#/components/schemas/PolicyExactVersionRef"}
        index_manifest: {$ref: "#/components/schemas/ArtifactRef"}
        validation_evidence: {$ref: "#/components/schemas/ArtifactRef"}
        created_by_operation_id: {$ref: "#/components/schemas/JobId"}
    ContextDatasetPublishedVersionPayload:
      type: object
      additionalProperties: false
      required: [document, validation]
      properties:
        document:
          type: object
          additionalProperties: false
          required: [resource_kind, spec]
          properties:
            resource_kind: {const: context_dataset}
            spec:
              type: object
              additionalProperties: false
              required: [authoring_package, contract_digest, dependency_versions, policy_versions, generation]
              properties:
                authoring_package:
                  type: object
                  additionalProperties: false
                  required: [artifact, manifest_digest]
                  properties:
                    artifact: {$ref: "#/components/schemas/ArtifactRef"}
                    manifest_digest: {$ref: "#/components/schemas/Digest"}
                contract_digest: {$ref: "#/components/schemas/Digest"}
                dependency_versions:
                  type: array
                  minItems: 3
                  maxItems: 3
                  items: {$ref: "#/components/schemas/PolicyExactVersionRef"}
                policy_versions:
                  type: array
                  minItems: 1
                  maxItems: 1
                  items: {$ref: "#/components/schemas/PolicyExactVersionRef"}
                generation: {$ref: "#/components/schemas/ContextDatasetGenerationSpec"}
        validation:
          type: object
          additionalProperties: false
          required: [validator_digest, validated_draft_digest, dependency_closure_digest, security_evidence_digest, warnings]
          properties:
            validator_digest: {$ref: "#/components/schemas/Digest"}
            validated_draft_digest: {$ref: "#/components/schemas/Digest"}
            dependency_closure_digest: {$ref: "#/components/schemas/Digest"}
            security_evidence_digest: {$ref: "#/components/schemas/Digest"}
            warnings:
              type: array
              maxItems: 256
              items:
                type: object
                additionalProperties: false
                required: [code, path]
                properties:
                  code: {type: string, pattern: "^[a-z][a-z0-9_]{0,63}$"}
                  path: {type: string, maxLength: 512}
    ContextDatasetGenerationViewV1:
      type: object
      additionalProperties: false
      required: [schema_version, resource_id, resource_kind, resource_version_id, revision_no, content_digest, artifact_id, payload, created_at, etag]
      properties:
        schema_version: {const: 1}
        resource_id: {$ref: "#/components/schemas/ContextDatasetId"}
        resource_kind: {const: context_dataset}
        resource_version_id: {$ref: "#/components/schemas/DatasetGenerationId"}
        revision_no: {type: integer, minimum: 1}
        content_digest: {$ref: "#/components/schemas/Digest"}
        artifact_id:
          oneOf:
            - {$ref: "#/components/schemas/ArtifactId"}
            - {type: "null"}
        payload: {$ref: "#/components/schemas/ContextDatasetPublishedVersionPayload"}
        created_at: {$ref: "#/components/schemas/UtcTimestamp"}
        etag: {type: string, minLength: 1, maxLength: 128}
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
    (WorkClass::RegistryValidation, ResourceKind::Job),
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
    (WorkClass::Context, ResourceKind::ContextDataset),
    (WorkClass::Sandbox, ResourceKind::Job),
    (WorkClass::Interaction, ResourceKind::Interaction),
    (WorkClass::Artifact, ResourceKind::Artifact),
    (WorkClass::Artifact, ResourceKind::InternalBlob),
    (WorkClass::Recovery, ResourceKind::Run),
    (WorkClass::Recovery, ResourceKind::NodeExecution),
    (WorkClass::Recovery, ResourceKind::CapabilityInvocation),
    (WorkClass::Recovery, ResourceKind::ContextQuery),
    (WorkClass::Recovery, ResourceKind::McpOperation),
    (WorkClass::Recovery, ResourceKind::ModelTurn),
    (WorkClass::Recovery, ResourceKind::Job),
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
    "contracts/platform-v1/schemas/deployment-closure.schema.json",
    "contracts/platform-v1/schemas/worker-manifest.schema.json",
    "contracts/platform-v1/schemas/candidate-manifest.schema.json",
    "contracts/platform-v1/schemas/policies/artifact-retention-policy.schema.json",
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
        "public_job_kinds": wire_values(PublicJobKind::ALL, |value| value.as_str()),
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
    let deployment_closure_schema = deployment_closure_schema();
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
            "schemas/deployment-closure.schema.json",
            pretty(&deployment_closure_schema),
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

fn deployment_variant_schema(resource_kind: &str, properties: Value) -> Value {
    let binding_properties = properties
        .as_object()
        .expect("deployment binding properties are an object");
    let required = binding_properties.keys().cloned().collect::<Vec<_>>();
    json!({
        "type": "object",
        "additionalProperties": false,
        "required": ["resource_kind", "bindings"],
        "properties": {
            "resource_kind": {"const": resource_kind},
            "bindings": {
                "type": "object",
                "additionalProperties": false,
                "required": required,
                "properties": properties
            }
        }
    })
}

fn tagged_content_variant(kind: &str, properties: Value) -> Value {
    let fields = properties
        .as_object()
        .expect("tagged content properties are an object");
    let required = fields.keys().cloned().collect::<Vec<_>>();
    json!({
        "type": "object", "additionalProperties": false,
        "required": ["kind", "binding"],
        "properties": {
            "kind": {"const": kind},
            "binding": {
                "type": "object", "additionalProperties": false,
                "required": required, "properties": properties
            }
        }
    })
}

fn tagged_flat_variant(kind: &str, properties: Value) -> Value {
    let mut properties = properties
        .as_object()
        .expect("tagged properties are an object")
        .clone();
    properties.insert("kind".to_owned(), json!({"const": kind}));
    let required = properties.keys().cloned().collect::<Vec<_>>();
    json!({
        "type": "object", "additionalProperties": false,
        "required": required, "properties": properties
    })
}

fn exact_secret_binding_schema() -> Value {
    json!({
        "type": "object", "additionalProperties": false,
        "required": ["secret_binding_id", "binding_generation", "provider_id", "purpose", "resolution_policy", "resolution_policy_digest"],
        "properties": {
            "secret_binding_id": {"$ref": "resource-id.schema.json"},
            "binding_generation": {"type": "integer", "minimum": 1},
            "provider_id": {"$ref": "resource-id.schema.json"},
            "purpose": {"type": "string", "minLength": 1, "maxLength": 128},
            "resolution_policy": {
                "oneOf": [
                    tagged_flat_variant("pinned", json!({
                        "opaque_version_identity_digest": {"$ref": "nominal/digest.schema.json"}
                    })),
                    tagged_flat_variant("follow_provider_rotation", json!({
                        "rotation_policy_revision_id": {"$ref": "resource-id.schema.json"}
                    }))
                ]
            },
            "resolution_policy_digest": {"$ref": "nominal/digest.schema.json"}
        }
    })
}

fn endpoint_schema() -> Value {
    json!({
        "type": "object", "additionalProperties": false,
        "required": ["scheme", "host", "port", "base_path"],
        "properties": {
            "scheme": {"type": "string", "enum": ["http", "https"]},
            "host": {"type": "string", "minLength": 1, "maxLength": 253},
            "port": {"type": "integer", "minimum": 1, "maximum": 65535},
            "base_path": {"type": "string", "minLength": 1, "maxLength": 8192}
        }
    })
}

fn capability_backend_binding_schema() -> Value {
    let version = json!({"$ref": "#/$defs/ExactVersionRef"});
    let digest = json!({"$ref": "nominal/digest.schema.json"});
    let endpoint = endpoint_schema();
    let remote = |kind: &str| {
        tagged_content_variant(
            kind,
            json!({
                "endpoint": endpoint.clone(),
                "endpoint_identity_digest": digest.clone(),
                "network_policy": version.clone(),
                "tls_policy": version.clone(),
                "trust_policy": version.clone()
            }),
        )
    };
    json!({"oneOf": [
        tagged_content_variant("native", json!({
            "worker_manifest_digest": digest.clone(),
            "adapter_module_digest": digest.clone()
        })),
        remote("http"),
        remote("grpc"),
        tagged_content_variant("mcp", json!({
            "mcp_deployment": {"$ref": "#/$defs/ExactDeploymentRef"},
            "discovery_snapshot_id": {"$ref": "resource-id.schema.json"},
            "discovery_snapshot_digest": digest.clone(),
            "authorization_policy": version.clone()
        })),
        tagged_content_variant("sandbox", json!({
            "runtime": version.clone(),
            "package": version.clone(),
            "profile": {
                "type": "object", "additionalProperties": false,
                "required": ["deployment", "revision"],
                "properties": {
                    "deployment": {"$ref": "#/$defs/ExactDeploymentRef"},
                    "revision": version.clone()
                }
            },
            "isolation": {"type": "string", "enum": ["wasm", "sandboxed_container"]},
            "network_policy": version.clone(),
            "resource_policy": version.clone(),
            "artifact_io_policy": version.clone(),
            "secret_policy": {"oneOf": [version, {"type": "null"}]}
        }))
    ]})
}

fn context_backend_binding_schema() -> Value {
    let digest = json!({"$ref": "nominal/digest.schema.json"});
    let region = json!({"type": "string", "minLength": 1, "maxLength": 32});
    json!({"oneOf": [
        tagged_flat_variant("managed_index", json!({
            "service_identity_digest": digest.clone(), "index_namespace_digest": digest.clone(), "region": region.clone()
        })),
        tagged_flat_variant("remote_search", json!({
            "endpoint_identity_digest": digest.clone(), "region": region
        })),
        tagged_flat_variant("mcp_resources", json!({
            "mcp_deployment": {"$ref": "#/$defs/ExactDeploymentRef"},
            "discovery_snapshot_id": {"$ref": "resource-id.schema.json"},
            "discovery_snapshot_digest": digest.clone()
        })),
        tagged_flat_variant("sql_catalog", json!({
            "database_identity_digest": digest.clone(),
            "dialect": {"type": "string", "minLength": 1, "maxLength": 128},
            "catalog_scope_digest": digest.clone()
        })),
        tagged_flat_variant("artifact_collection", json!({"collection_identity_digest": digest.clone()})),
        tagged_flat_variant("native_catalog", json!({"installed_adapter_digest": digest}))
    ]})
}

fn mcp_transport_binding_schema() -> Value {
    json!({"oneOf": [tagged_content_variant("streamable_http", json!({
        "endpoint": endpoint_schema(),
        "endpoint_identity_digest": {"$ref": "nominal/digest.schema.json"},
        "network_policy": {"$ref": "#/$defs/ExactVersionRef"},
        "tls_policy": {"$ref": "#/$defs/ExactVersionRef"}
    }))]})
}

fn deployment_closure_schema() -> Value {
    let exact_version_ref = json!({
        "type": "object",
        "additionalProperties": false,
        "required": ["revision_id", "resource_kind", "semantic_digest"],
        "properties": {
            "revision_id": {"$ref": "resource-id.schema.json"},
            "resource_kind": {
                "type": "string",
                "enum": ResourceKind::ALL.iter()
                    .filter(|kind| kind.is_revision())
                    .map(|kind| kind.descriptor().name)
                    .collect::<Vec<_>>()
            },
            "semantic_digest": {"$ref": "nominal/digest.schema.json"}
        }
    });
    let exact_deployment_ref = json!({
        "type": "object",
        "additionalProperties": false,
        "required": ["deployment_id", "resource_kind", "deployment_digest"],
        "properties": {
            "deployment_id": {"$ref": "resource-id.schema.json"},
            "resource_kind": {
                "type": "string",
                "enum": ResourceKind::ALL.iter()
                    .filter(|kind| kind.is_deployment())
                    .map(|kind| kind.descriptor().name)
                    .collect::<Vec<_>>()
            },
            "deployment_digest": {"$ref": "nominal/digest.schema.json"}
        }
    });
    let exact_policy_binding = json!({
        "type": "object",
        "additionalProperties": false,
        "required": ["deployment", "revision"],
        "properties": {
            "deployment": {"$ref": "#/$defs/ExactDeploymentRef"},
            "revision": {"$ref": "#/$defs/ExactVersionRef"}
        }
    });
    let exact_secret_binding = exact_secret_binding_schema();
    let capability_backend = capability_backend_binding_schema();
    let context_backend = context_backend_binding_schema();
    let mcp_transport = mcp_transport_binding_schema();
    json!({
        "$schema": "https://json-schema.org/draft/2020-12/schema",
        "$id": "urn:insight:platform:v1:deployment-closure",
        "title": "DeploymentClosure",
        "description": "Closed immutable Deployment closure variants generated from the Rust owner contract. Public management nouns and internal Model Provider deployments share this exact owner schema.",
        "oneOf": [
            {"$ref": "#/$defs/AgentDeploymentClosure"},
            {"$ref": "#/$defs/SkillDeploymentClosure"},
            {"$ref": "#/$defs/CapabilityDeploymentClosure"},
            {"$ref": "#/$defs/ContextDeploymentClosure"},
            {"$ref": "#/$defs/McpDeploymentClosure"},
            {"$ref": "#/$defs/ModelProviderDeploymentClosure"},
            {"$ref": "#/$defs/ModelDeploymentClosure"},
            {"$ref": "#/$defs/PolicyDeploymentClosure"},
            {"$ref": "#/$defs/SandboxProfileDeploymentClosure"}
        ],
        "$defs": {
            "ExactVersionRef": exact_version_ref,
            "ExactDeploymentRef": exact_deployment_ref,
            "ExactPolicyBinding": exact_policy_binding,
            "ExactSecretBindingRef": exact_secret_binding,
            "CapabilityBackendBinding": capability_backend,
            "ContextBackendBinding": context_backend,
            "McpTransportBinding": mcp_transport,
            "AgentDeploymentClosure": deployment_variant_schema("agent", json!({
                "interface": {"$ref": "#/$defs/ExactVersionRef"},
                "plan": {"$ref": "#/$defs/ExactVersionRef"},
                "entry_node_id": {"type": "string", "minLength": 1, "maxLength": 128},
                "entry_node_kind": {"type": "string", "enum": wire_values(PlanNodeKind::ALL, |value| value.as_str())},
                "slots": {"type": "array", "maxItems": 512, "items": {"$ref": "frozen-slot-binding.schema.json"}},
                "policies": {"type": "array", "maxItems": 64, "items": {"$ref": "#/$defs/ExactPolicyBinding"}},
                "execution_profile": {"$ref": "#/$defs/ExactPolicyBinding"}
            })),
            "SkillDeploymentClosure": {
                "type": "object",
                "additionalProperties": false,
                "required": ["resource_kind", "bindings"],
                "properties": {
                    "resource_kind": {"const": "skill"},
                    "bindings": {
                        "type": "object",
                        "additionalProperties": false,
                        "required": ["skill_revision", "requirements", "selection_policy", "qualification_evidence"],
                        "properties": {
                            "skill_revision": {"$ref": "#/$defs/ExactVersionRef"},
                            "requirements": {
                                "type": "array",
                                "maxItems": 512,
                                "items": {"$ref": "frozen-slot-binding.schema.json"}
                            },
                            "selection_policy": {"$ref": "#/$defs/ExactPolicyBinding"},
                            "qualification_evidence": {"$ref": "nominal/artifact-ref.schema.json"}
                        }
                    }
                }
            },
            "CapabilityDeploymentClosure": deployment_variant_schema("capability_interface", json!({
                "implementation": {"$ref": "#/$defs/ExactVersionRef"},
                "interface": {"$ref": "#/$defs/ExactVersionRef"},
                "backend": {"$ref": "#/$defs/CapabilityBackendBinding"},
                "secret_bindings": {"type": "array", "maxItems": 64, "items": {"$ref": "#/$defs/ExactSecretBindingRef"}},
                "policies": {"type": "array", "maxItems": 64, "items": {"$ref": "#/$defs/ExactVersionRef"}},
                "conformance_evidence": {"$ref": "nominal/artifact-ref.schema.json"}
            })),
            "ContextDeploymentClosure": deployment_variant_schema("context_source_interface", json!({
                "implementation": {"$ref": "#/$defs/ExactVersionRef"},
                "interface": {"$ref": "#/$defs/ExactVersionRef"},
                "backend": {"$ref": "#/$defs/ContextBackendBinding"},
                "secret_bindings": {"type": "array", "maxItems": 64, "items": {"$ref": "#/$defs/ExactSecretBindingRef"}},
                "network_policy": {"oneOf": [{"$ref": "#/$defs/ExactVersionRef"}, {"type": "null"}]},
                "parser_policy": {"$ref": "#/$defs/ExactVersionRef"},
                "chunker_policy": {"$ref": "#/$defs/ExactVersionRef"},
                "embedding_model_deployment": {"oneOf": [{"$ref": "#/$defs/ExactDeploymentRef"}, {"type": "null"}]},
                "ranking_policy": {"$ref": "#/$defs/ExactVersionRef"},
                "data_policy": {"$ref": "#/$defs/ExactVersionRef"},
                "conformance_evidence": {"$ref": "nominal/artifact-ref.schema.json"}
            })),
            "McpDeploymentClosure": deployment_variant_schema("mcp_server", json!({
                "server_revision": {"$ref": "#/$defs/ExactVersionRef"},
                "server_identity_digest": {"$ref": "nominal/digest.schema.json"},
                "transport": {"$ref": "#/$defs/McpTransportBinding"},
                "protocol_policy": {"$ref": "#/$defs/ExactVersionRef"},
                "trust_policy": {"$ref": "#/$defs/ExactVersionRef"},
                "auth_policy": {"oneOf": [{"$ref": "#/$defs/ExactVersionRef"}, {"type": "null"}]},
                "secret_bindings": {"type": "array", "maxItems": 64, "items": {"$ref": "#/$defs/ExactSecretBindingRef"}},
                "conformance_evidence": {"$ref": "nominal/artifact-ref.schema.json"}
            })),
            "ModelProviderDeploymentClosure": deployment_variant_schema("model_provider", json!({
                "provider_revision": {"$ref": "#/$defs/ExactVersionRef"},
                "endpoint_identity_digest": {"$ref": "nominal/digest.schema.json"},
                "secret_bindings": {"type": "array", "maxItems": 64, "items": {"$ref": "#/$defs/ExactSecretBindingRef"}},
                "protocol_policy": {"$ref": "#/$defs/ExactVersionRef"},
                "network_policy": {"$ref": "#/$defs/ExactVersionRef"},
                "tls_policy": {"$ref": "#/$defs/ExactVersionRef"},
                "trust_policy": {"$ref": "#/$defs/ExactVersionRef"},
                "data_policy": {"$ref": "#/$defs/ExactVersionRef"},
                "region": {"type": "string", "minLength": 1, "maxLength": 32},
                "conformance_evidence": {"$ref": "nominal/artifact-ref.schema.json"}
            })),
            "ModelDeploymentClosure": deployment_variant_schema("model_profile", json!({
                "profile_revision": {"$ref": "#/$defs/ExactVersionRef"},
                "provider_deployment": {"$ref": "#/$defs/ExactDeploymentRef"},
                "data_policy": {"$ref": "#/$defs/ExactVersionRef"},
                "budget_policy": {"$ref": "#/$defs/ExactVersionRef"},
                "public_projection_policy": {"$ref": "#/$defs/ExactVersionRef"},
                "generation_defaults": {
                    "type": "object", "additionalProperties": false,
                    "required": ["schema_digest", "value", "canonical_digest"],
                    "properties": {
                        "schema_digest": {"$ref": "nominal/digest.schema.json"},
                        "value": {},
                        "canonical_digest": {"$ref": "nominal/digest.schema.json"}
                    }
                }
            })),
            "PolicyDeploymentClosure": {
                "type": "object",
                "additionalProperties": false,
                "required": ["resource_kind", "bindings"],
                "properties": {
                    "resource_kind": {"const": "policy"},
                    "bindings": {
                        "type": "object",
                        "additionalProperties": false,
                        "required": ["policy_revision", "applicability_digest", "qualification_evidence"],
                        "properties": {
                            "policy_revision": {"$ref": "#/$defs/ExactVersionRef"},
                            "applicability_digest": {"$ref": "nominal/digest.schema.json"},
                            "qualification_evidence": {"$ref": "nominal/artifact-ref.schema.json"}
                        }
                    }
                }
            },
            "SandboxProfileDeploymentClosure": {
                "type": "object",
                "additionalProperties": false,
                "required": ["resource_kind", "bindings"],
                "properties": {
                    "resource_kind": {"const": "sandbox_profile"},
                    "bindings": {
                        "type": "object",
                        "additionalProperties": false,
                        "required": ["profile_revision", "runtime_revision", "policy_bindings", "qualification_evidence"],
                        "properties": {
                            "profile_revision": {"$ref": "#/$defs/ExactVersionRef"},
                            "runtime_revision": {"$ref": "#/$defs/ExactVersionRef"},
                            "policy_bindings": {
                                "type": "array",
                                "maxItems": 512,
                                "items": {"$ref": "#/$defs/ExactPolicyBinding"}
                            },
                            "qualification_evidence": {"$ref": "nominal/artifact-ref.schema.json"}
                        }
                    }
                }
            }
        }
    })
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
        "description": "Content-addressed CI/CD closure for one exact source, contract, schema, image, worker, configuration, limit, policy and qualification profile set; it is not runtime state.",
        "type": "object",
        "additionalProperties": false,
        "required": [
            "git_commit",
            "contract_digest",
            "database_schema_version",
            "component_images",
            "worker_manifests",
            "deployment_config_digest",
            "hard_limit_profile_digest",
            "policy_baseline_digest",
            "qualification_profile_digest",
            "created_at"
        ],
        "properties": {
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
                    "enum": crate::ComponentRole::ALL.iter().map(|role| role.as_str()).collect::<Vec<_>>()
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
            "qualification_profile_digest": digest,
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
    let selection_policy = json!({
        "type": "object",
        "additionalProperties": false,
        "required": ["deployment", "revision"],
        "properties": {
            "deployment": exact_deployment("policy_deployment", "pdep"),
            "revision": exact_revision("policy_revision", "prev")
        }
    });
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
                        "items": exact_deployment("skill_deployment", "skdep")
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
