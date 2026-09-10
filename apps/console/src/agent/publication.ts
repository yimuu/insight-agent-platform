import { uploadArtifact, validateArtifactUploadRecovery } from '../artifact/upload.ts'
import type { ArtifactUploadRecovery } from '../artifact/upload.ts'
import type { CompiledAgent } from './compiler.ts'
import { digestJson } from './compiler.ts'
import { PlatformClient } from '../api/client.ts'
import type {
  ArtifactRef,
  AuthorityResponse,
  Json,
  JsonObject,
  PublishedVersionSummary,
  ResourceView,
} from '../api/types.ts'

export type PublicationStage = 'validating' | 'publishing' | 'activating' | 'ready'

export interface PublicationResult {
  agentId: string
  deploymentId: string
  resource: ResourceView
}

interface PublicationHandle {
  schema_version: 4
  attempt_id: string
  gateway_origin: string
  source_bundle_digest: string
  manifest_digest: string
  agent_name: string
  existing_agent_id: string | null
  existing_agent_etag: string | null
  authoring_upload: ArtifactUploadRecovery | null
  plan_upload: ArtifactUploadRecovery | null
  authoring_artifact: ArtifactRef | null
  plan_artifact: ArtifactRef | null
  resource_id: string | null
  resource_etag: string | null
  resource_version: number | null
  resource_draft_generation: number | null
  validation_operation_id: string | null
  validated_resource_etag: string | null
  draft_generation: number | null
  published_versions: PublishedVersionSummary[] | null
  published_resource_etag: string | null
  published_resource_version: number | null
  deployment_id: string | null
  activation_etag: string | null
}

const HANDLE_KEY = 'insight.console.agent-publication.v3'
const MAX_HANDLE_BYTES = 16 * 1024
const encoder = new TextEncoder()

function receipt(attempt: string, phase: string): string {
  return `console-agent-${attempt}-${phase}`
}

function saveHandle(handle: PublicationHandle): void {
  const raw = JSON.stringify(handle)
  validateHandle(JSON.parse(raw))
  if (encoder.encode(raw).byteLength > MAX_HANDLE_BYTES) invalidHandle()
  sessionStorage.setItem(HANDLE_KEY, raw)
}

function invalidHandle(): never {
  throw new Error('publication_conflict: Publication recovery state is invalid; reconcile its existing server operation before another publication')
}

function closed(value: unknown, keys: string[]): Record<string, unknown> {
  if (typeof value !== 'object' || value === null || Array.isArray(value)) invalidHandle()
  const record = value as Record<string, unknown>
  if (Object.keys(record).length !== keys.length || keys.some((key) => !Object.hasOwn(record, key))) invalidHandle()
  return record
}

function bounded(value: unknown, maximum = 512): value is string {
  return typeof value === 'string' && value.length > 0 && encoder.encode(value).byteLength <= maximum
}

function positive(value: unknown): value is number {
  return typeof value === 'number' && Number.isSafeInteger(value) && value > 0
}

function identity(value: unknown, prefix: string): value is string {
  return typeof value === 'string' && new RegExp(`^${prefix}_[0-9a-f]{8}-[0-9a-f]{4}-7[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$`).test(value)
}

function digest(value: unknown): value is string {
  return typeof value === 'string' && /^sha256:[0-9a-f]{64}$/.test(value)
}

function validateArtifact(value: unknown): void {
  const item = closed(value, ['artifact_id', 'content_digest', 'byte_length', 'media_type', 'classification', 'display_name'])
  if (!identity(item.artifact_id, 'art') || !digest(item.content_digest) || !positive(item.byte_length)
    || !bounded(item.media_type) || !['public', 'internal', 'confidential', 'restricted'].includes(String(item.classification))
    || (item.display_name !== null && !bounded(item.display_name))) invalidHandle()
}

function validateHandle(value: unknown): asserts value is PublicationHandle {
  const item = closed(value, [
    'schema_version', 'attempt_id', 'gateway_origin', 'source_bundle_digest', 'manifest_digest', 'agent_name',
    'existing_agent_id', 'existing_agent_etag', 'authoring_upload', 'plan_upload', 'authoring_artifact', 'plan_artifact', 'resource_id', 'resource_etag',
    'resource_version', 'resource_draft_generation',
    'validation_operation_id', 'validated_resource_etag', 'draft_generation', 'published_versions',
    'published_resource_etag', 'published_resource_version', 'deployment_id', 'activation_etag',
  ])
  if (item.schema_version !== 4 || typeof item.attempt_id !== 'string'
    || !/^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/.test(item.attempt_id)
    || !bounded(item.gateway_origin, 2048) || !digest(item.source_bundle_digest) || !digest(item.manifest_digest)
    || !bounded(item.agent_name)) invalidHandle()
  for (const [key, prefix] of [['existing_agent_id', 'agt'], ['resource_id', 'agt'], ['validation_operation_id', 'job'], ['deployment_id', 'adep']]) {
    if (item[key] !== null && !identity(item[key], prefix)) invalidHandle()
  }
  for (const key of ['existing_agent_etag', 'resource_etag', 'validated_resource_etag', 'published_resource_etag', 'activation_etag']) {
    if (item[key] !== null && !bounded(item[key], 256)) invalidHandle()
  }
  for (const key of ['draft_generation', 'published_resource_version', 'resource_version', 'resource_draft_generation']) {
    if (item[key] !== null && !positive(item[key])) invalidHandle()
  }
  for (const key of ['authoring_artifact', 'plan_artifact']) {
    if (item[key] !== null) validateArtifact(item[key])
  }
  for (const key of ['authoring_upload', 'plan_upload']) if (item[key] !== null) validateArtifactUploadRecovery(item[key] as ArtifactUploadRecovery)
  if (item.published_versions !== null) {
    if (!Array.isArray(item.published_versions) || item.published_versions.length !== 2) invalidHandle()
    for (const entry of item.published_versions) {
      const version = closed(entry, ['resource_version_id', 'revision_no', 'content_digest', 'artifact_id', 'etag'])
      if ((!identity(version.resource_version_id, 'aif') && !identity(version.resource_version_id, 'arev'))
        || !positive(version.revision_no) || !digest(version.content_digest)
        || !identity(version.artifact_id, 'art') || !bounded(version.etag, 256)) invalidHandle()
    }
  }
  const present = (key: string) => item[key] !== null
  if (present('existing_agent_id') !== present('existing_agent_etag')
    || present('resource_id') !== present('resource_etag')
    || present('resource_id') !== present('resource_version')
    || present('resource_id') !== present('resource_draft_generation')
    || present('validated_resource_etag') !== present('draft_generation')
    || present('published_versions') !== present('published_resource_etag')
    || present('published_versions') !== present('published_resource_version')
    || (present('authoring_artifact') && !present('authoring_upload'))
    || (present('plan_artifact') && !present('plan_upload'))
    || (present('plan_upload') && !present('authoring_artifact'))
    || (present('plan_artifact') && !present('authoring_artifact'))
    || (present('resource_id') && !present('plan_artifact'))
    || (present('validation_operation_id') && !present('resource_id'))
    || (present('validated_resource_etag') && !present('validation_operation_id'))
    || (present('published_versions') && !present('validated_resource_etag'))
    || (present('deployment_id') && !present('published_versions'))
    || (present('activation_etag') && !present('deployment_id'))
    || (present('existing_agent_id') && present('resource_id') && item.existing_agent_id !== item.resource_id)) invalidHandle()
}

function loadHandle(client: PlatformClient, compiled: CompiledAgent, existing: ResourceView | null): PublicationHandle {
  const existingAgentId = existing?.resource_id ?? null
  const raw = sessionStorage.getItem(HANDLE_KEY)
  if (raw) {
    if (encoder.encode(raw).byteLength > MAX_HANDLE_BYTES) invalidHandle()
    const value: unknown = JSON.parse(raw)
    validateHandle(value)
    if (
      value.gateway_origin !== client.origin ||
      value.source_bundle_digest !== compiled.sourceBundleDigest ||
      value.manifest_digest !== compiled.manifestDigest ||
      value.agent_name !== compiled.name ||
      value.existing_agent_id !== existingAgentId
    ) {
      throw new Error('publication_conflict: A different Agent publication is awaiting recovery')
    }
    if ((value.authoring_artifact && value.authoring_artifact.content_digest !== compiled.sourceBundleDigest)
      || (value.plan_artifact && value.plan_artifact.content_digest !== compiled.typedPlanDigest)) invalidHandle()
    return value
  }
  const handle: PublicationHandle = {
    schema_version: 4,
    attempt_id: crypto.randomUUID(),
    gateway_origin: client.origin,
    source_bundle_digest: compiled.sourceBundleDigest,
    manifest_digest: compiled.manifestDigest,
    agent_name: compiled.name,
    existing_agent_id: existingAgentId,
    existing_agent_etag: existing?.etag ?? null,
    authoring_upload: null,
    plan_upload: null,
    authoring_artifact: null,
    plan_artifact: null,
    resource_id: null,
    resource_etag: null,
    resource_version: null,
    resource_draft_generation: null,
    validation_operation_id: null,
    validated_resource_etag: null,
    draft_generation: null,
    published_versions: null,
    published_resource_etag: null,
    published_resource_version: null,
    deployment_id: null,
    activation_etag: null,
  }
  saveHandle(handle)
  return handle
}


function object(value: unknown, label: string): JsonObject {
  if (typeof value !== 'object' || value === null || Array.isArray(value)) {
    throw new Error(`agent_compile_failed: ${label} is not an object`)
  }
  return value as JsonObject
}

export function materializeDocument(
  compiled: CompiledAgent,
  authoring: ArtifactRef,
  plan: ArtifactRef,
): JsonObject {
  const intent = object(compiled.resourceIntent, 'resource intent')
  return {
    resource_kind: 'agent',
    spec: {
      authoring_name: intent.authoring_name,
      required_features: intent.required_features,
      input_classification: intent.input_classification,
      default_deadline_seconds: intent.default_deadline_seconds,
      authoring_package: {
        artifact: authoring,
        manifest_digest: compiled.manifestDigest,
      },
      contract_digest: intent.contract_digest,
      dependency_versions: intent.dependency_versions,
      policy_versions: intent.policy_versions,
      author_instructions: intent.author_instructions,
      input_schema: intent.input_schema,
      output_schema: intent.output_schema,
      error_schema: intent.error_schema,
      typed_plan_artifact_id: plan.artifact_id,
      typed_plan_digest: compiled.typedPlanDigest,
    },
  }
}

function exactVersion(
  versions: PublishedVersionSummary[],
  prefix: 'aif_' | 'arev_',
  expectedDigest: string,
): JsonObject {
  const version = versions.find((candidate) => candidate.resource_version_id.startsWith(prefix))
  if (!version || version.content_digest !== expectedDigest || !positive(version.revision_no)) {
    throw new Error('publish_invalid: Published Agent revision closure does not match the compiler')
  }
  return {
    revision_id: version.resource_version_id,
    resource_kind: prefix === 'aif_' ? 'agent_interface_revision' : 'agent_plan_revision',
    semantic_digest: version.content_digest,
  }
}

function requireResource(resource: ResourceView, expectedId: string | null): void {
  if (resource.schema_version !== 1 || resource.resource_kind !== 'agent' || !identity(resource.resource_id, 'agt')
    || (expectedId !== null && resource.resource_id !== expectedId) || !positive(resource.version)
    || !bounded(resource.etag, 256) || resource.lifecycle_state !== 'active') {
    throw new Error('publication_conflict: Agent response does not match the publication authority')
  }
}

function requireResponseEtag(response: AuthorityResponse<{ etag: string }>): void {
  if (!bounded(response.data.etag, 256) || response.etag !== response.data.etag) {
    throw new Error('publication_conflict: Response ETag differs from its authority body')
  }
}

function requireActive(resource: ResourceView, handle: PublicationHandle): void {
  requireResource(resource, handle.resource_id)
  if (resource.gate_state !== 'enabled' || resource.active_deployment_id !== handle.deployment_id) {
    throw new Error('publication_conflict: The current active deployment differs from this publication')
  }
}

export async function publishCompiledAgent(
  client: PlatformClient,
  compiled: CompiledAgent,
  existing: ResourceView | null,
  onStage: (stage: PublicationStage) => void,
): Promise<PublicationResult> {
  const handle = loadHandle(client, compiled, existing)
  const intent = object(compiled.resourceIntent, 'resource intent')
  onStage('validating')

  handle.authoring_artifact = await uploadArtifact(client, encoder.encode(compiled.sourceBundle),
    object(intent.authoring_artifact, 'authoring artifact intent'), receipt(handle.attempt_id, 'authoring-prepare'), receipt(handle.attempt_id, 'authoring-complete'),
    handle.authoring_upload, state => { handle.authoring_upload = state; saveHandle(handle) })
  saveHandle(handle)
  handle.plan_artifact = await uploadArtifact(client, encoder.encode(compiled.typedPlan),
    object(intent.typed_plan_artifact, 'Typed Plan artifact intent'), receipt(handle.attempt_id, 'plan-prepare'), receipt(handle.attempt_id, 'plan-complete'),
    handle.plan_upload, state => { handle.plan_upload = state; saveHandle(handle) })
  saveHandle(handle)
  const draft = {
    display_name: intent.display_name,
    document: materializeDocument(compiled, handle.authoring_artifact, handle.plan_artifact),
  }
  if (!handle.resource_id || !handle.resource_etag) {
    const resource = handle.existing_agent_id && handle.existing_agent_etag
      ? await client.updateAgent(handle.existing_agent_id, draft, handle.existing_agent_etag, receipt(handle.attempt_id, 'update'))
      : await client.createAgent(draft, receipt(handle.attempt_id, 'create'))
    requireResponseEtag(resource)
    requireResource(resource.data, handle.existing_agent_id)
    handle.resource_id = resource.data.resource_id
    handle.resource_etag = resource.data.etag
    handle.resource_version = resource.data.version
    handle.resource_draft_generation = resource.data.draft_generation
    saveHandle(handle)
  }
  if (!handle.validation_operation_id) {
    const validation = await client.validateAgent(
      handle.resource_id,
      handle.resource_etag,
      receipt(handle.attempt_id, 'validate'),
    )
    handle.validation_operation_id = validation.data.operation_id
    saveHandle(handle)
  }
  const validation = await client.waitOperation(handle.validation_operation_id)
  if (validation.data.state !== 'succeeded') {
    throw new Error(`agent_validation_failed: ${validation.data.error?.code ?? validation.data.state}`)
  }
  if (!handle.validated_resource_etag || !handle.draft_generation) {
    const validated = await client.getResource('agents', handle.resource_id)
    requireResponseEtag(validated)
    requireResource(validated.data, handle.resource_id)
    if (validated.data.draft.validation === null) {
      throw new Error('agent_validation_failed: Validation succeeded without a validation summary')
    }
    if (handle.resource_version === null || validated.data.version !== handle.resource_version + 1
      || validated.data.draft_generation !== handle.resource_draft_generation
      || validated.data.draft.display_name !== draft.display_name
      || await digestJson(validated.data.draft.document as Json) !== await digestJson(draft.document as Json)) {
      throw new Error('publication_conflict: Agent draft changed during this validation')
    }
    handle.validated_resource_etag = validated.data.etag
    handle.draft_generation = validated.data.draft_generation
    saveHandle(handle)
  }

  onStage('publishing')
  if (!handle.published_versions || !handle.published_resource_etag) {
    const published = await client.publishAgent(handle.resource_id, {
      kind: 'agent',
      revision_no: handle.draft_generation,
      interface_content_digest: intent.contract_digest,
      plan_content_digest: compiled.typedPlanDigest,
      artifact_id: handle.plan_artifact.artifact_id,
    }, handle.validated_resource_etag, receipt(handle.attempt_id, 'publish'))
    requireResponseEtag(published)
    if (published.data.schema_version !== 1 || published.data.resource_id !== handle.resource_id
      || published.data.resource_kind !== 'agent' || published.data.draft_generation !== handle.draft_generation
      || !positive(published.data.version)) {
      throw new Error('publish_invalid: Published response does not match this Agent draft')
    }
    handle.published_versions = published.data.published_versions
    handle.published_resource_etag = published.data.etag
    handle.published_resource_version = published.data.version
    saveHandle(handle)
  }
  if (handle.published_versions.some((version) => version.artifact_id !== handle.plan_artifact?.artifact_id)) {
    throw new Error('publish_invalid: Published versions must bind this Typed Plan Artifact')
  }
  const interfaceRevision = exactVersion(handle.published_versions, 'aif_', String(intent.contract_digest))
  const planRevision = exactVersion(handle.published_versions, 'arev_', compiled.typedPlanDigest)
  const deploymentIntent = object(compiled.deploymentIntent, 'deployment intent')
  if (!handle.deployment_id) {
    const deployment = await client.createAgentDeployment(handle.resource_id, {
      resource_version_id: planRevision.revision_id,
      environment: deploymentIntent.environment,
      closure: {
        resource_kind: 'agent',
        bindings: {
          interface: interfaceRevision,
          plan: planRevision,
          entry_node_id: deploymentIntent.entry_node_id,
          entry_node_kind: deploymentIntent.entry_node_kind,
          slots: deploymentIntent.slots,
          policies: deploymentIntent.policies,
          execution_profile: deploymentIntent.execution_profile,
        },
      },
    }, handle.published_resource_etag, receipt(handle.attempt_id, 'deploy'))
    requireResponseEtag(deployment)
    if (deployment.data.schema_version !== 1 || deployment.data.resource_id !== handle.resource_id
      || deployment.data.resource_kind !== 'agent' || deployment.data.resource_version_id !== planRevision.revision_id
      || deployment.data.environment !== deploymentIntent.environment) {
      throw new Error('publication_conflict: Deployment response does not match this publication')
    }
    handle.deployment_id = deployment.data.deployment_id
    saveHandle(handle)
  }

  onStage('activating')
  if (!handle.activation_etag) {
    const beforeActivation = await client.getResource('agents', handle.resource_id)
    requireResponseEtag(beforeActivation)
    requireResource(beforeActivation.data, handle.resource_id)
    if (handle.published_resource_version === null || beforeActivation.data.version !== handle.published_resource_version + 1) {
      throw new Error('publication_conflict: Agent changed after this deployment was created')
    }
    handle.activation_etag = beforeActivation.data.etag
    saveHandle(handle)
  }
  const activated = await client.activateAgentDeployment(
    handle.resource_id,
    handle.deployment_id,
    handle.activation_etag,
    receipt(handle.attempt_id, 'activate'),
  )
  requireResponseEtag(activated)
  requireActive(activated.data, handle)
  const current = await client.getResource('agents', handle.resource_id)
  requireResponseEtag(current)
  requireActive(current.data, handle)
  sessionStorage.removeItem(HANDLE_KEY)
  onStage('ready')
  return { agentId: handle.resource_id, deploymentId: handle.deployment_id, resource: current.data }
}
