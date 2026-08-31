import type { CompiledAgent } from './compiler.ts'
import { PlatformClient } from '../api/client.ts'
import type {
  ArtifactRef,
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
  schema_version: 1
  manifest_digest: string
  agent_name: string
  existing_agent_id: string | null
  authoring_artifact: ArtifactRef | null
  plan_artifact: ArtifactRef | null
  resource_id: string | null
  resource_etag: string | null
  validation_operation_id: string | null
  validated_resource_etag: string | null
  draft_generation: number | null
  published_versions: PublishedVersionSummary[] | null
  published_resource_etag: string | null
  deployment_id: string | null
}

const HANDLE_KEY = 'insight.console.agent-publication.v1'
const encoder = new TextEncoder()

function receipt(digest: string, phase: string): string {
  const suffix = digest.startsWith('sha256:') ? digest.slice(7) : digest
  return `console-agent-${suffix}-${phase}`
}

function saveHandle(handle: PublicationHandle): void {
  sessionStorage.setItem(HANDLE_KEY, JSON.stringify(handle))
}

function loadHandle(compiled: CompiledAgent, existingAgentId: string | null): PublicationHandle {
  const raw = sessionStorage.getItem(HANDLE_KEY)
  if (raw) {
    const value = JSON.parse(raw) as PublicationHandle
    if (
      value.schema_version !== 1 ||
      value.manifest_digest !== compiled.manifestDigest ||
      value.agent_name !== compiled.name ||
      value.existing_agent_id !== existingAgentId
    ) {
      throw new Error('publication_conflict: A different Agent publication is awaiting recovery')
    }
    return value
  }
  const handle: PublicationHandle = {
    schema_version: 1,
    manifest_digest: compiled.manifestDigest,
    agent_name: compiled.name,
    existing_agent_id: existingAgentId,
    authoring_artifact: null,
    plan_artifact: null,
    resource_id: null,
    resource_etag: null,
    validation_operation_id: null,
    validated_resource_etag: null,
    draft_generation: null,
    published_versions: null,
    published_resource_etag: null,
    deployment_id: null,
  }
  saveHandle(handle)
  return handle
}

async function upload(
  client: PlatformClient,
  bytes: Uint8Array,
  intent: JsonObject,
  manifestDigest: string,
  phase: string,
): Promise<ArtifactRef> {
  const prepared = await client.prepareArtifactUpload({
    schema_version: 1,
    purpose: intent.purpose,
    classification: intent.classification,
    expected_size_bytes: bytes.byteLength,
    expected_digest: intent.content_digest,
    declared_media_type: intent.media_type,
    display_name: intent.display_name ?? null,
  }, receipt(manifestDigest, `${phase}-prepare`))
  await client.putArtifactObject(prepared.data.upload_target.url, bytes, String(intent.media_type))
  await client.completeArtifactUpload(prepared.data.artifact_id, {
    schema_version: 1,
    completion_proof: prepared.data.upload_target.completion_proof,
  }, prepared.data.artifact_etag, receipt(manifestDigest, `${phase}-complete`))
  const operation = await client.waitOperation(prepared.data.operation_id)
  if (operation.data.state !== 'succeeded') {
    throw new Error(`artifact_verification_failed: ${operation.data.error?.code ?? operation.data.state}`)
  }
  const artifact = await client.getArtifact(prepared.data.artifact_id)
  if (artifact.data.state !== 'ready' || artifact.data.content === null) {
    throw new Error('artifact_not_ready: Verification completed without Ready content authority')
  }
  return artifact.data.content
}

function object(value: unknown, label: string): JsonObject {
  if (typeof value !== 'object' || value === null || Array.isArray(value)) {
    throw new Error(`agent_compile_failed: ${label} is not an object`)
  }
  return value as JsonObject
}

function materializeDocument(
  compiled: CompiledAgent,
  authoring: ArtifactRef,
  plan: ArtifactRef,
): JsonObject {
  const intent = object(compiled.resourceIntent, 'resource intent')
  const authoringIntent = object(intent.authoring_artifact, 'authoring artifact intent')
  return {
    resource_kind: 'agent',
    spec: {
      authoring_name: intent.authoring_name,
      required_features: intent.required_features,
      input_classification: intent.input_classification,
      default_deadline_seconds: intent.default_deadline_seconds,
      authoring_package: {
        artifact: authoring,
        manifest_digest: authoringIntent.content_digest,
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
  if (!version || version.content_digest !== expectedDigest) {
    throw new Error('publish_invalid: Published Agent revision closure does not match the compiler')
  }
  return {
    revision_id: version.resource_version_id,
    resource_kind: prefix === 'aif_' ? 'agent_interface_revision' : 'agent_plan_revision',
    semantic_digest: version.content_digest,
  }
}

export async function publishCompiledAgent(
  client: PlatformClient,
  compiled: CompiledAgent,
  existing: ResourceView | null,
  onStage: (stage: PublicationStage) => void,
): Promise<PublicationResult> {
  const handle = loadHandle(compiled, existing?.resource_id ?? null)
  const intent = object(compiled.resourceIntent, 'resource intent')
  onStage('validating')

  if (!handle.authoring_artifact) {
    handle.authoring_artifact = await upload(
      client,
      encoder.encode(compiled.canonicalManifest),
      object(intent.authoring_artifact, 'authoring artifact intent'),
      compiled.manifestDigest,
      'authoring',
    )
    saveHandle(handle)
  }
  if (!handle.plan_artifact) {
    handle.plan_artifact = await upload(
      client,
      encoder.encode(compiled.typedPlan),
      object(intent.typed_plan_artifact, 'Typed Plan artifact intent'),
      compiled.manifestDigest,
      'plan',
    )
    saveHandle(handle)
  }
  const draft = {
    display_name: intent.display_name,
    document: materializeDocument(compiled, handle.authoring_artifact, handle.plan_artifact),
  }
  if (!handle.resource_id || !handle.resource_etag) {
    const resource = existing
      ? await client.updateAgent(existing.resource_id, draft, existing.etag, receipt(compiled.manifestDigest, 'update'))
      : await client.createAgent(draft, receipt(compiled.manifestDigest, 'create'))
    handle.resource_id = resource.data.resource_id
    handle.resource_etag = resource.data.etag
    saveHandle(handle)
  }
  if (!handle.validation_operation_id) {
    const validation = await client.validateAgent(
      handle.resource_id,
      handle.resource_etag,
      receipt(compiled.manifestDigest, 'validate'),
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
    if (validated.data.draft.validation === null) {
      throw new Error('agent_validation_failed: Validation succeeded without a validation summary')
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
      artifact_id: handle.authoring_artifact.artifact_id,
    }, handle.validated_resource_etag, receipt(compiled.manifestDigest, 'publish'))
    handle.published_versions = published.data.published_versions
    handle.published_resource_etag = published.data.etag
    saveHandle(handle)
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
    }, handle.published_resource_etag, receipt(compiled.manifestDigest, 'deploy'))
    handle.deployment_id = deployment.data.deployment_id
    saveHandle(handle)
  }

  onStage('activating')
  const beforeActivation = await client.getResource('agents', handle.resource_id)
  const activated = await client.activateAgentDeployment(
    handle.resource_id,
    handle.deployment_id,
    beforeActivation.data.etag,
    receipt(compiled.manifestDigest, 'activate'),
  )
  if (activated.data.gate_state !== 'enabled') {
    throw new Error('agent_activation_failed: Agent did not become Ready')
  }
  sessionStorage.removeItem(HANDLE_KEY)
  onStage('ready')
  return { agentId: handle.resource_id, deploymentId: handle.deployment_id, resource: activated.data }
}

export function clearPublicationRecovery(): void {
  sessionStorage.removeItem(HANDLE_KEY)
}
