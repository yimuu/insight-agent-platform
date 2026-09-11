import {
  applyQuotaIntent,
  createQuotaIntent,
  readModelQuota,
  validQuotaLimits,
  validateQuotaIntent,
} from './quota.ts'
import type { ModelQuotaIntent } from './quota.ts'
import { uploadArtifact, validateArtifactUploadRecovery } from '../../shared/artifact/upload.ts'
import type { ArtifactUploadRecovery } from '../../shared/artifact/upload.ts'
import { id, positive, sha, uuid4, closed } from './validation.ts'
import { digestJson } from '../../shared/compiler/compiler.ts'
import type { PlatformClient } from '../../shared/api/client.ts'
import type {
  ArtifactRef,
  AuthorityResponse,
  DeploymentView,
  Json,
  JsonObject,
  PublishedVersionSummary,
  ResourceView,
} from '../../shared/api/types.ts'
import type {
  ModelConfigurationInput,
  ModelDeclaration,
  ModelPublicationRequest,
  ModelPublicationResult,
  ModelResourceNoun,
  ModelQuotaLimits,
} from '../../shared/api/model-types.ts'

const KEY = 'insight.console.model-publication.v1'
const encoder = new TextEncoder()
interface Handle {
  schema_version: 1
  attempt: string
  origin: string
  tenant_id: string
  input_digest: string
  input: ModelConfigurationInput
  quota: ModelQuotaLimits | null
  quota_intent: ModelQuotaIntent | null
  existing: { id: string; etag: string; version: number; draft_generation: number } | null
  artifact: ArtifactRef | null
  upload: ArtifactUploadRecovery | null
  resource: { id: string; etag: string; version: number; draft_generation: number } | null
  operation_id: string | null
  validated: { etag: string; draft_generation: number } | null
  published: { etag: string; version: number; revision: PublishedVersionSummary } | null
  deployment: DeploymentView | null
  activation_etag: string | null
}
function conflict(): never {
  throw new Error(
    'model_configuration_conflict: Resume the original model operation and reconcile current server state before changing its inputs.',
  )
}
function canonical(value: Json): string {
  if (Array.isArray(value)) return `[${value.map(canonical).join(',')}]`
  if (value !== null && typeof value === 'object')
    return `{${Object.keys(value)
      .sort()
      .map((key) => `${JSON.stringify(key)}:${canonical(value[key])}`)
      .join(',')}}`
  const encoded = JSON.stringify(value)
  if (encoded === undefined || (typeof value === 'number' && !Number.isFinite(value))) conflict()
  return encoded
}
export async function declarationBytes(declaration: ModelDeclaration): Promise<Uint8Array> {
  const bytes = encoder.encode(canonical(declaration.content))
  if (
    declaration.schema_version !== 1 ||
    bytes.length === 0 ||
    bytes.length > 65_536 ||
    bytes.length !== declaration.size_bytes
  )
    conflict()
  const digest = `sha256:${Array.from(new Uint8Array(await crypto.subtle.digest('SHA-256', bytes)), (byte) => byte.toString(16).padStart(2, '0')).join('')}`
  if (digest !== declaration.content_digest || (await digestJson(declaration.content)) !== digest)
    conflict()
  return bytes
}
function validate(handle: Handle): void {
  const source = handle.input?.kind === 'source'
  const prefix = source ? 'mpr' : 'mdl'
  if (
    !closed(handle, [
      'schema_version',
      'attempt',
      'origin',
      'tenant_id',
      'input_digest',
      'input',
      'quota',
      'quota_intent',
      'existing',
      'artifact',
      'upload',
      'resource',
      'operation_id',
      'validated',
      'published',
      'deployment',
      'activation_etag',
    ]) ||
    handle.schema_version !== 1 ||
    !uuid4(handle.attempt) ||
    !id(handle.tenant_id, 'ten') ||
    !sha(handle.input_digest) ||
    typeof handle.origin !== 'string' ||
    !['source', 'model'].includes(handle.input?.kind)
  )
    conflict()
  if (
    source ? handle.quota !== null || handle.quota_intent !== null : !validQuotaLimits(handle.quota)
  )
    conflict()
  if (handle.quota_intent !== null) {
    validateQuotaIntent(handle.quota_intent)
    if (
      !handle.deployment ||
      handle.quota_intent.tenant_id !== handle.tenant_id ||
      handle.quota_intent.origin !== handle.origin ||
      handle.quota_intent.model_deployment.deployment_id !== handle.deployment.deployment_id ||
      handle.quota_intent.model_deployment.deployment_digest !== handle.deployment.closure_digest ||
      canonical(handle.quota_intent.limits as unknown as Json) !==
        canonical(handle.quota as unknown as Json)
    )
      conflict()
  }
  for (const resource of [handle.existing, handle.resource])
    if (
      resource !== null &&
      (!closed(resource, ['id', 'etag', 'version', 'draft_generation']) ||
        !id(resource.id, prefix) ||
        !positive(resource.version) ||
        !positive(resource.draft_generation) ||
        resource.etag !== `"${resource.id}-${resource.version}"`)
    )
      conflict()
  if (handle.upload !== null) validateArtifactUploadRecovery(handle.upload)
  if (
    handle.artifact &&
    (!id(handle.artifact.artifact_id, 'art') ||
      !sha(handle.artifact.content_digest) ||
      !positive(handle.artifact.byte_length) ||
      handle.artifact.media_type !== 'application/json' ||
      handle.artifact.classification !== 'internal')
  )
    conflict()
  if (handle.operation_id !== null && !id(handle.operation_id, 'job')) conflict()
  if (
    (handle.operation_id && !handle.resource) ||
    (handle.validated && !handle.operation_id) ||
    (handle.published && !handle.validated) ||
    (handle.deployment && !handle.published) ||
    (handle.activation_etag && !handle.deployment)
  )
    conflict()
  if (
    handle.existing &&
    handle.resource &&
    (handle.existing.id !== handle.resource.id ||
      handle.resource.version !== handle.existing.version + 1 ||
      handle.resource.draft_generation !== handle.existing.draft_generation + 1)
  )
    conflict()
  if (
    !handle.existing &&
    handle.resource &&
    (handle.resource.version !== 1 || handle.resource.draft_generation !== 1)
  )
    conflict()
  if (
    (handle.artifact &&
      (!handle.upload || handle.artifact.artifact_id !== handle.upload.artifact_id)) ||
    (handle.resource && !handle.artifact)
  )
    conflict()
  if (
    handle.validated &&
    (!closed(handle.validated, ['etag', 'draft_generation']) ||
      handle.validated.draft_generation !== handle.resource!.draft_generation ||
      handle.validated.etag !== `"${handle.resource!.id}-${handle.resource!.version + 1}"`)
  )
    conflict()
  if (handle.published) {
    const published = handle.published
    const revision = published.revision
    if (
      !closed(published, ['etag', 'version', 'revision']) ||
      published.version !== handle.resource!.version + 2 ||
      published.etag !== `"${handle.resource!.id}-${published.version}"` ||
      !revision ||
      !id(revision.resource_version_id, source ? 'mprev' : 'mdrev') ||
      revision.revision_no !== handle.resource!.draft_generation ||
      !sha(revision.content_digest) ||
      revision.artifact_id !== handle.artifact!.artifact_id
    )
      conflict()
  }
  if (handle.deployment) {
    const deployment = handle.deployment
    if (
      deployment.schema_version !== 1 ||
      deployment.resource_id !== handle.resource!.id ||
      deployment.resource_kind !== (source ? 'model_provider' : 'model_profile') ||
      !id(deployment.deployment_id, source ? 'mpdep' : 'mdep') ||
      deployment.resource_version_id !== handle.published!.revision.resource_version_id ||
      !sha(deployment.closure_digest) ||
      deployment.etag !== `"${deployment.deployment_id}-${deployment.closure_digest.slice(7)}"`
    )
      conflict()
  }
  if (
    handle.activation_etag &&
    handle.activation_etag !== `"${handle.resource!.id}-${handle.published!.version + 1}"`
  )
    conflict()
}
function save(handle: Handle): void {
  validate(handle)
  const raw = JSON.stringify(handle)
  if (encoder.encode(raw).length > 131_072) conflict()
  sessionStorage.setItem(KEY, raw)
}
export function hasPendingModelPublication(): boolean {
  return sessionStorage.getItem(KEY) !== null
}
export function pendingModelInput(): ModelConfigurationInput | null {
  const raw = sessionStorage.getItem(KEY)
  if (!raw) return null
  if (encoder.encode(raw).length > 131_072) conflict()
  const handle = JSON.parse(raw) as Handle
  validate(handle)
  return handle.input
}
export async function resumeModelPublication(
  client: PlatformClient,
  tenantId: string,
  installationDigest: string,
  progress: (stage: string) => void,
): Promise<ModelPublicationResult> {
  const raw = sessionStorage.getItem(KEY)
  if (!raw || encoder.encode(raw).length > 131_072) conflict()
  const handle = JSON.parse(raw) as Handle
  validate(handle)
  if (handle.origin !== client.origin || handle.tenant_id !== tenantId) conflict()
  return publishModelConfiguration(
    client,
    {
      tenant_id: tenantId,
      installation_digest: installationDigest,
      input: handle.input,
      quota: handle.quota,
      existing: handle.existing
        ? {
            resource_id: handle.existing.id,
            etag: handle.existing.etag,
            version: handle.existing.version,
            draft_generation: handle.existing.draft_generation,
          }
        : null,
    },
    progress,
  )
}
async function semantic(request: ModelPublicationRequest): Promise<string> {
  const input = structuredClone(request.input)
  // The first declaration time is frozen in the handle; pressing Resume is not a new declaration.
  if (input.kind === 'model') input.configuration.declared_at = ''
  return digestJson({
    installation_digest: request.installation_digest,
    input: input as unknown as Json,
    quota: request.quota as unknown as Json,
    existing: request.existing
      ? {
          resource_id: request.existing.resource_id,
          etag: request.existing.etag,
          version: request.existing.version,
          draft_generation: request.existing.draft_generation,
        }
      : null,
    tenant_id: request.tenant_id,
  })
}
async function load(client: PlatformClient, request: ModelPublicationRequest): Promise<Handle> {
  const inputDigest = await semantic(request)
  const raw = sessionStorage.getItem(KEY)
  if (raw) {
    if (encoder.encode(raw).length > 131_072) conflict()
    const handle = JSON.parse(raw) as Handle
    validate(handle)
    if (
      handle.origin !== client.origin ||
      handle.input_digest !== inputDigest ||
      handle.tenant_id !== request.tenant_id
    )
      conflict()
    // Recompute the stored semantic identity, rather than trusting a local success flag/digest.
    if (
      (await semantic({
        ...request,
        input: handle.input,
        quota: handle.quota,
        existing: handle.existing
          ? {
              resource_id: handle.existing.id,
              etag: handle.existing.etag,
              version: handle.existing.version,
              draft_generation: handle.existing.draft_generation,
            }
          : null,
      })) !== handle.input_digest
    )
      conflict()
    return handle
  }
  const existing = request.existing
  const handle: Handle = {
    schema_version: 1,
    attempt: crypto.randomUUID(),
    origin: client.origin,
    tenant_id: request.tenant_id,
    input_digest: inputDigest,
    input: structuredClone(request.input),
    quota: structuredClone(request.quota),
    quota_intent: null,
    existing: existing
      ? {
          id: existing.resource_id,
          etag: existing.etag,
          version: existing.version,
          draft_generation: existing.draft_generation,
        }
      : null,
    artifact: null,
    upload: null,
    resource: null,
    operation_id: null,
    validated: null,
    published: null,
    deployment: null,
    activation_etag: null,
  }
  save(handle)
  return handle
}
function requireEtag<T extends { etag: string }>(response: AuthorityResponse<T>): void {
  if (response.etag !== response.data.etag) conflict()
}
function requireResource(response: AuthorityResponse<ResourceView>, handle: Handle): void {
  requireEtag(response)
  const item = response.data
  if (
    item.schema_version !== 1 ||
    item.resource_kind !== (handle.input.kind === 'source' ? 'model_provider' : 'model_profile') ||
    item.draft.alias !== handle.input.configuration.alias ||
    item.lifecycle_state !== 'active' ||
    !positive(item.version) ||
    item.etag !== `"${item.resource_id}-${item.version}"` ||
    (handle.resource && item.resource_id !== handle.resource.id) ||
    (handle.existing && item.resource_id !== handle.existing.id)
  )
    conflict()
}
async function upload(
  client: PlatformClient,
  handle: Handle,
  declaration: ModelDeclaration,
  receipt: (step: string) => string,
): Promise<ArtifactRef> {
  const bytes = await declarationBytes(declaration)
  const artifact = await uploadArtifact(
    client,
    bytes,
    {
      purpose: 'authoring_document',
      classification: 'internal',
      content_digest: declaration.content_digest,
      media_type: 'application/json',
      display_name: null,
    },
    receipt('upload-prepare'),
    receipt('upload-complete'),
    handle.upload,
    (state) => {
      handle.upload = state
      save(handle)
    },
  )
  if (
    handle.artifact &&
    (await digestJson(handle.artifact as unknown as Json)) !==
      (await digestJson(artifact as unknown as Json))
  )
    conflict()
  handle.artifact = artifact
  save(handle)
  return artifact
}
export async function publishModelConfiguration(
  client: PlatformClient,
  request: ModelPublicationRequest,
  progress: (stage: string) => void,
): Promise<ModelPublicationResult> {
  const handle = await load(client, request)
  const noun: ModelResourceNoun = handle.input.kind === 'source' ? 'model-providers' : 'models'
  const kind = handle.input.kind === 'source' ? 'model_provider' : 'model_profile'
  const receipt = (step: string) => `console-model-${handle.attempt}-${step}`
  progress('正在核验配置')
  const declaration = (
    await client.declareModelConfiguration(handle.input, request.installation_digest)
  ).data
  const artifact = await upload(client, handle, declaration, receipt)
  const compiled = (
    await client.compileModelConfiguration(handle.input, request.installation_digest, artifact)
  ).data
  if (
    compiled.environment.length === 0 ||
    compiled.draft.display_name !== handle.input.configuration.display_name ||
    compiled.schema_version !== 1 ||
    compiled.draft.alias !== handle.input.configuration.alias ||
    compiled.deployment.resource_kind !== kind ||
    (await digestJson(compiled.declaration as unknown as Json)) !==
      (await digestJson(declaration as unknown as Json))
  )
    conflict()
  const draft = {
    alias: compiled.draft.alias,
    display_name: compiled.draft.display_name,
    document: compiled.draft.document,
  }
  progress('正在校验来源配置')
  if (!handle.resource) {
    const response = handle.existing
      ? await client.updateModelResource(
          noun,
          handle.existing.id,
          draft,
          handle.existing.etag,
          receipt('update'),
        )
      : await client.createModelResource(noun, draft, receipt('create'))
    requireResource(response, handle)
    if (response.data.version !== (handle.existing ? handle.existing.version + 1 : 1)) conflict()
    handle.resource = {
      id: response.data.resource_id,
      etag: response.data.etag,
      version: response.data.version,
      draft_generation: response.data.draft_generation,
    }
    save(handle)
  }
  const resource = handle.resource
  if (!handle.operation_id) {
    handle.operation_id = (
      await client.validateModelResource(noun, resource.id, resource.etag, receipt('validate'))
    ).data.operation_id
    save(handle)
  }
  const operation = await client.waitOperation(handle.operation_id)
  if (operation.data.state !== 'succeeded')
    throw new Error(
      'model_validation_failed: Resource validation did not succeed; inspect its operation before another attempt.',
    )
  if (!handle.validated) {
    const current = await client.getResource(noun, resource.id)
    requireResource(current, handle)
    if (
      current.data.version !== resource.version + 1 ||
      current.data.draft_generation !== resource.draft_generation ||
      !current.data.draft.validation ||
      current.data.draft.display_name !== draft.display_name ||
      (await digestJson(current.data.draft.document as Json)) !==
        (await digestJson(draft.document as Json))
    )
      conflict()
    handle.validated = { etag: current.data.etag, draft_generation: current.data.draft_generation }
    save(handle)
  }
  progress('正在发布模型配置')
  if (!handle.published) {
    const published = await client.publishModelResource(
      noun,
      resource.id,
      {
        kind: 'single',
        revision_no: handle.validated.draft_generation,
        content_digest: declaration.content_digest,
        artifact_id: artifact.artifact_id,
      },
      handle.validated.etag,
      receipt('publish'),
    )
    requireEtag(published)
    const revision = published.data.published_versions[0]
    if (
      published.data.resource_id !== resource.id ||
      published.data.resource_kind !== kind ||
      published.data.published_versions.length !== 1 ||
      published.data.version !== resource.version + 2 ||
      !revision ||
      !id(revision.resource_version_id, kind === 'model_provider' ? 'mprev' : 'mdrev') ||
      revision.content_digest !== declaration.content_digest ||
      revision.artifact_id !== artifact.artifact_id ||
      revision.revision_no !== handle.validated.draft_generation
    )
      conflict()
    handle.published = { etag: published.data.etag, version: published.data.version, revision }
    save(handle)
  }
  const published = handle.published
  const revision = {
    revision_id: published.revision.resource_version_id,
    resource_kind: kind === 'model_provider' ? 'model_provider_revision' : 'model_profile_revision',
    semantic_digest: published.revision.content_digest,
  }
  const bindings: JsonObject = {
    ...compiled.deployment.bindings,
    [kind === 'model_provider' ? 'provider_revision' : 'profile_revision']: revision,
  }
  if (!handle.deployment) {
    const deployment = await client.createModelDeployment(
      noun,
      resource.id,
      {
        resource_version_id: revision.revision_id,
        environment: compiled.environment,
        closure: { resource_kind: kind, bindings },
      },
      published.etag,
      receipt('deploy'),
    )
    requireEtag(deployment)
    if (
      deployment.data.resource_id !== resource.id ||
      deployment.data.resource_version_id !== revision.revision_id ||
      deployment.data.resource_kind !== kind ||
      deployment.data.environment !== compiled.environment ||
      (await digestJson(deployment.data.closure as Json)) !==
        (await digestJson({ resource_kind: kind, bindings } as Json)) ||
      !sha(deployment.data.closure_digest)
    )
      conflict()
    if (
      deployment.data.closure_digest !==
      (await digestJson({ schema_version: 1, resource_kind: kind, bindings } as Json))
    )
      conflict()
    handle.deployment = deployment.data
    save(handle)
  }
  const deployment = handle.deployment
  if (
    deployment.environment !== compiled.environment ||
    (await digestJson(deployment.closure as Json)) !==
      (await digestJson({ resource_kind: kind, bindings } as Json))
  )
    conflict()
  // Re-read exact deployment state even when resuming a persisted completed HTTP response.
  const exact = await client.getDeployment(noun, resource.id, deployment.deployment_id)
  requireEtag(exact)
  if (
    (await digestJson(exact.data as unknown as Json)) !==
    (await digestJson(deployment as unknown as Json))
  )
    conflict()
  progress('正在激活配置')
  if (!handle.activation_etag) {
    const current = await client.getResource(noun, resource.id)
    requireResource(current, handle)
    if (current.data.version !== published.version + 1) conflict()
    handle.activation_etag = current.data.etag
    save(handle)
  }
  const activated = await client.activateModelDeployment(
    noun,
    resource.id,
    deployment.deployment_id,
    handle.activation_etag,
    receipt('activate'),
  )
  requireResource(activated, handle)
  const current = await client.getResource(noun, resource.id)
  requireResource(current, handle)
  if (
    current.data.gate_state !== 'enabled' ||
    current.data.active_deployment_id !== deployment.deployment_id ||
    current.data.version !== published.version + 2
  )
    conflict()
  if (handle.quota !== null) {
    progress('正在分配模型额度')
    const target = {
      deployment_id: deployment.deployment_id,
      resource_kind: 'model_deployment' as const,
      deployment_digest: deployment.closure_digest,
    }
    if (!handle.quota_intent) {
      handle.quota_intent = createQuotaIntent(
        client,
        await readModelQuota(client, handle.tenant_id, target),
        handle.quota,
      )
      save(handle)
    }
    await applyQuotaIntent(client, handle.tenant_id, handle.quota_intent)
  }
  sessionStorage.removeItem(KEY)
  progress('已就绪')
  return {
    resource: current.data,
    artifact,
    deployment: {
      deployment_id: deployment.deployment_id,
      resource_kind: kind === 'model_provider' ? 'model_provider_deployment' : 'model_deployment',
      deployment_digest: deployment.closure_digest,
    },
  }
}
