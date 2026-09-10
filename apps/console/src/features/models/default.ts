import { id, uuid4, positive, closed, exactModel } from './validation.ts'
import type { PlatformClient } from '../../shared/api/client.ts'
import type { ExactDeploymentRef, JsonObject } from '../../shared/api/types.ts'
import type { ModelDefault } from '../../shared/api/model-types.ts'
const KEY = 'insight.console.model-default.v1'
interface Intent {
  schema_version: 1
  origin: string
  tenant_id: string
  etag: string
  receipt: string
  model: ExactDeploymentRef | null
}
export function hasPendingModelDefault(): boolean {
  return sessionStorage.getItem(KEY) !== null
}
function pending(client: PlatformClient, tenantId: string): Intent | null {
  const raw = sessionStorage.getItem(KEY)
  if (!raw) return null
  if (new TextEncoder().encode(raw).length > 4096)
    throw new Error('model_default_conflict: Invalid pending default selection.')
  const intent = JSON.parse(raw) as Intent
  if (
    !closed(intent, ['schema_version', 'origin', 'tenant_id', 'etag', 'receipt', 'model']) ||
    intent.schema_version !== 1 ||
    intent.origin !== client.origin ||
    intent.tenant_id !== tenantId ||
    !id(intent.tenant_id, 'ten') ||
    typeof intent.etag !== 'string' ||
    !new RegExp(`^"${intent.tenant_id}-[1-9][0-9]*"$`).test(intent.etag) ||
    typeof intent.receipt !== 'string' ||
    !intent.receipt.startsWith('console-model-default-') ||
    !uuid4(intent.receipt.slice('console-model-default-'.length)) ||
    (intent.model !== null && !exactModel(intent.model))
  )
    throw new Error('model_default_conflict: Invalid pending default selection.')
  return intent
}
export async function resumeModelDefault(
  client: PlatformClient,
  current: ModelDefault,
): Promise<ModelDefault> {
  const intent = pending(client, current.tenant_id)
  if (!intent) throw new Error('model_default_conflict: No pending default selection.')
  return selectModelDefault(client, current, intent.model)
}
export async function selectModelDefault(
  client: PlatformClient,
  current: ModelDefault,
  model: ExactDeploymentRef | null,
): Promise<ModelDefault> {
  if (!validDefault(current) || (model !== null && !exactModel(model)))
    throw new Error('model_default_conflict: Invalid model selection authority.')
  const saved = pending(client, current.tenant_id)
  const intent: Intent = saved ?? {
    schema_version: 1,
    origin: client.origin,
    tenant_id: current.tenant_id,
    etag: current.etag,
    receipt: `console-model-default-${crypto.randomUUID()}`,
    model,
  }
  if (!sameModel(intent.model, model))
    throw new Error('model_default_conflict: Resume the pending default selection first.')
  if (!saved) sessionStorage.setItem(KEY, JSON.stringify(intent))
  const result = await client.setModelDefault(
    { schema_version: 1, default_model: intent.model as unknown as JsonObject | null },
    intent.etag,
    intent.receipt,
  )
  const verified = await client.getModelDefault()
  if (
    !validDefault(result.data) ||
    !validDefault(verified.data) ||
    result.data.tenant_id !== intent.tenant_id ||
    verified.data.tenant_id !== intent.tenant_id ||
    result.etag !== result.data.etag ||
    verified.etag !== verified.data.etag ||
    !sameModel(result.data.default_model, model) ||
    !sameModel(verified.data.default_model, model)
  )
    throw new Error('model_default_conflict: Current default differs from this selection.')
  sessionStorage.removeItem(KEY)
  return verified.data
}

function sameModel(left: ExactDeploymentRef | null, right: ExactDeploymentRef | null): boolean {
  return left === null || right === null
    ? left === right
    : left.deployment_id === right.deployment_id &&
        left.deployment_digest === right.deployment_digest &&
        left.resource_kind === right.resource_kind
}
function validDefault(value: ModelDefault): boolean {
  return (
    closed(value, ['schema_version', 'tenant_id', 'default_model', 'version', 'etag']) &&
    value.schema_version === 1 &&
    id(value.tenant_id, 'ten') &&
    positive(value.version) &&
    value.etag === `"${value.tenant_id}-${value.version}"` &&
    (value.default_model === null || exactModel(value.default_model))
  )
}
