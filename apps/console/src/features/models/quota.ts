import type { PlatformClient } from '../../shared/api/client.ts'
import type { ExactDeploymentRef } from '../../shared/api/types.ts'
import type { ModelQuotaLimits, ModelQuotaView } from '../../shared/api/model-types.ts'
import { closed, exactModel, id, uuid4 } from './validation.ts'

const KEY = 'insight.console.model-quota.v1'
export const INITIAL_QUOTA: ModelQuotaLimits = {
  requests: 20,
  tokens: 204800,
  cost_microunits: 20000000,
}
const FIELDS = ['requests', 'tokens', 'cost_microunits'] as const
function conflict(): never {
  throw new Error(
    'model_quota_conflict: Resume the original allocation and reconcile current quota before changing inputs.',
  )
}
const integer = (value: unknown): value is number =>
  Number.isSafeInteger(value) && Number(value) >= 0
const etag = (value: unknown): value is string =>
  typeof value === 'string' && /^"model-quota-[0-9a-f]{64}"$/.test(value)
export function validQuotaLimits(value: unknown): value is ModelQuotaLimits {
  return (
    closed(value, [...FIELDS]) &&
    FIELDS.every((field) => integer((value as ModelQuotaLimits)[field]))
  )
}
function equalLimits(left: ModelQuotaLimits, right: ModelQuotaLimits): boolean {
  return FIELDS.every((field) => left[field] === right[field])
}
function equalTarget(left: ExactDeploymentRef, right: ExactDeploymentRef): boolean {
  return (
    left.resource_kind === right.resource_kind &&
    left.deployment_id === right.deployment_id &&
    left.deployment_digest === right.deployment_digest
  )
}
export function validQuotaView(value: ModelQuotaView): boolean {
  if (
    !closed(value, [
      'schema_version',
      'tenant_id',
      'model_deployment',
      'allocation',
      'tenant_concurrency',
      'etag',
    ]) ||
    value.schema_version !== 1 ||
    !id(value.tenant_id, 'ten') ||
    !exactModel(value.model_deployment) ||
    !etag(value.etag)
  )
    return false
  const concurrency = value.tenant_concurrency
  if (
    !closed(concurrency, ['limit', 'reserved', 'used']) ||
    ![concurrency.limit, concurrency.reserved, concurrency.used].every(integer) ||
    concurrency.reserved > concurrency.limit - concurrency.used
  )
    return false
  const allocation = value.allocation
  return (
    allocation === null ||
    (closed(allocation, ['limits', 'reserved', 'used']) &&
      [allocation.limits, allocation.reserved, allocation.used].every(validQuotaLimits) &&
      FIELDS.every(
        (field) => allocation.reserved[field] <= allocation.limits[field] - allocation.used[field],
      ))
  )
}
export interface ModelQuotaIntent {
  schema_version: 1
  origin: string
  tenant_id: string
  model_deployment: ExactDeploymentRef
  limits: ModelQuotaLimits
  etag: string
  receipt: string
}
export function validateQuotaIntent(value: ModelQuotaIntent): void {
  if (
    !closed(value, [
      'schema_version',
      'origin',
      'tenant_id',
      'model_deployment',
      'limits',
      'etag',
      'receipt',
    ]) ||
    value.schema_version !== 1 ||
    typeof value.origin !== 'string' ||
    value.origin.length > 2048 ||
    !id(value.tenant_id, 'ten') ||
    !exactModel(value.model_deployment) ||
    !validQuotaLimits(value.limits) ||
    !etag(value.etag) ||
    typeof value.receipt !== 'string' ||
    !value.receipt.startsWith('console-model-quota-') ||
    !uuid4(value.receipt.slice('console-model-quota-'.length))
  )
    conflict()
}
export async function readModelQuota(
  client: PlatformClient,
  tenantId: string,
  target: ExactDeploymentRef,
): Promise<ModelQuotaView> {
  if (!id(tenantId, 'ten') || !exactModel(target)) conflict()
  const response = await client.getModelQuota(target.deployment_id)
  if (
    !validQuotaView(response.data) ||
    response.data.tenant_id !== tenantId ||
    !equalTarget(response.data.model_deployment, target) ||
    response.etag !== response.data.etag
  )
    conflict()
  return response.data
}
export function createQuotaIntent(
  client: PlatformClient,
  current: ModelQuotaView,
  limits: ModelQuotaLimits,
): ModelQuotaIntent {
  if (!validQuotaView(current) || !validQuotaLimits(limits)) conflict()
  return {
    schema_version: 1,
    origin: client.origin,
    tenant_id: current.tenant_id,
    model_deployment: structuredClone(current.model_deployment),
    limits: { ...limits },
    etag: current.etag,
    receipt: `console-model-quota-${crypto.randomUUID()}`,
  }
}
export async function applyQuotaIntent(
  client: PlatformClient,
  tenantId: string,
  intent: ModelQuotaIntent,
): Promise<ModelQuotaView> {
  validateQuotaIntent(intent)
  if (intent.origin !== client.origin || intent.tenant_id !== tenantId) conflict()
  const response = await client.setModelQuota(
    intent.model_deployment,
    intent.limits,
    intent.etag,
    intent.receipt,
  )
  if (
    !validQuotaView(response.data) ||
    response.data.tenant_id !== tenantId ||
    !equalTarget(response.data.model_deployment, intent.model_deployment) ||
    response.etag !== response.data.etag ||
    response.data.allocation === null ||
    !equalLimits(response.data.allocation.limits, intent.limits)
  )
    conflict()
  const current = await readModelQuota(client, tenantId, intent.model_deployment)
  if (current.allocation === null || !equalLimits(current.allocation.limits, intent.limits))
    conflict()
  return current
}
export function hasPendingModelQuota(): boolean {
  return sessionStorage.getItem(KEY) !== null
}
function pending(client: PlatformClient, tenantId: string): ModelQuotaIntent | null {
  const raw = sessionStorage.getItem(KEY)
  if (raw === null) return null
  if (new TextEncoder().encode(raw).length > 4096) conflict()
  const intent = JSON.parse(raw) as ModelQuotaIntent
  validateQuotaIntent(intent)
  if (intent.origin !== client.origin || intent.tenant_id !== tenantId) conflict()
  return intent
}
export async function saveModelQuota(
  client: PlatformClient,
  current: ModelQuotaView,
  limits: ModelQuotaLimits,
): Promise<ModelQuotaView> {
  if (!validQuotaView(current) || !validQuotaLimits(limits)) conflict()
  const intent = pending(client, current.tenant_id) ?? createQuotaIntent(client, current, limits)
  if (
    !equalTarget(intent.model_deployment, current.model_deployment) ||
    !equalLimits(intent.limits, limits)
  )
    conflict()
  sessionStorage.setItem(KEY, JSON.stringify(intent))
  const result = await applyQuotaIntent(client, current.tenant_id, intent)
  sessionStorage.removeItem(KEY)
  return result
}
export async function resumeModelQuota(
  client: PlatformClient,
  tenantId: string,
): Promise<ModelQuotaView> {
  const intent = pending(client, tenantId)
  if (!intent) conflict()
  const result = await applyQuotaIntent(client, tenantId, intent)
  sessionStorage.removeItem(KEY)
  return result
}
