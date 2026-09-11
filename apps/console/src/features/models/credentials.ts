import { id, sha, uuid4, closed } from './validation.ts'
import type { PlatformClient } from '../../shared/api/client.ts'
import type { ExactModelCredential } from '../../shared/api/model-types.ts'

const KEY = 'insight.console.model-credential.v1'
export interface SourceCredentialIntent {
  display_name: string
  tenant_id: string
  provider_id: string
  alias: string
  destination_digest: string
  resource_id: string | null
  resource_etag: string | null
}
interface Handle {
  schema_version: 1
  origin: string
  operation_id: string
  intent: SourceCredentialIntent
  binding: ExactModelCredential | null
}
function conflict(): never {
  throw new Error(
    'credential_import_pending: Re-enter the same key and resume the original source configuration before starting another import.',
  )
}
function validate(value: Handle): void {
  if (
    !closed(value, ['schema_version', 'origin', 'operation_id', 'intent', 'binding']) ||
    value.schema_version !== 1 ||
    typeof value.origin !== 'string' ||
    !uuid4(value.operation_id) ||
    !closed(value.intent, [
      'display_name',
      'tenant_id',
      'provider_id',
      'alias',
      'destination_digest',
      'resource_id',
      'resource_etag',
    ]) ||
    !id(value.intent.tenant_id, 'ten') ||
    !id(value.intent.provider_id, 'spr') ||
    !/^[a-z][a-z0-9._-]{0,63}$/.test(value.intent.alias) ||
    !sha(value.intent.destination_digest) ||
    typeof value.intent.display_name !== 'string' ||
    !value.intent.display_name.trim() ||
    new TextEncoder().encode(value.intent.display_name).length > 255
  )
    conflict()
  if (
    value.intent.resource_id !== null &&
    (!id(value.intent.resource_id, 'mpr') ||
      typeof value.intent.resource_etag !== 'string' ||
      !new RegExp(`^"${value.intent.resource_id}-[1-9][0-9]*"$`).test(value.intent.resource_etag))
  )
    conflict()
  if (value.intent.resource_id === null && value.intent.resource_etag !== null) conflict()
  if (
    value.binding &&
    (!closed(value.binding, [
      'secret_binding_id',
      'binding_generation',
      'provider_id',
      'purpose',
      'resolution_policy',
      'resolution_policy_digest',
    ]) ||
      value.binding.provider_id !== value.intent.provider_id ||
      value.binding.purpose !== 'model_api_key' ||
      value.binding.binding_generation !== 1 ||
      !id(value.binding.secret_binding_id, 'sbd') ||
      !sha(value.binding.resolution_policy_digest))
  )
    conflict()
}
export async function importSourceCredential(
  client: PlatformClient,
  intent: SourceCredentialIntent,
  key: string,
): Promise<ExactModelCredential> {
  const raw = sessionStorage.getItem(KEY)
  if (raw && new TextEncoder().encode(raw).length > 8192) conflict()
  const handle: Handle = raw
    ? (JSON.parse(raw) as Handle)
    : {
        schema_version: 1,
        origin: client.origin,
        operation_id: crypto.randomUUID(),
        intent,
        binding: null,
      }
  validate(handle)
  if (handle.origin !== client.origin || JSON.stringify(handle.intent) !== JSON.stringify(intent))
    conflict()
  // Persist only operation metadata; raw keys never enter browser storage.
  if (!raw) sessionStorage.setItem(KEY, JSON.stringify(handle))
  if (!key) {
    if (handle.binding) return handle.binding
    conflict()
  }
  try {
    const result = await client.importModelCredential(handle.operation_id, intent.provider_id, key)
    if (result.data.schema_version !== 1) conflict()
    handle.binding = result.data.binding
    validate(handle)
    sessionStorage.setItem(KEY, JSON.stringify(handle))
    return handle.binding
  } catch {
    // Do not surface provider/request bodies from this sensitive boundary.
    throw new Error(
      'credential_import_pending: Import did not complete. Re-enter the same key and retry; the saved operation identity will be reused.',
    )
  }
}
export function finishSourceCredential(): void {
  sessionStorage.removeItem(KEY)
}
export function hasPendingCredential(): boolean {
  return sessionStorage.getItem(KEY) !== null
}

export function pendingSourceCredential(
  origin: string,
  tenantId: string,
): { intent: SourceCredentialIntent; binding: ExactModelCredential | null } | null {
  const raw = sessionStorage.getItem(KEY)
  if (!raw) return null
  if (new TextEncoder().encode(raw).length > 8192) conflict()
  const handle = JSON.parse(raw) as Handle
  validate(handle)
  if (handle.origin !== origin || handle.intent.tenant_id !== tenantId) conflict()
  return { intent: handle.intent, binding: handle.binding }
}
