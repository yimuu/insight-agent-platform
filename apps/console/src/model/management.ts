import type { PlatformClient } from '../api/client.ts'
import type { ExactDeploymentRef } from '../api/types.ts'
import type { ModelConnectionObservation, ModelConnectionOutcome, ModelCredentialMetadata } from './types.ts'
import { closed, exactModel, id, positive, sha, uuid4 } from './validation.ts'

const REVOKE_KEY = 'insight.console.model-revoke.v1'
interface RevokeIntent { schema_version: 1; origin: string; tenant_id: string; credential: ModelCredentialMetadata; receipt: string }
export const CONNECTION_LABELS: Record<ModelConnectionOutcome, string> = {
  response_received: 'Provider response received', credentials_rejected: 'Credential rejected', model_unavailable: 'Model unavailable',
  rate_limited: 'Provider rate limit reached', provider_unavailable: 'Provider unavailable', invalid_response: 'Invalid protocol response', timed_out: 'Connection timed out', transport_unavailable: 'Transport unavailable',
}
export function validCredential(value: ModelCredentialMetadata): boolean {
  return closed(value, ['schema_version', 'tenant_id', 'secret_binding_id', 'provider_id', 'purpose', 'state', 'generation', 'version', 'etag']) && value.schema_version === 1 && id(value.tenant_id, 'ten') && id(value.secret_binding_id, 'sbd') && id(value.provider_id, 'spr') && value.purpose === 'model_api_key' && ['active', 'revoked'].includes(value.state) && positive(value.generation) && positive(value.version) && value.etag === `"${value.secret_binding_id}-${value.version}"`
}
export async function readModelCredential(client: PlatformClient, tenant: string, bindingId: string): Promise<ModelCredentialMetadata> {
  if (!id(tenant, 'ten') || !id(bindingId, 'sbd')) throw new Error('model_credential_invalid: Invalid binding identity.')
  const result = await client.getModelCredential(bindingId)
  if (!validCredential(result.data) || result.data.tenant_id !== tenant || result.data.secret_binding_id !== bindingId || result.etag !== result.data.etag) throw new Error('model_credential_invalid: Invalid credential metadata.')
  return result.data
}
export function pendingCredentialRevocation(client: PlatformClient, tenant: string): RevokeIntent | null {
  const raw = sessionStorage.getItem(REVOKE_KEY)
  if (!raw) return null
  if (raw.length > 4096) throw new Error('model_credential_conflict: Invalid pending revocation.')
  const intent = JSON.parse(raw) as RevokeIntent
  if (!closed(intent, ['schema_version', 'origin', 'tenant_id', 'credential', 'receipt']) || intent.schema_version !== 1 || intent.origin !== client.origin || !id(tenant, 'ten') || intent.tenant_id !== tenant || !validCredential(intent.credential) || intent.credential.tenant_id !== tenant || intent.credential.state !== 'active' || !intent.receipt?.startsWith('console-model-revoke-') || !uuid4(intent.receipt.slice('console-model-revoke-'.length))) throw new Error('model_credential_conflict: Pending revocation belongs to another session or is invalid.')
  return intent
}
export async function revokeCredential(client: PlatformClient, tenant: string, credential: ModelCredentialMetadata): Promise<ModelCredentialMetadata> {
  if (!id(tenant, 'ten') || !validCredential(credential) || credential.tenant_id !== tenant) throw new Error('model_credential_invalid')
  const pending = pendingCredentialRevocation(client, tenant)
  if (pending && pending.credential.secret_binding_id !== credential.secret_binding_id) throw new Error('model_credential_conflict: Resume the pending revocation first.')
  if (!pending && credential.state === 'revoked') return readModelCredential(client, tenant, credential.secret_binding_id)
  const intent: RevokeIntent = pending ?? { schema_version: 1, origin: client.origin, tenant_id: tenant, credential, receipt: `console-model-revoke-${crypto.randomUUID()}` }
  if (!pending) sessionStorage.setItem(REVOKE_KEY, JSON.stringify(intent))
  const original = intent.credential
  const result = await client.revokeModelCredential(original.secret_binding_id, original.generation, original.etag, intent.receipt)
  const current = await readModelCredential(client, tenant, original.secret_binding_id)
  for (const value of [result.data, current]) {
    if (!validCredential(value) || value.tenant_id !== tenant || value.secret_binding_id !== original.secret_binding_id || value.provider_id !== original.provider_id || value.state !== 'revoked' || value.generation !== original.generation + 1 || value.version !== original.version + 1) throw new Error('model_credential_conflict: Revocation differs from the original intent.')
  }
  if (result.etag !== result.data.etag) throw new Error('model_credential_invalid')
  sessionStorage.removeItem(REVOKE_KEY)
  return current
}
export async function probeConnection(client: PlatformClient, installation: string, model: ExactDeploymentRef): Promise<ModelConnectionObservation> {
  if (!sha(installation) || !exactModel(model)) throw new Error('model_connection_invalid')
  const result = (await client.probeModel(installation, model)).data
  if (!closed(result, ['schema_version', 'model_deployment', 'provider_deployment', 'model_identity', 'protocol', 'observed_at', 'outcome']) || result.schema_version !== 1 || !exactModel(result.model_deployment) || result.model_deployment.deployment_id !== model.deployment_id || result.model_deployment.deployment_digest !== model.deployment_digest
    || !closed(result.provider_deployment, ['resource_kind', 'deployment_id', 'deployment_digest']) || result.provider_deployment.resource_kind !== 'model_provider_deployment' || !id(result.provider_deployment.deployment_id, 'mpdep') || !sha(result.provider_deployment.deployment_digest)
    || !closed(result.model_identity, ['value', 'stability']) || typeof result.model_identity.value !== 'string' || result.model_identity.value.length === 0 || new TextEncoder().encode(result.model_identity.value).length > 512 || Array.from(result.model_identity.value).length > 255 || Array.from(result.model_identity.value).some((character) => { const code = character.codePointAt(0)!; return code < 32 || code >= 127 && code <= 159 }) || !['pinned', 'externally_mutable'].includes(result.model_identity.stability)
    || !['open_ai_responses', 'anthropic_messages'].includes(result.protocol) || typeof result.observed_at !== 'string' || !Number.isFinite(Date.parse(result.observed_at)) || !Object.hasOwn(CONNECTION_LABELS, result.outcome)) throw new Error('model_connection_invalid: Invalid connection observation.')
  return result
}
