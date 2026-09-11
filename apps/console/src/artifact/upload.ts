import type { PlatformClient } from '../api/client.ts'
import type { ArtifactRef, ArtifactView, AuthorityResponse, Json, JsonObject, PrepareArtifactUploadResponse } from '../api/types.ts'
import { digestJson } from '../agent/compiler.ts'

// Metadata only. Signed URLs and completion proofs remain in memory, never browser storage.
export interface ArtifactUploadRecovery {
  schema_version: 1
  artifact_id: string
  operation_id: string
  upload_grant_id: string
  artifact_etag: string
  upload_expires_at: string
  request_digest: string
  prepare_receipt: string
  complete_receipt: string
  uploaded: boolean
}
const encoder = new TextEncoder()
function conflict(): never { throw new Error('artifact_upload_conflict: Retain the original upload attempt and reconcile current Artifact authority.') }
function id(value: unknown, prefix: string): value is string { return typeof value === 'string' && new RegExp(`^${prefix}_[0-9a-f]{8}-[0-9a-f]{4}-7[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$`).test(value) }
function text(value: unknown, limit: number): value is string { return typeof value === 'string' && value.length > 0 && encoder.encode(value).length <= limit }
function expiry(value: unknown): value is string { return typeof value === 'string' && /^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{6}Z$/.test(value) && Number.isFinite(Date.parse(value)) }
function positive(value: unknown): value is number { return typeof value === 'number' && Number.isSafeInteger(value) && value > 0 }
export function validateArtifactUploadRecovery(value: ArtifactUploadRecovery): void {
  const fields = ['schema_version', 'artifact_id', 'operation_id', 'upload_grant_id', 'artifact_etag', 'upload_expires_at', 'request_digest', 'prepare_receipt', 'complete_receipt', 'uploaded']
  if (!value || typeof value !== 'object' || Object.keys(value).length !== fields.length || fields.some(field => !Object.hasOwn(value, field)) || value.schema_version !== 1
    || !id(value.artifact_id, 'art') || !id(value.operation_id, 'job') || !id(value.upload_grant_id, 'grt')
    || typeof value.artifact_etag !== 'string' || !new RegExp(`^"${value.artifact_id}-[1-9][0-9]*"$`).test(value.artifact_etag)
    || !expiry(value.upload_expires_at) || !/^sha256:[0-9a-f]{64}$/.test(value.request_digest) || !text(value.prepare_receipt, 255) || !text(value.complete_receipt, 255) || typeof value.uploaded !== 'boolean') conflict()
}
function validatePrepared(value: PrepareArtifactUploadResponse): void {
  if (value.schema_version !== 1 || !id(value.artifact_id, 'art') || !id(value.operation_id, 'job') || !id(value.upload_grant_id, 'grt')
    || !text(value.artifact_etag, 255) || !expiry(value.upload_expires_at) || !value.upload_target || !text(value.upload_target.url, 8192)
    || !text(value.upload_target.completion_proof, 4096) || !/^[\x21-\x7e]+$/.test(value.upload_target.completion_proof)) conflict()
  let url: URL
  try { url = new URL(value.upload_target.url) } catch { conflict() }
  if (url.protocol !== 'https:' || url.username || url.password || url.hash) conflict()
}
function samePrepared(old: ArtifactUploadRecovery, value: PrepareArtifactUploadResponse): void {
  validatePrepared(value)
  if (old.artifact_id !== value.artifact_id || old.operation_id !== value.operation_id || old.upload_grant_id !== value.upload_grant_id
    || old.artifact_etag !== value.artifact_etag || old.upload_expires_at !== value.upload_expires_at) conflict()
}
function currentArtifact(response: AuthorityResponse<ArtifactView>, state: ArtifactUploadRecovery, request: JsonObject): ArtifactView {
  const value = response.data
  if (value.schema_version !== 1 || value.artifact_id !== state.artifact_id || !positive(value.version) || value.etag !== `"${value.artifact_id}-${value.version}"` || response.etag !== value.etag
    || value.purpose !== request.purpose || value.classification !== request.classification || value.expected_size_bytes !== request.expected_size_bytes || value.declared_media_type !== request.declared_media_type) conflict()
  return value
}
function ready(value: ArtifactView, state: ArtifactUploadRecovery, request: JsonObject): ArtifactRef {
  const content = value.content
  if (value.state !== 'ready' || !content || content.artifact_id !== state.artifact_id || content.content_digest !== request.expected_digest || content.byte_length !== request.expected_size_bytes
    || content.media_type !== request.declared_media_type || content.classification !== request.classification || content.display_name !== request.display_name) conflict()
  return content
}
export async function uploadArtifact(
  client: PlatformClient, bytes: Uint8Array, intent: JsonObject,
  prepareReceipt: string, completeReceipt: string, saved: ArtifactUploadRecovery | null,
  persist: (state: ArtifactUploadRecovery) => void,
): Promise<ArtifactRef> {
  const request: JsonObject = { schema_version: 1, purpose: intent.purpose, classification: intent.classification, expected_size_bytes: bytes.byteLength,
    expected_digest: intent.content_digest, declared_media_type: intent.media_type, display_name: intent.display_name ?? null }
  const requestDigest = await digestJson(request as Json)
  const actualDigest = `sha256:${Array.from(new Uint8Array(await crypto.subtle.digest('SHA-256', new Uint8Array(bytes).buffer)), byte => byte.toString(16).padStart(2, '0')).join('')}`
  if (!positive(bytes.byteLength) || actualDigest !== request.expected_digest) conflict()
  let state = saved
  let prepared: PrepareArtifactUploadResponse | null = null
  if (state) {
    validateArtifactUploadRecovery(state)
    if (state.request_digest !== requestDigest || state.prepare_receipt !== prepareReceipt || state.complete_receipt !== completeReceipt) conflict()
  } else {
    prepared = (await client.prepareArtifactUpload(request, prepareReceipt)).data
    validatePrepared(prepared)
    state = { schema_version: 1, artifact_id: prepared.artifact_id, operation_id: prepared.operation_id, upload_grant_id: prepared.upload_grant_id,
      artifact_etag: prepared.artifact_etag, upload_expires_at: prepared.upload_expires_at, request_digest: requestDigest,
      prepare_receipt: prepareReceipt, complete_receipt: completeReceipt, uploaded: false }
    validateArtifactUploadRecovery(state)
    persist(state) // Before any object PUT or complete mutation; no capabilities are persisted.
  }
  let current = currentArtifact(await client.getArtifact(state.artifact_id), state, request)
  if (current.state === 'ready') return ready(current, state, request)
  if (current.state === 'staging') {
    if (current.etag !== state.artifact_etag || Date.parse(state.upload_expires_at) <= Date.now()) conflict()
    // Only a still-staging attempt may ask for an ephemeral target, with its original deadline.
    prepared ??= (await client.prepareArtifactUpload(request, state.prepare_receipt)).data
    samePrepared(state, prepared)
    if (!state.uploaded) {
      await client.putArtifactObject(prepared.upload_target.url, bytes, String(request.declared_media_type))
      state = { ...state, uploaded: true }; persist(state)
    }
    if (Date.parse(state.upload_expires_at) <= Date.now()) conflict()
    await client.completeArtifactUpload(state.artifact_id, { schema_version: 1, completion_proof: prepared.upload_target.completion_proof }, state.artifact_etag, state.complete_receipt)
  } else if (!['uploaded', 'verifying', 'verified'].includes(current.state)) conflict()
  const operation = await client.waitOperation(state.operation_id)
  if (operation.data.operation_id !== state.operation_id || operation.data.state !== 'succeeded') throw new Error('artifact_verification_pending: Inspect the original Artifact operation before resuming.')
  current = currentArtifact(await client.getArtifact(state.artifact_id), state, request)
  return ready(current, state, request)
}
