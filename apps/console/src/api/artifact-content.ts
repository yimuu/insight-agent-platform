import type { PlatformClient } from './client'
import type { ArtifactRef } from './types'

export function sameJson(left: unknown, right: unknown): boolean {
  if (left === right) return true
  if (left === null || right === null || typeof left !== 'object' || typeof right !== 'object' || Array.isArray(left) !== Array.isArray(right)) return false
  const a = Object.keys(left).sort(), b = Object.keys(right).sort()
  return a.length === b.length && a.every((key, index) => key === b[index] && sameJson((left as Record<string, unknown>)[key], (right as Record<string, unknown>)[key]))
}
export async function bytesDigest(bytes: Uint8Array): Promise<string> {
  const digest = await crypto.subtle.digest('SHA-256', new Uint8Array(bytes))
  return `sha256:${[...new Uint8Array(digest)].map((byte) => byte.toString(16).padStart(2, '0')).join('')}`
}
/** Every download reads current metadata and content authority again. No URL is retained. */
export async function readExactArtifact(client: PlatformClient, reference: ArtifactRef, options: { signal?: AbortSignal; maximumBytes: number; purpose?: string }): Promise<Blob> {
  if (!reference || !/^art_[0-9a-f-]{36}$/.test(reference.artifact_id) || !/^sha256:[0-9a-f]{64}$/.test(reference.content_digest)
    || !Number.isSafeInteger(reference.byte_length) || reference.byte_length < 0 || reference.byte_length > options.maximumBytes) throw new Error('The exact Artifact reference exceeds its supported content boundary.')
  const metadata = await client.getArtifact(reference.artifact_id, { signal: options.signal })
  if (metadata.data.artifact_id !== reference.artifact_id || metadata.data.state !== 'ready' || (options.purpose && metadata.data.purpose !== options.purpose)
    || !sameJson(metadata.data.content, reference)) throw new Error('Current Artifact metadata differs from the exact content reference.')
  const content = await client.downloadArtifact(reference.artifact_id, { signal: options.signal, maximumBytes: reference.byte_length })
  if (content.blob.size !== reference.byte_length || content.mediaType !== reference.media_type || content.etag !== `"${reference.content_digest}"`
    || await bytesDigest(new Uint8Array(await content.blob.arrayBuffer())) !== reference.content_digest) throw new Error('Artifact bytes, media type or digest differ from the exact reference.')
  options.signal?.throwIfAborted()
  return content.blob
}
