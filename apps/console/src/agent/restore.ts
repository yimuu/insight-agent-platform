import type { PlatformClient } from '../api/client'
import type { ArtifactRef } from '../api/types'
import { readExactArtifact, sameJson } from '../api/artifact-content.ts'
import { compileFrozenSourceBundle } from './compiler.ts'
import { MAX_EDITOR_BUNDLE_BYTES, readEditableSourceBundle } from './editor.ts'
import { materializeDocument } from './publication.ts'

const object = (value: unknown): value is Record<string, unknown> => value !== null && typeof value === 'object' && !Array.isArray(value)
export async function restorePublishedSources(client: PlatformClient, agentId: string, versionId: string, signal?: AbortSignal) {
  const uuid = '[0-9a-f]{8}-[0-9a-f]{4}-7[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}'
  if (!new RegExp(`^agt_${uuid}$`).test(agentId) || !new RegExp(`^(aif|arev)_${uuid}$`).test(versionId)) throw new Error('Choose an exact Agent and published interface or Plan version ID.')
  const response = await client.getResourceVersion('agents', agentId, versionId, { signal })
  const version = response.data
  if (version.schema_version !== 1 || version.resource_id !== agentId || version.resource_kind !== 'agent' || version.resource_version_id !== versionId
    || !Number.isSafeInteger(version.revision_no) || version.revision_no < 1 || !/^sha256:[0-9a-f]{64}$/.test(version.content_digest)
    || version.etag !== response.etag || version.etag !== `"${versionId}-${version.content_digest.slice(7)}"`) throw new Error('Published version identity or ETag differs from the exact selection.')
  const document = version.payload.document
  if (!object(document) || document.resource_kind !== 'agent' || !object(document.spec) || !object(document.spec.authoring_package)) throw new Error('Published version has no complete Agent source authority.')
  const spec = document.spec
  const authoring = spec.authoring_package as Record<string, unknown>
  const reference = authoring.artifact as unknown as ArtifactRef
  if (reference?.media_type !== 'application/json') throw new Error('Published authoring source is not JSON.')
  const blob = await readExactArtifact(client, reference, { signal, maximumBytes: MAX_EDITOR_BUNDLE_BYTES, purpose: 'authoring_document' })
  const text = await blob.text()
  const compiled = await compileFrozenSourceBundle(text, signal)
  if (compiled.sourceBundleDigest !== reference.content_digest || compiled.manifestDigest !== authoring.manifest_digest || compiled.typedPlanDigest !== spec.typed_plan_digest
    || version.content_digest !== (versionId.startsWith('aif_') ? compiled.contractDigest : compiled.typedPlanDigest)
    || (versionId.startsWith('arev_') && version.artifact_id !== spec.typed_plan_artifact_id)) throw new Error('Recompiled sources differ from the exact published manifest, interface or Plan.')
  if (typeof spec.typed_plan_artifact_id !== 'string') throw new Error('Published Plan Artifact identity is missing.')
  const plan = await client.getArtifact(spec.typed_plan_artifact_id, { signal })
  if (plan.data.state !== 'ready' || plan.data.purpose !== 'typed_plan' || !plan.data.content || plan.data.content.content_digest !== compiled.typedPlanDigest
    || plan.data.content.artifact_id !== spec.typed_plan_artifact_id || !sameJson(materializeDocument(compiled, reference, plan.data.content), document)) throw new Error('Current Plan Artifact or complete Agent document differs from its compiled sources.')
  const sources = await readEditableSourceBundle(text)
  const current = await client.getResource('agents', agentId, { signal })
  if (current.data.resource_id !== agentId || current.data.resource_kind !== 'agent' || current.etag !== current.data.etag) throw new Error('Current Agent edit authority differs from the selected Agent.')
  signal?.throwIfAborted()
  return { sources, current: current.data, version, compiled }
}
