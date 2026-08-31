import assert from 'node:assert/strict'
import { readFile } from 'node:fs/promises'
import test from 'node:test'

import { compileAgentManifest } from './compiler.ts'
import { publishCompiledAgent } from './publication.ts'

const corpusRoot = new URL('../../../../contracts/product-experience/agent-compiler/v1/', import.meta.url)
const text = (relative) => readFile(new URL(relative, corpusRoot), 'utf8')

function storage() {
  const values = new Map()
  return {
    getItem: (key) => values.get(key) ?? null,
    setItem: (key, value) => values.set(key, String(value)),
    removeItem: (key) => values.delete(key),
    snapshot: () => [...values.values()].join('\n'),
  }
}

function artifact(id, digest, displayName) {
  return {
    artifact_id: id,
    content_digest: digest,
    byte_length: 100,
    media_type: 'application/json',
    classification: 'internal',
    display_name: displayName,
  }
}

test('publication recovery resumes exact receipts without persisting manifest or input', async () => {
  const corpus = JSON.parse(await text('corpus.json'))
  const fixture = corpus.cases.find((candidate) => candidate.case_id === 'deterministic-yaml')
  const compiled = await compileAgentManifest({
    manifest: await text(fixture.manifest),
    inputSchema: await text(fixture.input_schema),
    outputSchema: await text(fixture.output_schema),
    profile: corpus.profile,
    bindings: fixture.bindings,
  })
  const memory = storage()
  globalThis.sessionStorage = memory
  const calls = []
  let publishFails = true
  const resource = (etag, validation) => ({
    schema_version: 1,
    resource_id: 'agt_0198f1cc-32e4-75e1-a9e8-d95ca0f80001',
    resource_kind: 'agent',
    lifecycle_state: 'active',
    gate_state: 'enabled',
    draft_generation: 1,
    version: 1,
    draft: { display_name: 'Echo Agent', document: {}, validation },
    etag,
  })
  const refs = [
    artifact('art_0198f1cc-32e4-75e1-a9e8-d95ca0f80002', compiled.manifestDigest, 'echo-agent.agent.json'),
    artifact('art_0198f1cc-32e4-75e1-a9e8-d95ca0f80003', compiled.typedPlanDigest, 'echo-agent.plan.json'),
  ]
  let prepareIndex = 0
  const client = {
    prepareArtifactUpload: async (_body, receipt) => {
      const selected = refs[prepareIndex++]
      calls.push(['prepare', receipt])
      return { data: { artifact_id: selected.artifact_id, operation_id: `job_${selected.artifact_id.slice(4)}`, upload_grant_id: `grt_${selected.artifact_id.slice(4)}`, artifact_etag: '"artifact-v1"', upload_target: { url: 'https://objects.example/upload', completion_proof: 'proof' } } }
    },
    putArtifactObject: async () => calls.push(['put']),
    completeArtifactUpload: async (_id, _body, _etag, receipt) => { calls.push(['complete', receipt]); return { data: {} } },
    waitOperation: async () => ({ data: { state: 'succeeded', error: null } }),
    getArtifact: async (id) => ({ data: { state: 'ready', content: refs.find((item) => item.artifact_id === id) } }),
    createAgent: async (_body, receipt) => { calls.push(['create', receipt]); return { data: resource('"resource-v1"', null) } },
    validateAgent: async (_id, _etag, receipt) => { calls.push(['validate', receipt]); return { data: { operation_id: 'job_0198f1cc-32e4-75e1-a9e8-d95ca0f80004' } } },
    getResource: async () => ({ data: resource('"resource-v4"', {}) }),
    publishAgent: async (_id, _body, _etag, receipt) => {
      calls.push(['publish', receipt])
      if (publishFails) { publishFails = false; throw new Error('simulated response loss') }
      return { data: {
        etag: '"resource-v3"',
        published_versions: [
          { resource_version_id: 'aif_0198f1cc-32e4-75e1-a9e8-d95ca0f80005', revision_no: 1, content_digest: compiled.contractDigest, artifact_id: refs[0].artifact_id, etag: '"aif"' },
          { resource_version_id: 'arev_0198f1cc-32e4-75e1-a9e8-d95ca0f80006', revision_no: 1, content_digest: compiled.typedPlanDigest, artifact_id: refs[0].artifact_id, etag: '"arev"' },
        ],
      } }
    },
    createAgentDeployment: async (_id, _body, _etag, receipt) => { calls.push(['deploy', receipt]); return { data: { deployment_id: 'adep_0198f1cc-32e4-75e1-a9e8-d95ca0f80007' } } },
    activateAgentDeployment: async (_id, _deployment, _etag, receipt) => { calls.push(['activate', receipt]); return { data: resource('"resource-v5"', {}) } },
  }

  await assert.rejects(publishCompiledAgent(client, compiled, null, () => {}), /response loss/)
  const persisted = memory.snapshot()
  assert.equal(persisted.includes(compiled.canonicalManifest), false)
  assert.equal(persisted.includes('input.schema.json'), false)
  const before = calls.filter(([kind]) => ['prepare', 'create', 'validate'].includes(kind)).length
  const stages = []
  const result = await publishCompiledAgent(client, compiled, null, (stage) => stages.push(stage))
  const after = calls.filter(([kind]) => ['prepare', 'create', 'validate'].includes(kind)).length
  assert.equal(after, before)
  assert.equal(result.resource.gate_state, 'enabled')
  assert.deepEqual(stages, ['validating', 'publishing', 'activating', 'ready'])
  assert.equal(memory.snapshot(), '')
  const publishReceipts = calls.filter(([kind]) => kind === 'publish').map(([, receipt]) => receipt)
  assert.equal(publishReceipts[0], publishReceipts[1])
})
