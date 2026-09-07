import '../../tests/wasm-worker-fixture.mjs'
import assert from 'node:assert/strict'
import { readFile } from 'node:fs/promises'
import test from 'node:test'
import { createServer } from 'node:http'
import { PlatformClient } from '../api/client.ts'
import { compileAgentManifest } from './compiler.ts'
import { publishCompiledAgent } from './publication.ts'

const corpusRoot = new URL('../../../../contracts/product-experience/agent-compiler/v2/', import.meta.url)
const text = (relative) => readFile(new URL(relative, corpusRoot), 'utf8')
const handleKey = 'insight.console.agent-publication.v3'
const id = (prefix, counter) => prefix + '_0198f1cc-32e4-75e1-a9e8-' + counter.toString(16).padStart(12, '0')

async function compilation(displayName) {
  const corpus = JSON.parse(await text('corpus.json'))
  const item = corpus.cases.find((candidate) => candidate.case_id === 'deterministic-yaml')
  const original = await text(item.manifest)
  const manifest = displayName ? original.replace('  name: echo-agent', '  name: echo-agent\n  displayName: ' + displayName) : original
  return compileAgentManifest({
    manifest, inputSchema: await text(item.input_schema),
    outputSchema: await text(item.output_schema), profile: corpus.profile, bindings: item.bindings,
  })
}

// The HTTP fixture owns CAS and Receipt replay independently of the client journal.
// Artifact transport is stubbed here; the native Chrome journey covers actual PUT.
async function fixture(t, options = {}) {
  const memory = new Map()
  globalThis.sessionStorage = {
    getItem: key => memory.get(key) ?? null,
    setItem: (key, value) => memory.set(key, String(value)),
    removeItem: key => memory.delete(key),
  }
  const receipts = new Map(), calls = [], artifacts = new Map(), uploads = new Map()
  let counter = 10, resource = null, published = null, drop = options.drop ?? null
  let failPublish = options.failPublish ?? false
  const agentId = id('agt', 1)
  const etag = () => '"resource-v' + resource.version + '"'
  const observe = () => structuredClone(resource)
  const advance = () => { resource.version++; resource.etag = etag() }
  const server = createServer(async (request, response) => {
    try {
      const chunks = []
      for await (const chunk of request) chunks.push(chunk)
      const raw = Buffer.concat(chunks).toString()
      const body = raw ? JSON.parse(raw) : null
      const path = request.url
      const send = (status, value) => {
        response.writeHead(status, { 'content-type': 'application/json', ...(value?.etag ? { etag: options.invalidCurrentEtag && request.method === 'GET' && value.gate_state === 'enabled' ? '"different"' : value.etag } : {}) })
        response.end(JSON.stringify(value))
      }
      if (request.method === 'GET') {
        assert.equal(path, '/v1/agents/' + agentId)
        return send(200, observe())
      }
      const phase = path.endsWith(':activate') ? 'activate'
        : path.endsWith('/deployments') ? 'deploy'
        : path.endsWith('draft:publish') ? 'publish'
        : path.endsWith('draft:validate') ? 'validate'
        : request.method === 'PUT' ? 'update' : 'create'
      const key = request.headers['idempotency-key']
      const fingerprint = JSON.stringify([request.method, path, raw, request.headers['if-match'] ?? null])
      calls.push({ phase, key, fingerprint, body, etag: request.headers['if-match'] })
      const replay = receipts.get(key)
      if (replay) {
        if (replay.fingerprint !== fingerprint) return send(409, { code: 'receipt_conflict', detail: 'Receipt request changed' })
        return send(200, replay.value)
      }
      if (phase !== 'create' && request.headers['if-match'] !== resource.etag) return send(409, { code: 'version_conflict', detail: 'CAS changed' })
      if (phase === 'publish' && failPublish) {
        failPublish = false
        return send(503, { code: 'temporary_failure', detail: 'publication unavailable' })
      }
      let value
      if (phase === 'create') {
        resource = { schema_version: 1, resource_id: agentId, resource_kind: 'agent', lifecycle_state: 'active',
          gate_state: 'disabled', active_deployment_id: null, draft_generation: 1, version: 1,
          draft: { ...body, validation: null }, etag: '"resource-v1"' }
        value = observe()
      } else if (phase === 'update') {
        resource.draft = { ...body, validation: null }
        resource.draft_generation++
        advance()
        value = observe()
      } else if (phase === 'validate') {
        resource.draft.validation = { status: 'valid' }
        advance()
        if (options.driftDuringValidation === 'version') advance()
        if (options.driftDuringValidation === 'generation') resource.draft_generation++
        if (options.driftDuringValidation === 'document') resource.draft.document.spec.default_deadline_seconds++
        value = { operation_id: id('job', counter++) }
      } else if (phase === 'publish') {
        const spec = resource.draft.document.spec
        assert.notEqual(spec.authoring_package.artifact.artifact_id, spec.typed_plan_artifact_id)
        assert.equal(body.artifact_id, spec.typed_plan_artifact_id)
        assert.equal(body.plan_content_digest, spec.typed_plan_digest)
        advance()
        published = [
          { resource_version_id: id('aif', counter++), revision_no: resource.draft_generation, content_digest: body.interface_content_digest, artifact_id: body.artifact_id, etag: '"aif"' },
          { resource_version_id: id('arev', counter++), revision_no: resource.draft_generation, content_digest: body.plan_content_digest, artifact_id: body.artifact_id, etag: '"arev"' },
        ]
        value = { schema_version: 1, resource_id: agentId, resource_kind: 'agent', draft_generation: resource.draft_generation,
          version: resource.version, published_versions: published, etag: resource.etag }
      } else if (phase === 'deploy') {
        assert.equal(body.resource_version_id, published[1].resource_version_id)
        advance()
        value = { schema_version: 1, deployment_id: id('adep', counter++), resource_id: agentId,
          resource_kind: 'agent', resource_version_id: body.resource_version_id, environment: body.environment, closure: body.closure, etag: '"deployment"' }
        if (options.driftBeforeActivation) advance()
      } else {
        resource.active_deployment_id = path.split('/').at(-1).split(':')[0]
        resource.gate_state = 'enabled'
        advance()
        value = observe()
      }
      receipts.set(key, { fingerprint, value: structuredClone(value) })
      if (drop === phase) {
        drop = null
        request.socket.destroy()
        return
      }
      send(200, value)
    } catch (error) {
      response.writeHead(500, { 'content-type': 'application/json' })
      response.end(JSON.stringify({ code: 'fixture_assertion', detail: error.message }))
    }
  })
  await new Promise(resolve => server.listen(0, '127.0.0.1', resolve))
  t.after(() => new Promise(resolve => server.close(resolve)))
  const client = Object.assign(new PlatformClient('http://127.0.0.1:' + server.address().port, ''), {
    prepareArtifactUpload: async (body, key) => {
      if (uploads.has(key)) return uploads.get(key)
      const artifactId = id('art', counter++)
      artifacts.set(artifactId, { artifact_id: artifactId, content_digest: body.expected_digest,
        byte_length: body.expected_size_bytes, media_type: body.declared_media_type, classification: body.classification, display_name: body.display_name })
      const result = { data: { artifact_id: artifactId, operation_id: id('job', counter++), artifact_etag: '"artifact-v1"',
        upload_target: { url: 'https://objects.example/upload', completion_proof: 'proof' } } }
      uploads.set(key, result)
      return result
    },
    putArtifactObject: async () => {},
    completeArtifactUpload: async () => ({ data: {} }),
    waitOperation: async () => ({ data: { state: 'succeeded', error: null } }),
    getArtifact: async artifactId => ({ data: { state: 'ready', content: artifacts.get(artifactId) } }),
  })
  return { client, memory, calls, observe, drift: () => { resource.active_deployment_id = id('adep', 999); advance() } }
}

test('actual HTTP publish binds Plan Artifact and retries the same receipt without storing source', async t => {
  const compiled = await compilation(), f = await fixture(t, { failPublish: true })
  await assert.rejects(publishCompiledAgent(f.client, compiled, null, () => {}), /unavailable/)
  const persisted = f.memory.get(handleKey)
  assert.equal(persisted.includes(compiled.canonicalManifest), false)
  assert.equal(persisted.includes('input.schema.json'), false)
  const result = await publishCompiledAgent(f.client, compiled, null, () => {})
  assert.equal(result.resource.active_deployment_id, result.deploymentId)
  const calls = f.calls.filter(call => call.phase === 'publish')
  assert.equal(calls.length, 2)
  assert.equal(calls[0].fingerprint, calls[1].fingerprint)
  assert.equal(calls[0].key, calls[1].key)
  assert.equal(f.memory.size, 0)
})

test('A to B to C to B creates separate publication commands for repeated source content', async t => {
  const f = await fixture(t)
  const variants = await Promise.all(['A', 'B', 'C', 'B'].map(name => compilation('Agent ' + name)))
  assert.equal(variants[1].sourceBundleDigest, variants[3].sourceBundleDigest)
  assert.notEqual(variants[1].sourceBundleDigest, variants[2].sourceBundleDigest)
  let current = null
  for (const variant of variants) {
    const result = await publishCompiledAgent(f.client, variant, current, () => {})
    current = result.resource
    assert.equal(f.observe().active_deployment_id, result.deploymentId)
    assert.equal(current.draft.document.spec.authoring_package.artifact.content_digest, variant.sourceBundleDigest)
  }
  const updates = f.calls.filter(call => call.phase === 'update')
  assert.equal(updates.length, 3)
  assert.notEqual(updates[0].key, updates[2].key)
  assert.equal(f.memory.size, 0)
})

for (const phase of ['update', 'activate']) {
  test(phase + ' response loss replays the persisted exact request and CAS', async t => {
    const compiled = await compilation(), f = await fixture(t, { drop: phase })
    if (phase === 'update') await publishCompiledAgent(f.client, compiled, null, () => {})
    const existing = phase === 'update' ? f.observe() : null
    await assert.rejects(publishCompiledAgent(f.client, compiled, existing, () => {}), /fetch failed/)
    const before = f.calls.filter(call => call.phase === phase).at(-1)
    const result = await publishCompiledAgent(f.client, compiled, phase === 'update' ? f.observe() : null, () => {})
    const after = f.calls.filter(call => call.phase === phase).at(-1)
    assert.equal(after.key, before.key)
    assert.equal(after.fingerprint, before.fingerprint)
    assert.equal(result.resource.active_deployment_id, result.deploymentId)
  })
}

test('a historical activation receipt cannot claim a different current head is ready', async t => {
  const compiled = await compilation(), f = await fixture(t, { drop: 'activate' })
  await assert.rejects(publishCompiledAgent(f.client, compiled, null, () => {}))
  f.drift()
  const stages = []
  await assert.rejects(publishCompiledAgent(f.client, compiled, null, stage => stages.push(stage)), /current active deployment differs/)
  assert.equal(stages.includes('ready'), false)
  assert.ok(f.memory.has(handleKey))
})

test('a concurrent Resource mutation after deployment cannot be swallowed by activation', async t => {
  const compiled = await compilation(), f = await fixture(t, { driftBeforeActivation: true })
  await assert.rejects(publishCompiledAgent(f.client, compiled, null, () => {}), /changed after this deployment/)
  assert.equal(f.calls.some(call => call.phase === 'activate'), false)
})

test('invalid, oversized or out of order handles fail before HTTP and preserve the pending intent', async t => {
  const compiled = await compilation(), f = await fixture(t, { failPublish: true })
  await assert.rejects(publishCompiledAgent(f.client, compiled, null, () => {}))
  const valid = f.memory.get(handleKey), before = f.calls.length
  for (const change of [
    value => ({ ...value, unknown: true }),
    value => ({ ...value, gateway_origin: 'https://different.example' }),
    value => ({ ...value, activation_etag: '"premature"' }),
    value => ({ ...value, source_bundle_digest: 'sha256:' + 'f'.repeat(64) }),
    value => ({ ...value, attempt_id: 'not-an-attempt' }),
    value => ({ ...value, agent_name: 'x'.repeat(17000) }),
  ]) {
    f.memory.set(handleKey, JSON.stringify(change(JSON.parse(valid))))
    await assert.rejects(publishCompiledAgent(f.client, compiled, null, () => {}), /publication_conflict/)
    assert.equal(f.calls.length, before)
    assert.ok(f.memory.has(handleKey))
  }
})

for (const drift of ['version', 'generation', 'document']) {
  test('validation cannot absorb a concurrent ' + drift + ' change', async t => {
    const compiled = await compilation(), f = await fixture(t, { driftDuringValidation: drift })
    await assert.rejects(publishCompiledAgent(f.client, compiled, null, () => {}), /draft changed during this validation/)
    assert.equal(f.calls.some(call => call.phase === 'publish'), false)
    assert.ok(f.memory.has(handleKey))
  })
}

test('mismatched current HTTP and body ETags cannot clear recovery or report ready', async t => {
  const compiled = await compilation(), f = await fixture(t, { invalidCurrentEtag: true })
  const stages = []
  await assert.rejects(publishCompiledAgent(f.client, compiled, null, stage => stages.push(stage)), /ETag differs/)
  assert.equal(stages.includes('ready'), false)
  assert.ok(f.memory.has(handleKey))
})
