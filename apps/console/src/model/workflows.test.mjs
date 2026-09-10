import '../../tests/wasm-worker-fixture.mjs'
import { INITIAL_QUOTA } from './quota.ts'
import test from 'node:test'
import assert from 'node:assert/strict'
import { createServer } from 'node:http'
import { PlatformClient } from '../api/client.ts'
import { digestJson } from '../agent/compiler.ts'
import { declarationBytes, publishModelConfiguration, resumeModelPublication } from './publication.ts'
import { importSourceCredential, pendingSourceCredential, finishSourceCredential } from './credentials.ts'
import { selectModelDefault, hasPendingModelDefault, resumeModelDefault } from './default.ts'
import { probeConnection, readModelCredential, revokeCredential, pendingCredentialRevocation } from './management.ts'

const publicationKey = 'insight.console.model-publication.v1'
const credentialKey = 'insight.console.model-credential.v1'
const id = (prefix, n) => `${prefix}_0198f1cc-32e4-75e1-a9e8-${n.toString(16).padStart(12, '0')}`
const sha = (n) => `sha256:${String(n).repeat(64)}`
const tenant = id('ten', 1), provider = id('spr', 2)
const exactModel = (n) => ({ resource_kind: 'model_deployment', deployment_id: id('mdep', n), deployment_digest: sha('c') })
function memory(t) {
  const storage = new Map()
  const previous = globalThis.sessionStorage
  globalThis.sessionStorage = { getItem: key => storage.get(key) ?? null, setItem: (key, value) => storage.set(key, String(value)), removeItem: key => storage.delete(key) }
  t.after(() => { if (previous === undefined) delete globalThis.sessionStorage; else globalThis.sessionStorage = previous })
  return storage
}

test('credential revoke resumes the original generation, version and Receipt after a lost response', async t => {
  const storage = memory(t), client = new PlatformClient('https://platform.example', 'session'), calls = []
  const original = { schema_version: 1, tenant_id: tenant, secret_binding_id: id('sbd', 3), provider_id: provider, purpose: 'model_api_key', state: 'active', generation: 7, version: 9, etag: `"${id('sbd', 3)}-9"` }
  const revoked = { ...original, state: 'revoked', generation: 8, version: 10, etag: `"${id('sbd', 3)}-10"` }
  let current = original
  t.mock.method(client, 'getModelCredential', async () => ({ data: current, etag: current.etag }))
  t.mock.method(client, 'revokeModelCredential', async (...args) => { calls.push(args); current = revoked; if (calls.length === 1) throw new Error('response lost'); return { data: revoked, etag: revoked.etag } })
  assert.deepEqual(await readModelCredential(client, tenant, original.secret_binding_id), original)
  await assert.rejects(revokeCredential(client, tenant, original), /lost/)
  assert.deepEqual(pendingCredentialRevocation(client, tenant).credential, original)
  assert.throws(() => pendingCredentialRevocation(client, id('ten', 8)), /another session/)
  await assert.rejects(revokeCredential(client, tenant, { ...original, secret_binding_id: id('sbd', 4), etag: `"${id('sbd', 4)}-9"` }), /pending/)
  assert.deepEqual(await revokeCredential(client, tenant, revoked), revoked)
  assert.deepEqual(calls[0], calls[1])
  assert.deepEqual(calls[0].slice(0, 3), [original.secret_binding_id, 7, original.etag])
  assert.equal(storage.size, 0)
  await revokeCredential(client, tenant, revoked)
  assert.equal(calls.length, 2)
})

test('probe is a single call with the exact deployment and rejects a changed target or invented outcome', async t => {
  const client = new PlatformClient('https://platform.example', 'session'), target = exactModel(20), calls = []
  let value = { schema_version: 1, model_deployment: target, provider_deployment: { resource_kind: 'model_provider_deployment', deployment_id: id('mpdep', 21), deployment_digest: sha('d') }, model_identity: { value: 'example-model', stability: 'externally_mutable' }, protocol: 'open_ai_responses', observed_at: '2026-09-09T10:00:00Z', outcome: 'timed_out' }
  t.mock.method(client, 'probeModel', async (...args) => { calls.push(args); return { data: value } })
  assert.deepEqual(await probeConnection(client, sha('a'), target), value)
  assert.equal(calls.length, 1)
  assert.deepEqual(calls[0], [sha('a'), target])
  value = { ...value, model_deployment: exactModel(22) }
  await assert.rejects(probeConnection(client, sha('a'), target), /Invalid connection observation/)
  value = { ...value, model_deployment: target, outcome: 'agent_qualified' }
  await assert.rejects(probeConnection(client, sha('a'), target), /Invalid connection observation/)
})
async function credential() {
  const policy = { kind: 'pinned', opaque_version_identity_digest: sha('a') }
  return { secret_binding_id: id('sbd', 3), binding_generation: 1, provider_id: provider, purpose: 'model_api_key', resolution_policy: policy, resolution_policy_digest: await digestJson(policy) }
}

test('credential response loss reuses exact operation, never persists the key, and refuses another tenant or source', async t => {
  const storage = memory(t), binding = await credential(), calls = []
  const client = new PlatformClient('https://platform.example', 'private-session')
  t.mock.method(client, 'importModelCredential', async (operation, providerId, key) => {
    calls.push([operation, providerId, key])
    if (calls.length === 1) throw new Error(`unsafe provider error: ${key}`)
    return { data: { schema_version: 1, binding } }
  })
  const intent = { display_name: 'Regional source', tenant_id: tenant, provider_id: provider, alias: 'regional', destination_digest: sha('d'), resource_id: null, resource_etag: null }
  const secret = 'test-model-credential-not-for-storage'
  await assert.rejects(importSourceCredential(client, intent, secret), error => !error.message.includes(secret) && /pending/.test(error.message))
  const pending = JSON.parse(storage.get(credentialKey))
  assert.equal(JSON.stringify([...storage]).includes(secret), false)
  assert.deepEqual(pendingSourceCredential(client.origin, tenant).intent, intent)
  await assert.rejects(importSourceCredential(client, { ...intent, alias: 'different' }, secret), /pending/)
  assert.throws(() => pendingSourceCredential(client.origin, id('ten', 99)), /pending/)
  assert.deepEqual(await importSourceCredential(client, intent, secret), binding)
  assert.equal(calls.length, 2)
  assert.equal(calls[0][0], pending.operation_id)
  assert.equal(calls[1][0], pending.operation_id)
  assert.deepEqual(await importSourceCredential(client, intent, ''), binding)
  assert.equal(calls.length, 2)
  assert.equal(JSON.stringify([...storage]).includes(secret), false)
  finishSourceCredential()
  assert.equal(storage.size, 0)
})

test('default recovery retains original CAS and refuses concurrent drift after replay', async t => {
  memory(t)
  const client = new PlatformClient('https://platform.example', 'session'), calls = []
  const current = version => ({ schema_version: 1, tenant_id: tenant, version, etag: `"${tenant}-${version}"`, default_model: null })
  const selected = exactModel(20), other = exactModel(21)
  let value = current(4), lost = true
  t.mock.method(client, 'setModelDefault', async (body, etag, receipt) => {
    calls.push({ body, etag, receipt })
    if (lost) { lost = false; value = { ...current(5), default_model: selected }; throw new Error('response lost') }
    return { data: value, etag: value.etag }
  })
  t.mock.method(client, 'getModelDefault', async () => ({ data: value, etag: value.etag }))
  await assert.rejects(selectModelDefault(client, current(4), selected), /lost/)
  await assert.rejects(selectModelDefault(client, current(5), other), /pending/)
  assert.equal(hasPendingModelDefault(), true)
  assert.deepEqual(await resumeModelDefault(client, { ...current(5), default_model: selected }), value)
  assert.equal(hasPendingModelDefault(), false)
  assert.deepEqual(calls[0], calls[1])
  assert.equal(calls[1].etag, `"${tenant}-4"`)
  t.mock.method(client, 'getModelDefault', async () => ({ data: { ...current(6), default_model: other }, etag: current(6).etag }))
  await assert.rejects(selectModelDefault(client, current(5), selected), /differs/)
})

// The fixture owns Resource versions, CAS and Receipt replay independently. Only physical object
// PUT is stubbed; its byte/digest assertion uses the real shared Rust canonicalizer.
async function fixture(t, { drop = null, update = false, kind = 'source', deferUpload = false, prepareDrift = null } = {}) {
  const storage = memory(t), receipts = new Map(), calls = [], artifacts = new Map()
  const source = kind === 'source', noun = source ? 'model-providers' : 'models', resourceId = id(source ? 'mpr' : 'mdl', 10)
  let resource = update ? { schema_version: 1, resource_id: resourceId, resource_kind: source ? 'model_provider' : 'model_profile', lifecycle_state: 'active', gate_state: 'enabled', active_deployment_id: null, version: 7, draft_generation: 2, etag: `"${resourceId}-7"`, draft: { alias: 'primary', display_name: 'Old', document: {}, validation: null } } : null
  const existing = structuredClone(resource)
  let deployment = null, artifactId = null, declaration = null, compiled = null, counter = 40, quotaAllocation = null
  const quota = () => ({ schema_version: 1, tenant_id: tenant, model_deployment: { resource_kind: 'model_deployment', deployment_id: deployment.deployment_id, deployment_digest: deployment.closure_digest }, allocation: quotaAllocation, tenant_concurrency: { limit: 8, reserved: 0, used: 0 }, etag: `"model-quota-${(quotaAllocation ? 'b' : 'a').repeat(64)}"` })
  const advance = () => { resource.version++; resource.etag = `"${resourceId}-${resource.version}"` }
  const server = createServer(async (request, response) => {
    try {
      const bytes = []
      for await (const chunk of request) bytes.push(chunk)
      const raw = Buffer.concat(bytes).toString(), body = raw ? JSON.parse(raw) : null, path = request.url
      const send = (status, value) => { response.writeHead(status, { 'content-type': 'application/json', ...(value?.etag ? { etag: value.etag } : {}) }); response.end(JSON.stringify(value)) }
      assert.equal(request.headers.authorization, 'Bearer session')
      if (path.startsWith('/v1/model-quotas/')) {
        assert.equal(path, `/v1/model-quotas/${deployment.deployment_id}`)
        if (request.method === 'GET') return send(200, quota())
        assert.equal(request.method, 'PUT')
        assert.deepEqual(body, { schema_version: 1, model_deployment: quota().model_deployment, limits: INITIAL_QUOTA })
        assert.equal(request.headers['if-match'], `"model-quota-${'a'.repeat(64)}"`)
        const receipt = request.headers['idempotency-key']
        if (receipts.has(receipt)) { assert.deepEqual(receipts.get(receipt), body); return send(200, quota()) }
        receipts.set(receipt, body); quotaAllocation = { limits: body.limits, reserved: { requests: 0, tokens: 0, cost_microunits: 0 }, used: { requests: 0, tokens: 0, cost_microunits: 0 } }
        if (drop === 'quota') { drop = null; response.destroy(); return }
        return send(200, quota())
      }
      if (request.method === 'GET') {
        if (path.startsWith('/v1/artifacts/')) {
          const artifact = artifacts.get(path.split('/').at(-1))
          if (drop === 'ready-get' && artifact.state === 'ready') { drop = null; response.destroy(); return }
          return send(200, artifact)
        }
        if (path.startsWith('/v1/operations/')) return send(200, { operation_id: path.split('/').at(-1), state: deferUpload ? 'running' : 'succeeded' })
        if (path.includes('/deployments/')) return send(200, deployment)
        assert.equal(path, `/v1/${noun}/${resourceId}`)
        return send(200, resource)
      }
      if (path === '/v1/model-configuration:declare') {
        assert.equal(body.installation_digest, sha('b'))
        const content = { schema_version: 1, input: body.input }
        declaration = { schema_version: 1, content, content_digest: await digestJson(content), size_bytes: 0 }
        // This fixture's content contains only integer/string metadata, so sorted JSON is exact JCS.
        const canonical = value => Array.isArray(value) ? value.map(canonical) : value && typeof value === 'object' ? Object.fromEntries(Object.keys(value).sort().map(key => [key, canonical(value[key])])) : value
        declaration.size_bytes = Buffer.byteLength(JSON.stringify(canonical(content)))
        return send(200, declaration)
      }
      if (path === '/v1/model-configuration:compile') {
        assert.equal(body.artifact.content_digest, declaration.content_digest)
        compiled = { schema_version: 1, environment: 'development', declaration, draft: { alias: 'primary', display_name: 'Primary', document: { kind: source ? 'model_provider' : 'model_profile', declaration_digest: declaration.content_digest } }, deployment: { resource_kind: source ? 'model_provider' : 'model_profile', bindings: { fixed_policy_digest: sha('d') } } }
        return send(200, compiled)
      }
      const key = request.headers['idempotency-key'], fingerprint = JSON.stringify([request.method, path, raw, request.headers['if-match'] ?? null])
      const phase = path === '/v1/artifacts:prepare-upload' ? 'prepare' : path.includes(':complete-upload') ? 'complete' : path.endsWith(':activate') ? 'activate' : path.endsWith('/deployments') ? 'deploy' : path.endsWith(':publish') ? 'publish' : path.endsWith(':validate') ? 'validate' : update ? 'update' : 'create'
      calls.push({ phase, key, fingerprint })
      if (receipts.has(key)) {
        const saved = receipts.get(key)
        if (phase === 'prepare' && artifacts.get(saved.value.artifact_id)?.state !== 'staging') {
          return send(409, { code: 'conflict', title: 'Terminal upload preparation cannot be replayed.' })
        }
        assert.equal(saved.fingerprint, fingerprint)
        if (phase === 'prepare' && prepareDrift) {
          const changed = structuredClone(saved.value)
          if (prepareDrift === 'expiry') changed.upload_expires_at = new Date(Date.parse(changed.upload_expires_at) + 1000).toISOString().replace(/(\.\d{3})Z$/, '$1000Z')
          else changed[prepareDrift] = prepareDrift === 'artifact_etag' ? `"${changed.artifact_id}-2"` : id({ artifact_id: 'art', operation_id: 'job', upload_grant_id: 'grt' }[prepareDrift], 999)
          return send(saved.status, changed)
        }
        return send(saved.status, saved.value)
      }
      if (!['prepare', 'complete', 'create'].includes(phase)) assert.equal(request.headers['if-match'], resource.etag)
      let value, status = 200
      if (phase === 'prepare') {
        artifactId = id('art', counter++)
        artifacts.set(artifactId, { schema_version: 1, artifact_id: artifactId, purpose: body.purpose, classification: body.classification, expected_size_bytes: body.expected_size_bytes,
          declared_media_type: body.declared_media_type, version: 1, etag: `"${artifactId}-1"`, state: 'staging', content: null })
        value = { schema_version: 1, artifact_id: artifactId, artifact_etag: `"${artifactId}-1"`, operation_id: id('job', counter++), upload_grant_id: id('grt', counter++), upload_expires_at: new Date(Date.now() + 120_000).toISOString().replace(/(\.\d{3})Z$/, '$1000Z'), upload_target: { url: 'https://objects.example/exact', completion_proof: 'private-upload-proof' } }
      } else if (phase === 'complete') {
        const content = { artifact_id: artifactId, content_digest: declaration.content_digest, byte_length: declaration.size_bytes, media_type: 'application/json', classification: 'internal', display_name: null }
        artifacts.set(artifactId, { ...artifacts.get(artifactId), version: 4, etag: `"${artifactId}-4"`, state: deferUpload ? 'uploaded' : 'ready', content }); value = {}
      } else if (phase === 'create' || phase === 'update') {
        assert.deepEqual(body, compiled.draft)
        if (!resource) resource = { schema_version: 1, resource_id: resourceId, resource_kind: source ? 'model_provider' : 'model_profile', lifecycle_state: 'active', gate_state: 'suspended', active_deployment_id: null, version: 0, draft_generation: 0 }
        resource.draft = { ...body, validation: null }; resource.draft_generation++; advance(); value = resource; status = update ? 200 : 201
      } else if (phase === 'validate') {
        status = 202; resource.draft.validation = { valid: true }; advance(); value = { operation_id: id('job', counter++) }
      } else if (phase === 'publish') {
        assert.equal(body.revision_no, resource.draft_generation)
        status = 201; advance(); value = { schema_version: 1, resource_id: resourceId, resource_kind: resource.resource_kind, version: resource.version, etag: resource.etag, published_versions: [{ resource_version_id: id(source ? 'mprev' : 'mdrev', counter++), revision_no: resource.draft_generation, content_digest: declaration.content_digest, artifact_id: artifactId, etag: '"revision"' }] }
      } else if (phase === 'deploy') {
        const deploymentId = id(source ? 'mpdep' : 'mdep', counter++), digest = await digestJson({ schema_version: 1, ...body.closure })
        status = 201; advance(); deployment = { schema_version: 1, resource_id: resourceId, resource_kind: resource.resource_kind, resource_version_id: body.resource_version_id, deployment_id: deploymentId, environment: body.environment, closure: body.closure, closure_digest: digest, created_at: '2026-09-09T00:00:00.000000Z', etag: `"${deploymentId}-${digest.slice(7)}"` }; value = deployment
      } else {
        resource.gate_state = 'enabled'; resource.active_deployment_id = deployment.deployment_id; advance(); value = resource
      }
      receipts.set(key, { fingerprint, value: structuredClone(value), status })
      if (phase === drop) { drop = null; response.destroy(); return }
      send(status, value)
    } catch (error) { response.writeHead(500); response.end(JSON.stringify({ code: 'fixture_failed', detail: error.message })); }
  })
  await new Promise(resolve => server.listen(0, '127.0.0.1', resolve))
  t.after(() => { server.closeAllConnections(); return new Promise(resolve => server.close(resolve)) })
  const client = new PlatformClient(`http://127.0.0.1:${server.address().port}`, 'session')
  t.mock.method(client, 'putArtifactObject', async (_target, bytes) => {
    assert.deepEqual(bytes, await declarationBytes(declaration))
    if (drop === 'object-put') { drop = null; throw new Error('object PUT outcome unknown') }
    const saved = JSON.parse(storage.get(publicationKey))
    assert.equal(saved.upload.artifact_id, artifactId)
    assert.equal(saved.upload.uploaded, false)
    assert.equal(JSON.stringify(saved).includes('private-upload-proof'), false)
    assert.equal(JSON.stringify(saved).includes('https://objects.example/exact'), false)
  })
  const configuration = source ? { schema_version: 1, alias: 'primary', display_name: 'Primary', destination_digest: sha('d'), credential: await credential() } : { schema_version: 1, alias: 'primary', display_name: 'Primary', source: { deployment_id: id('mpdep', 5), deployment_digest: sha('e'), resource_kind: 'model_provider_deployment' }, model: 'fixture-model', maximum_input_tokens: 8192, maximum_output_tokens: 1024, declared_at: '2026-09-09T00:00:00.000000Z' }
  return { client, storage, calls, request: { tenant_id: tenant, installation_digest: sha('b'), input: { kind, configuration }, quota: source ? null : { ...INITIAL_QUOTA }, existing }, change: () => advance(), current: () => structuredClone(resource), releaseUpload: () => { deferUpload = false; if (artifactId) artifacts.get(artifactId).state = 'ready' } }
}

test('model source and profile publish actual declaration bytes through all public lifecycle stages', async t => {
  for (const kind of ['source', 'model']) await t.test(kind, async t => {
    const f = await fixture(t, { kind })
    const result = await publishModelConfiguration(f.client, f.request, () => {})
    assert.equal(result.resource.version, 5)
    assert.equal(result.resource.active_deployment_id, result.deployment.deployment_id)
    assert.equal(f.storage.has(publicationKey), false)
    assert.deepEqual(f.calls.map(call => call.phase), ['prepare', 'complete', 'create', 'validate', 'publish', 'deploy', 'activate'])
  })
})

test('lost publication responses recover through exact readback or the original Receipt and CAS', async t => {
  for (const phase of ['prepare', 'complete', 'update', 'validate', 'publish', 'deploy', 'activate']) await t.test(phase, async t => {
    const f = await fixture(t, { drop: phase, update: true })
    await assert.rejects(publishModelConfiguration(f.client, f.request, () => {}))
    const result = await resumeModelPublication(f.client, tenant, sha('b'), () => {})
    assert.equal(result.resource.version, 12)
    const repeated = f.calls.filter(call => call.phase === phase)
    if (phase === 'complete') assert.equal(repeated.length, 1) // Ready Artifact readback resolves the uncertainty.
    else { assert.equal(repeated.length, 2); assert.deepEqual(repeated[0], repeated[1]) }
    assert.equal(f.storage.has(publicationKey), false)
  })
})

test('pending publication cannot accept a changed tenant, original CAS, declaration or post-activation drift', async t => {
  const f = await fixture(t, { drop: 'activate', update: true })
  await assert.rejects(publishModelConfiguration(f.client, f.request, () => {}))
  await assert.rejects(resumeModelPublication(f.client, id('ten', 99), sha('b'), () => {}), /conflict/)
  const saved = f.storage.get(publicationKey), handle = JSON.parse(saved)
  handle.existing.etag = `"${handle.existing.id}-9"`; handle.existing.version = 9
  f.storage.set(publicationKey, JSON.stringify(handle))
  await assert.rejects(resumeModelPublication(f.client, tenant, sha('b'), () => {}), /conflict/)
  f.storage.set(publicationKey, saved)
  f.change()
  await assert.rejects(resumeModelPublication(f.client, tenant, sha('b'), () => {}), /conflict/)
  assert.equal(f.storage.has(publicationKey), true)
})


test('uploaded and Ready artifacts recover after wait timeout or final read loss without terminal prepare replay', async t => {
  for (const failure of ['wait-timeout', 'ready-get']) await t.test(failure, async t => {
    const f = await fixture(t, { drop: failure === 'ready-get' ? 'ready-get' : null, deferUpload: failure === 'wait-timeout' })
    if (failure === 'wait-timeout') {
      const previous = globalThis.window; globalThis.window = { setTimeout }
      t.after(() => { if (previous === undefined) delete globalThis.window; else globalThis.window = previous })
      const wait = f.client.waitOperation.bind(f.client)
      t.mock.method(f.client, 'waitOperation', operation => wait(operation, 5))
    }
    await assert.rejects(publishModelConfiguration(f.client, f.request, () => {}))
    const saved = JSON.parse(f.storage.get(publicationKey))
    assert.ok(saved.upload); assert.equal(saved.artifact, null)
    f.releaseUpload()
    const result = await resumeModelPublication(f.client, tenant, sha('b'), () => {})
    assert.equal(result.resource.active_deployment_id, result.deployment.deployment_id)
    assert.equal(f.calls.filter(call => call.phase === 'prepare').length, 1)
    assert.equal(f.calls.filter(call => call.phase === 'complete').length, 1)
    assert.equal(f.storage.has(publicationKey), false)
  })
})


test('staging recovery refuses changed Artifact, Operation, Grant, CAS and original deadline before another PUT', async t => {
  for (const prepareDrift of ['artifact_id', 'operation_id', 'upload_grant_id', 'artifact_etag', 'expiry']) await t.test(prepareDrift, async t => {
    const f = await fixture(t, { drop: 'object-put', prepareDrift })
    await assert.rejects(publishModelConfiguration(f.client, f.request, () => {}), /PUT outcome unknown/)
    const original = f.storage.get(publicationKey)
    await assert.rejects(resumeModelPublication(f.client, tenant, sha('b'), () => {}), /artifact_upload_conflict/)
    assert.equal(f.storage.get(publicationKey), original)
    assert.equal(f.client.putArtifactObject.mock.callCount(), 1)
    assert.equal(f.calls.filter(call => call.phase === 'complete').length, 0)
  })
})

test('a model handle without the required upload metadata is retained and rejected before HTTP', async t => {
  const f = await fixture(t, { drop: 'object-put' })
  await assert.rejects(publishModelConfiguration(f.client, f.request, () => {}))
  const saved = JSON.parse(f.storage.get(publicationKey)); delete saved.upload
  const original = JSON.stringify(saved); f.storage.set(publicationKey, original)
  const calls = f.calls.length
  await assert.rejects(resumeModelPublication(f.client, tenant, sha('b'), () => {}), /conflict/)
  assert.equal(f.calls.length, calls); assert.equal(f.storage.get(publicationKey), original)
})


test('publication retains frozen quota and resumes its original allocation after activation', async t => {
  const f = await fixture(t, { kind: 'model', drop: 'quota' })
  await assert.rejects(publishModelConfiguration(f.client, f.request, () => {}))
  const raw = f.storage.get(publicationKey), pending = JSON.parse(raw)
  assert.equal(pending.deployment.deployment_id, pending.quota_intent.model_deployment.deployment_id)
  assert.deepEqual(pending.quota, INITIAL_QUOTA)
  await assert.rejects(publishModelConfiguration(f.client, { ...f.request, quota: { ...INITIAL_QUOTA, requests: 30 } }, () => {}), /conflict/)
  assert.equal(f.storage.get(publicationKey), raw)
  const result = await resumeModelPublication(f.client, tenant, sha('b'), () => {})
  assert.equal(result.deployment.deployment_id, pending.deployment.deployment_id)
  assert.equal(f.storage.has(publicationKey), false)
})
