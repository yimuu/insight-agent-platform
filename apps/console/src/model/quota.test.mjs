import test from 'node:test'
import assert from 'node:assert/strict'
import { PlatformClient } from '../api/client.ts'
import { saveModelQuota, resumeModelQuota, validQuotaView, validQuotaLimits, hasPendingModelQuota, INITIAL_QUOTA } from './quota.ts'
const id = (prefix, n) => `${prefix}_0198f1cc-32e4-75e1-a9e8-${String(n).padStart(12, '0')}`
const tenant = id('ten', 1), target = { resource_kind: 'model_deployment', deployment_id: id('mdep', 2), deployment_digest: `sha256:${'a'.repeat(64)}` }
const zero = { requests: 0, tokens: 0, cost_microunits: 0 }
const view = (letter, allocation = null) => ({ schema_version: 1, tenant_id: tenant, model_deployment: target, allocation, tenant_concurrency: { limit: 8, reserved: 0, used: 0 }, etag: `"model-quota-${letter.repeat(64)}"` })
function memory(t) {
  const values = new Map(), previous = globalThis.sessionStorage
  globalThis.sessionStorage = { getItem: key => values.get(key) ?? null, setItem: (key, value) => values.set(key, String(value)), removeItem: key => values.delete(key) }
  t.after(() => { if (previous === undefined) delete globalThis.sessionStorage; else globalThis.sessionStorage = previous })
  return values
}
test('quota loss replays exact original deployment, limits, CAS and Receipt while observing new usage', async t => {
  const storage = memory(t), client = new PlatformClient('https://platform.example', 'private-session'), calls = []
  const original = view('a'), allocated = view('b', { limits: INITIAL_QUOTA, reserved: zero, used: zero })
  let current = allocated
  t.mock.method(client, 'setModelQuota', async (...args) => { calls.push(args); if (calls.length === 1) throw new Error('response lost'); return { data: allocated, etag: allocated.etag } })
  t.mock.method(client, 'getModelQuota', async id => { assert.equal(id, target.deployment_id); return { data: current, etag: current.etag } })
  await assert.rejects(saveModelQuota(client, original, INITIAL_QUOTA), /lost/)
  assert.equal(hasPendingModelQuota(), true)
  const pending = [...storage]
  await assert.rejects(saveModelQuota(client, allocated, { ...INITIAL_QUOTA, requests: 30 }), /conflict/)
  await assert.rejects(resumeModelQuota(client, id('ten', 4)), /conflict/)
  await assert.rejects(resumeModelQuota(new PlatformClient('https://different.example', 'session'), tenant), /conflict/)
  assert.deepEqual([...storage], pending)
  current = view('c', { limits: INITIAL_QUOTA, reserved: zero, used: { ...zero, requests: 2 } })
  assert.deepEqual(await resumeModelQuota(client, tenant), current)
  assert.deepEqual(calls[0], calls[1]); assert.equal(calls[1][2], original.etag)
  assert.equal(hasPendingModelQuota(), false)
  assert.equal(JSON.stringify(pending).includes('private-session'), false)
  current = view('d', { limits: { ...INITIAL_QUOTA, requests: 30 }, reserved: zero, used: zero })
  await assert.rejects(saveModelQuota(client, allocated, INITIAL_QUOTA), /conflict/)
  assert.equal(hasPendingModelQuota(), true)
})
test('quota projections are closed, finite and internally consistent', () => {
  for (const invalid of [null, { ...INITIAL_QUOTA, requests: -1 }, { ...INITIAL_QUOTA, requests: 1.5 }, { ...INITIAL_QUOTA, requests: Number.MAX_SAFE_INTEGER + 1 }, { ...INITIAL_QUOTA, extra: 0 }, { requests: 1, tokens: 1 }]) assert.equal(validQuotaLimits(invalid), false)
  assert.equal(validQuotaLimits(zero), true)
  assert.equal(validQuotaView(view('a')), true)
  assert.equal(validQuotaView({ ...view('a'), etag: `W/${view('a').etag}` }), false)
  assert.equal(validQuotaView(view('b', { limits: zero, reserved: zero, used: { ...zero, requests: 1 } })), false)
  assert.equal(validQuotaView({ ...view('a'), tenant_concurrency: { limit: 8, reserved: 8, used: 1 } }), false)
  assert.equal(validQuotaView({ ...view('a'), allocation: { limits: INITIAL_QUOTA, used: zero } }), false)
})
