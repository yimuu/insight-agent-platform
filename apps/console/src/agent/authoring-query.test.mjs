import assert from 'node:assert/strict'
import test from 'node:test'
import { PlatformClient } from '../api/client.ts'
import { parseBindingSelections, parseBindingResolution, parseDependencyPage, exactFeatureSelections, exactResolvedFeatures } from './authoring-query.ts'
const digest = (character) => `sha256:${character.repeat(64)}`
const deployment = { deployment_id: 'mdep_0198f1c3-8f49-7c3e-b1f3-773c28367b90', resource_kind: 'model_deployment', deployment_digest: digest('a') }
const policy = { deployment: { ...deployment, deployment_id: 'pdep_policy', resource_kind: 'policy_deployment' }, revision: { revision_id: 'prev_policy', resource_kind: 'policy_revision', semantic_digest: digest('b') } }
const selections = { schema_version: 1, slots: [{ slot_id: 'model', requirement_digest: digest('c'), interface_contract_digest: null, target: { kind: 'model', candidates: [{ kind: 'active', resource_id: 'mpr_model', environment: 'dev' }], selection_policy: policy } }] }
const binding = { slot_id: 'model', requirement_digest: digest('c'), target: { kind: 'model', candidates: [deployment], selection_policy: policy } }
const resolved = { schema_version: 1, slots: [{ slot_id: 'model', resolution: { kind: 'resolved', deployment_features: [], binding, observed_contract_digests: [digest('d')], contract_match: null, call_authorized: false } }] }

test('authoring query POST is a cancellable read with no receipt or mutation preconditions', async (context) => {
  let call
  context.mock.method(globalThis, 'fetch', async (url, init) => { call = { url, init }; return new Response(JSON.stringify(resolved)) })
  const client = new PlatformClient('https://gateway.example', 'token')
  const controller = new AbortController()
  const response = await client.resolveAgentBindings(parseBindingSelections(JSON.stringify(selections)), { signal: controller.signal })
  assert.equal(call.url, 'https://gateway.example/v1/agent-authoring-bindings:resolve')
  assert.equal(call.init.method, 'POST')
  assert.equal(call.init.signal, controller.signal)
  assert.equal(call.init.headers.has('idempotency-key'), false)
  assert.equal(call.init.headers.has('if-match'), false)
  assert.deepEqual(JSON.parse(call.init.body), selections)
  assert.equal(response.data.slots[0].resolution.call_authorized, false)
  assert.equal(response.data.slots[0].resolution.contract_match, null)
  assert.deepEqual(response.data.slots[0].resolution.binding, binding)
})

test('discovery preserves empty continuations and keeps compatibility independent from call permission', async (context) => {
  const item = { schema_version: 1, kind: 'model', resource_id: 'mpr_model', environment: 'dev', deployment, interface_contract_digest: digest('d'), contract_match: false, call_authorized: true }
  const page = { schema_version: 1, items: [item], next_cursor: 'opaque+/next==' }
  const filters = { kind: 'model', environment: 'dev', interfaceContractDigest: digest('e'), cursor: 'opaque+/old==' }
  let url
  context.mock.method(globalThis, 'fetch', async (input) => { url = new URL(input); return new Response(JSON.stringify({ ...page, items: [] })) })
  const client = new PlatformClient('https://gateway.example', 'token')
  const result = await client.listAuthoringDependencies(filters)
  assert.equal(result.data.next_cursor, page.next_cursor)
  assert.equal(url.searchParams.get('interface_contract_digest'), digest('e'))
  assert.equal(url.searchParams.get('cursor'), filters.cursor)
  const checked = parseDependencyPage(JSON.stringify(page), filters)
  assert.equal(checked.items[0].contract_match, false)
  assert.equal(checked.items[0].call_authorized, true)
  assert.throws(() => parseDependencyPage(JSON.stringify(page), { ...filters, interfaceContractDigest: digest('d') }), /mismatched response/)
})

test('strict dynamic authoring rejects duplicates, unknown fields, reordering and foreign binding identities', () => {
  assert.throws(() => parseBindingSelections(JSON.stringify(selections).replace('"slot_id":"model"', '"slot_id":"bad","slot_id":"model"')), /duplicate keys/)
  assert.throws(() => parseBindingSelections(JSON.stringify({ ...selections, mutation: false })), /unsupported shape/)
  assert.throws(() => parseBindingResolution(JSON.stringify(resolved).replace('"schema_version":1', '"schema_version":1,"schema_version":1'), selections), /duplicate keys/)
  const wrong = structuredClone(resolved); wrong.slots[0].resolution.binding.requirement_digest = digest('e')
  assert.throws(() => parseBindingResolution(JSON.stringify(wrong), selections), /mismatched response/)
  const swapped = structuredClone(resolved); swapped.slots[0].slot_id = 'other'
  assert.throws(() => parseBindingResolution(JSON.stringify(swapped), selections), /mismatched response/)
  const forged = structuredClone(resolved); forged.slots[0].resolution.binding.binding_digest = digest('a')
  assert.throws(() => parseBindingResolution(JSON.stringify(forged), selections), /unsupported shape/)
  const wrongCount = structuredClone(resolved); wrongCount.slots[0].resolution.binding.target.candidates.push(deployment)
  assert.throws(() => parseBindingResolution(JSON.stringify(wrongCount), selections), /mismatched response/)
  const oversized = { ...selections, slots: Array.from({ length: 65 }, (_, n) => ({ ...selections.slots[0], slot_id: `s${n}` })) }
  assert.throws(() => parseBindingSelections(JSON.stringify(oversized)), /unsupported shape/)
})

test('exact feature queries carry server projections without inferring a Capability backend',()=>{
  const cap={...deployment,deployment_id:'cdep_0198f1c3-8f49-7c3e-b1f3-773c28367b90',resource_kind:'capability_deployment'}
  const input={slot_id:'tool',requirement_digest:digest('e'),target:{kind:'capability',candidates:[cap],selection_policy:policy,tool_alias:null}}
  const request=exactFeatureSelections([input])
  assert.deepEqual(request.slots[0].target.candidates,[{kind:'exact',deployment:cap}])
  const evidence={schema_version:1,deployment:cap,interface_contract_digest:digest('d'),required_features:['remote-capability','mcp']}
  const result={schema_version:1,slots:[{slot_id:'tool',resolution:{kind:'resolved',binding:input,deployment_features:[evidence],observed_contract_digests:[digest('d')],contract_match:null,call_authorized:false}}]}
  assert.deepEqual(exactResolvedFeatures(parseBindingResolution(JSON.stringify(result),request)),[evidence])
  for(const alter of [v=>v.slots[0].resolution.deployment_features=[],v=>v.slots[0].resolution.deployment_features[0].interface_contract_digest=digest('f'),v=>v.slots[0].resolution.deployment_features[0].required_features=['mcp','remote-capability']]){
    const invalid=structuredClone(result);alter(invalid);assert.throws(()=>parseBindingResolution(JSON.stringify(invalid),request))
  }
})
