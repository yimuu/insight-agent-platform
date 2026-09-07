import '../../tests/wasm-worker-fixture.mjs'
import assert from 'node:assert/strict'
import { readFile } from 'node:fs/promises'
import test from 'node:test'
import { PlatformClient, PlatformProblem } from '../api/client.ts'
import { compileAgentManifest } from './compiler.ts'
import { materializeDocument } from './publication.ts'
import { restorePublishedSources } from './restore.ts'
import { publishedRunDefaults, frozenRunSources } from './published-run.ts'

const agent = 'agt_01950000-0000-7000-8000-000000000001'
const versionId = 'arev_01950000-0000-7000-8000-000000000002'
const authoringId = 'art_01950000-0000-7000-8000-000000000003'
const planId = 'art_01950000-0000-7000-8000-000000000004'
async function fixture() {
  const base = new URL('../../../../contracts/product-experience/agent-compiler/v2/', import.meta.url)
  const corpus = JSON.parse(await readFile(new URL('corpus.json', base), 'utf8'))
  const c = corpus.cases.find((c) => c.bindings.model !== null)
  const compiled = await compileAgentManifest({ manifest: await readFile(new URL(c.manifest, base), 'utf8'), manifestPath: 'agents/restored/agent.yaml', inputSchema: await readFile(new URL(c.input_schema, base), 'utf8'), outputSchema: await readFile(new URL(c.output_schema, base), 'utf8'), profile: corpus.profile, bindings: c.bindings })
  const ref = (id, digest, content) => ({ artifact_id: id, content_digest: digest, byte_length: Buffer.byteLength(content), media_type: 'application/json', classification: 'internal', display_name: null })
  const authoring = ref(authoringId, compiled.sourceBundleDigest, compiled.sourceBundle)
  const plan = ref(planId, compiled.typedPlanDigest, compiled.typedPlan)
  const etag = `"${versionId}-${compiled.typedPlanDigest.slice(7)}"`
  const version = { schema_version: 1, resource_id: agent, resource_kind: 'agent', resource_version_id: versionId, revision_no: 1, content_digest: compiled.typedPlanDigest, artifact_id: planId, payload: { document: materializeDocument(compiled, authoring, plan), validation: {} }, created_at: '2026-09-06T00:00:00.000000Z', etag }
  const current = { resource_id: agent, resource_kind: 'agent', etag: '"current-draft-CAS"', draft: { document: { resource_kind: 'agent', spec: { authoring_name: 'current-draft-different' } } } }
  return { compiled, authoring, plan, version, current, bindings: c.bindings }
}
function transport(context, fixture, alter = () => undefined) {
  const calls = []
  context.mock.method(globalThis, 'fetch', async (url, init) => {
    const path = new URL(url).pathname; calls.push({ path, init })
    const altered = alter(path, fixture); if (altered) return altered
    const json = (body, headers = {}) => new Response(JSON.stringify(body), { headers: { 'content-type': 'application/json', ...headers } })
    if (path === `/v1/agents/${agent}/versions/${versionId}`) return json(fixture.version, { etag: fixture.version.etag })
    if (path === `/v1/agents/${agent}`) return json(fixture.current, { etag: fixture.current.etag })
    if (path === `/v1/artifacts/${authoringId}`) return json({ artifact_id: authoringId, state: 'ready', purpose: 'authoring_document', content: fixture.authoring })
    if (path === `/v1/artifacts/${planId}`) return json({ artifact_id: planId, state: 'ready', purpose: 'typed_plan', content: fixture.plan })
    if (path === `/v1/artifacts/${authoringId}/content`) return new Response(fixture.compiled.sourceBundle, { headers: { 'content-length': String(fixture.authoring.byte_length), 'content-type': 'application/json', etag: `"${fixture.authoring.content_digest}"` } })
    throw new Error(`Unexpected fixture path ${path}`)
  })
  return calls
}

test('published restore uses exact authorized sources, original manifest path and frozen model; current draft only supplies edit CAS', async (context) => {
  const f = await fixture()
  const calls = transport(context, f)
  const controller = new AbortController()
  const result = await restorePublishedSources(new PlatformClient('https://platform.example', 'token'), agent, versionId, controller.signal)
  assert.equal(result.sources.manifestPath, 'agents/restored/agent.yaml')
  assert.equal(result.sources.manifest, JSON.parse(f.compiled.sourceBundle).sources.files['agents/restored/agent.yaml'])
  assert.deepEqual(result.sources.modelBinding, f.bindings.model)
  assert.deepEqual(result.current, f.current)
  assert.ok(calls.every(({ init }) => init.method === undefined && init.cache === 'no-store' && init.signal === controller.signal))
  assert.equal(calls.length, 5)
})

test('published restore stops on current content 403 before editor authority; no partial result', async (context) => {
  const f = await fixture()
  const calls = transport(context, f, (path) => path.endsWith('/content') ? new Response(JSON.stringify({ code: 'permission_denied', detail: 'Content read denied.', retryable: false }), { status: 403 }) : undefined)
  await assert.rejects(restorePublishedSources(new PlatformClient('https://platform.example', 'token'), agent, versionId), (error) => error instanceof PlatformProblem && error.status === 403)
  assert.equal(calls.length, 3)
  assert.ok(!calls.some(({ path }) => path === `/v1/agents/${agent}`))
})

test('published restore rejects altered bytes, exact version mismatch and complete document drift', async (context) => {
  const client = new PlatformClient('https://platform.example', 'token')
  for (const variant of ['bytes', 'identity', 'document', 'etag', 'plan']) {
    const f = await fixture()
    if (variant === 'identity') f.version.resource_version_id = 'arev_01950000-0000-7000-8000-000000000099'
    if (variant === 'document') f.version.payload.document.spec.author_instructions = 'silently changed'
    if (variant === 'etag') f.version.etag = '"different"'
    if (variant === 'plan') f.plan.content_digest = `sha256:${'f'.repeat(64)}`
    const calls = transport(context, f, (path) => variant === 'bytes' && path.endsWith('/content') ? new Response(f.compiled.sourceBundle.replace('model', 'mOdel'), { headers: { 'content-length': String(f.authoring.byte_length), 'content-type': 'application/json', etag: `"${f.authoring.content_digest}"` } }) : undefined)
    await assert.rejects(restorePublishedSources(client, agent, versionId), undefined, variant)
    assert.ok(!calls.some(({ path }) => path === `/v1/agents/${agent}`), variant)
    context.mock.restoreAll()
  }
})

function frozenDefinition(f) {
  const deploymentId='adep_01950000-0000-7000-8000-000000000005'
  const exact={deployment_id:deploymentId,resource_kind:'agent_deployment',deployment_digest:`sha256:${'a'.repeat(64)}`}
  const plan={revision_id:versionId,resource_kind:'agent_plan_revision',semantic_digest:f.compiled.typedPlanDigest}
  const contract={revision_id:'aif_01950000-0000-7000-8000-000000000006',resource_kind:'agent_interface_revision',semantic_digest:f.compiled.contractDigest}
  const definition={schema_version:1,run_id:'run_01950000-0000-7000-8000-000000000007',agent_id:agent,agent_deployment:exact,agent_interface:contract,plan}
  const deployment={schema_version:1,resource_id:agent,resource_kind:'agent',deployment_id:deploymentId,closure_digest:exact.deployment_digest,closure:{resource_kind:'agent',bindings:{plan,interface:contract}},etag:`"${deploymentId}-${exact.deployment_digest.slice(7)}"`}
  return {definition,deployment,summary:{agent_id:agent,active_deployment:exact},run:{run_id:definition.run_id,agent_deployment_id:deploymentId}}
}
test('Run defaults use the exact active published Plan while the editable draft differs',async context=>{
  const f=await fixture(), exact=frozenDefinition(f)
  const calls=transport(context,f,path=>path.endsWith(`/deployments/${exact.deployment.deployment_id}`)?new Response(JSON.stringify(exact.deployment),{headers:{etag:exact.deployment.etag}}):undefined)
  const defaults=await publishedRunDefaults(new PlatformClient('https://platform.example','token'),exact.summary)
  assert.equal(defaults.schemaDigest,f.version.payload.document.spec.input_schema.canonical_digest)
  assert.deepEqual(defaults.exactDeployment,exact.definition.agent_deployment)
  assert.equal(calls.length,2)
  assert.ok(!calls.some(({path})=>path===`/v1/agents/${agent}`))
})
test('Run source map is rebuilt from its exact frozen definition and current authorized Artifact',async context=>{
  const f=await fixture(), exact=frozenDefinition(f)
  const calls=transport(context,f,path=>path.endsWith('/definition')?new Response(JSON.stringify(exact.definition)):path.endsWith(`/deployments/${exact.deployment.deployment_id}`)?new Response(JSON.stringify(exact.deployment),{headers:{etag:exact.deployment.etag}}):undefined)
  const source=await frozenRunSources(new PlatformClient('https://platform.example','token'),exact.run)
  assert.equal(source.sourceMapDigest,f.compiled.sourceMapDigest)
  assert.equal(source.typedPlanDigest,exact.definition.plan.semantic_digest)
  assert.ok(calls.every(({init})=>init.cache==='no-store' && !init.headers.has('idempotency-key')))
})
test('Run source and defaults reject altered exact references and content revocation without alternate lookups',async context=>{
  for(const mode of ['head','plan','content','deployment_identity','etag']) {
    const f=await fixture(), exact=frozenDefinition(f)
    if(mode==='head') exact.deployment.closure_digest=`sha256:${'f'.repeat(64)}`
    if(mode==='plan') exact.definition.plan.semantic_digest=`sha256:${'f'.repeat(64)}`
    if(mode==='deployment_identity') exact.deployment.resource_kind='capability'
    if(mode==='etag') exact.deployment.etag='"wrong-exact-etag"'
    transport(context,f,path=>path.endsWith('/definition')?new Response(JSON.stringify(exact.definition)):path.endsWith(`/deployments/${exact.deployment.deployment_id}`)?new Response(JSON.stringify(exact.deployment),{headers:{etag:exact.deployment.etag}}):mode==='content'&&path.endsWith('/content')?new Response(JSON.stringify({code:'permission_denied',retryable:false}),{status:403}):undefined)
    const client=new PlatformClient('https://platform.example','token')
    await assert.rejects(mode==='head'?publishedRunDefaults(client,exact.summary):frozenRunSources(client,exact.run))
    context.mock.restoreAll()
  }
})
