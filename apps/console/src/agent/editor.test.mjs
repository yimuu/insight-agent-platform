import '../../tests/wasm-worker-fixture.mjs'
import assert from 'node:assert/strict'
import { readFile } from 'node:fs/promises'
import test from 'node:test'
import { AgentCompilerError, compileAgentManifest, inspectAgentManifest } from './compiler.ts'
import { buildFormManifest, exactSlotBindings, manifestFormFields, planNodeOutline, readEditableSourceBundle } from './editor.ts'

const corpusRoot = new URL('../../../../contracts/product-experience/agent-compiler/v2/', import.meta.url)
const source = (path) => readFile(new URL(path, corpusRoot), 'utf8')
async function seed() {
  const corpus = JSON.parse(await source('corpus.json'))
  const input = { manifest: await source('deterministic.yaml'), inputSchema: await source('schema-message.json'), outputSchema: await source('schema-message.json'), profile: corpus.profile, bindings: { model: null, slots: [] } }
  const compiled = await compileAgentManifest(input)
  const fields = await manifestFormFields(compiled.canonicalManifest)
  return { input: { ...input, manifest: buildFormManifest({ ...fields, executionKind: 'full_plan', planPath: 'plans/flow.json' }), plan: compiled.typedPlan }, compiled }
}

test('Form/YAML preserve Full Plan kind, instructions, and referenced source paths through Rust inspection', async () => {
  const { input } = await seed()
  const inspection = await inspectAgentManifest(input.manifest)
  assert.equal(inspection.executionKind, 'full_plan')
  assert.equal(inspection.planPath, 'plans/flow.json')
  const fields = await manifestFormFields(input.manifest)
  assert.equal(fields.planPath, inspection.planPath)
  assert.equal(fields.inputSchemaPath, inspection.inputSchemaPath)
  assert.equal((await manifestFormFields(buildFormManifest(fields))).executionKind, 'full_plan')
})

test('a Full Plan compiles unchanged through actual WASM and its complete source bundle imports without loss', async () => {
  const { input, compiled: original } = await seed()
  const compiled = await compileAgentManifest(input)
  assert.equal(compiled.executionKind, 'full_plan')
  assert.equal(JSON.parse(compiled.typedPlan).plan_version, 6)
  assert.equal(compiled.typedPlanDigest, original.typedPlanDigest)
  const imported = await readEditableSourceBundle(compiled.sourceBundle)
  assert.equal(imported.plan, input.plan)
  assert.equal(imported.fields.executionKind, 'full_plan')
  assert.equal(imported.fields.planPath, 'plans/flow.json')
  assert.equal(imported.inputSchema, input.inputSchema)
  assert.equal(imported.outputSchema, input.outputSchema)
  const recompiled = await compileAgentManifest({ ...input, manifest: buildFormManifest(imported.fields), plan: imported.plan, bindings: { model: null, slots: exactSlotBindings(imported.slotBindings) } })
  assert.equal(recompiled.typedPlanDigest, compiled.typedPlanDigest)
})

test('unsupported nodes and exact slot structures reach Rust errors without a template fallback', async () => {
  const { input } = await seed()
  const plan = JSON.parse(input.plan)
  plan.nodes[plan.entry_node_id].kind = 'not_a_supported_node'
  assert.equal(planNodeOutline(JSON.stringify(plan)).nodes.find((node) => node.entry).kind, 'not_a_supported_node')
  await assert.rejects(compileAgentManifest({ ...input, plan: JSON.stringify(plan) }), (error) => error instanceof AgentCompilerError && error.code === 'agent_compile_failed')
  const slots = exactSlotBindings('[{"slot_id":"unbound","target":{"kind":"future_backend"}}]')
  await assert.rejects(compileAgentManifest({ ...input, bindings: { model: null, slots } }), (error) => error instanceof AgentCompilerError)
  await assert.rejects(manifestFormFields(input.manifest.replace('full_plan', 'future_template')), (error) => error instanceof AgentCompilerError)
})

test('editor JSON rejects duplicate slot keys, unbounded input, and malformed containers', () => {
  assert.throws(() => exactSlotBindings('[{"slot_id":"first","slot_id":"second"}]'), /duplicate keys/)
  assert.throws(() => exactSlotBindings('{}'), /JSON array/)
  assert.throws(() => exactSlotBindings('['), /complete JSON/)
  assert.throws(() => exactSlotBindings(`['${'x'.repeat(1_048_576)}']`), /byte limit/)
  assert.equal(planNodeOutline('{'), null)
  const nodes = Object.fromEntries(Array.from({ length: 129 }, (_, index) => [String(index), { kind: 'return' }]))
  assert.equal(planNodeOutline(JSON.stringify({ nodes })).nodes.length, 128)
  assert.equal(planNodeOutline(JSON.stringify({ nodes })).truncated, true)
})

test('bundle import does not discard missing, unreferenced, or unsupported source data', async () => {
  const { input } = await seed()
  const compiled = await compileAgentManifest(input)
  const bundle = JSON.parse(compiled.sourceBundle)
  delete bundle.sources.files['plans/flow.json']
  await assert.rejects(readEditableSourceBundle(JSON.stringify(bundle)), /missing plans\/flow.json/)
  bundle.sources.files['unreferenced.txt'] = 'must not disappear'
  await assert.rejects(readEditableSourceBundle(JSON.stringify(bundle)), /unreferenced files/)
  bundle.schema_version = 99
  await assert.rejects(readEditableSourceBundle(JSON.stringify(bundle)), /supported complete Agent source bundle/)
})

test('framework graph keeps the static export as source and compiles at Platform node granularity through WASM', async () => {
  const { createHash } = await import('node:crypto')
  const { input, compiled } = await seed()
  const plan = JSON.parse(compiled.typedPlan)
  const descriptor = { adapter: 'langgraph-static-typed-ports', semantic_version: 1, state: 'single_assignment_exact_ports', checkpoint_owner: 'platform_run', ir_abi: 6 }
  const adapterDigest = `sha256:${createHash('sha256').update(JSON.stringify(descriptor, Object.keys(descriptor).sort())).digest('hex')}`
  const graph = { schema_version: 1, dialect: 'lang_graph_static_typed_ports_v1', adapter_semantic_identity: adapterDigest, entry_node_id: plan.entry_node_id, nodes: plan.nodes, dependency_slots: plan.dependency_slots, schema_documents: plan.schema_documents }
  const manifest = buildFormManifest({ ...await manifestFormFields(input.manifest), executionKind: 'framework_graph', planPath: 'framework.json' })
  const source = JSON.stringify(graph)
  const result = await compileAgentManifest({ ...input, manifest, plan: source })
  assert.equal(result.executionKind, 'framework_graph')
  assert.equal(JSON.parse(result.typedPlan).plan_version, 6)
  assert.equal(result.typedPlanDigest, compiled.typedPlanDigest)
  const editable = await readEditableSourceBundle(result.sourceBundle)
  assert.equal(editable.plan, source)
  assert.equal(editable.fields.executionKind, 'framework_graph')
  assert.equal(editable.fields.planPath, 'framework.json')
  await assert.rejects(compileAgentManifest({ ...input, manifest, plan: 'def graph(): pass' }), AgentCompilerError)
  await assert.rejects(compileAgentManifest({ ...input, manifest, plan: JSON.stringify({ ...graph, reducers: {} }) }), AgentCompilerError)
})
