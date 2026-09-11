import '../../../tests/wasm-worker-fixture.ts'
import assert from 'node:assert/strict'
import { readFile } from 'node:fs/promises'
import { createHash } from 'node:crypto'
import test from 'node:test'
import { draftFields, readNodeEditorDescriptor, sourceLocations } from './plan-editor.ts'
import { branchIndex } from '../../shared/schema/tree.ts'
import { compileAgentManifest, rebuildExpression } from '../../shared/compiler/compiler.ts'

const descriptor = readNodeEditorDescriptor(
  await readFile(
    new URL('../../../../../contracts/platform-v1/agent-node-editor.v1.json', import.meta.url),
    'utf8',
  ),
)
test('every owning node template has editable scalar, array, port and expression fields without a TypeScript node registry', () => {
  for (const entry of descriptor.nodes) {
    const value = structuredClone(entry.template)
    value.future_field = { keep: 'Rust must diagnose this exact input' }
    const fields = draftFields(descriptor, entry.template, value)
    assert.deepEqual(Object.keys(fields.properties).sort(), Object.keys(value).sort(), entry.kind)
    assert.ok(fields.properties.future_field)
    assert.deepEqual(value.future_field, { keep: 'Rust must diagnose this exact input' })
  }
  const port = draftFields(descriptor, descriptor.templates.port, descriptor.templates.output_port)
  assert.equal(branchIndex(port, descriptor.templates.output_port), 1)
  const instructions = draftFields(descriptor, [], [], 'instructions')
  assert.equal(instructions.items.alternatives.length, descriptor.choices.instruction.length)
  const branches = draftFields(descriptor, [], [], 'ordered_arms')
  assert.ok(branches.items.properties.when.properties.instructions)
  const carried = draftFields(descriptor, [], [], 'carried_ports')
  assert.ok(carried.items.properties.next_iteration_port.alternatives.length)
})

test('actual Rust source map bytes are digest exact and map selected Plan nodes to real source positions', async () => {
  const base = new URL(
    '../../../../../contracts/product-experience/agent-compiler/v2/',
    import.meta.url,
  )
  const read = (file) => readFile(new URL(file, base), 'utf8')
  const corpus = JSON.parse(await read('corpus.json'))
  const input = {
    manifest: await read('deterministic.yaml'),
    inputSchema: await read('schema-message.json'),
    outputSchema: await read('schema-message.json'),
    profile: corpus.profile,
    bindings: { model: null, slots: [] },
  }
  const echo = await compileAgentManifest(input)
  assert.equal(
    echo.sourceMapDigest,
    `sha256:${createHash('sha256').update(echo.sourceMap).digest('hex')}`,
  )
  const map = JSON.parse(echo.sourceMap)
  assert.equal(map.compiler_semantic_identity, descriptor.compiler_semantic_identity)
  assert.equal(map.typed_plan_digest, echo.typedPlanDigest)
  for (const id of Object.keys(JSON.parse(echo.typedPlan).nodes)) {
    const locations = sourceLocations(echo.sourceMap, id)
    assert.ok(locations.length, id)
    assert.ok(
      locations.every(
        (location) => location.file === 'agent.yaml' && location.line > 0 && location.column > 0,
      ),
    )
  }
})

test('actual WASM expression rebuild computes exact stack/digest and rejects invalid instruction programs', async () => {
  const draft = structuredClone(descriptor.templates.expression)
  assert.ok(draft && typeof draft === 'object' && !Array.isArray(draft))
  const rebuilt = await rebuildExpression({
    ...draft,
    maximum_stack_depth: 65535,
    semantic_digest: `sha256:${'f'.repeat(64)}`,
    unknown_authored_field: 'preserved for complete compile diagnostics',
  })
  assert.equal(rebuilt.maximum_stack_depth, 1)
  assert.notEqual(rebuilt.semantic_digest, draft.semantic_digest)
  assert.equal(rebuilt.unknown_authored_field, 'preserved for complete compile diagnostics')
  assert.deepEqual(await rebuildExpression(rebuilt), rebuilt)
  await assert.rejects(
    rebuildExpression({ ...draft, instructions: [{ op: 'boolean_and' }] }),
    /expression|instruction|stack/i,
  )
})
