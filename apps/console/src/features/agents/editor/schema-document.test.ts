import test from 'node:test'
import assert from 'node:assert/strict'
import { changeField, fieldTableSupported, newField, parseSchema } from './schema-document.ts'
import { buildFormManifest, updateFormManifest } from '../editor.ts'
import type { AgentFormFields } from '../editor.ts'
import { parseDocument } from 'yaml'

test('editing a field preserves unrelated constraints, nested schemas and the original object', () => {
  const original = parseSchema(
    JSON.stringify({
      type: 'object',
      properties: {
        text: { type: 'string', pattern: '^x', default: 'x', 'x-custom': { keep: true } },
        details: newField('object'),
      },
      required: ['text'],
      additionalProperties: false,
      $defs: { saved: { const: 7 } },
      'x-envelope': true,
    }),
  )
  const field = (original.properties as Record<string, Record<string, unknown>>).text
  const next = changeField(original, 'text', 'answer', { ...field, description: '回答' }, true)
  assert.deepEqual(next.$defs, original.$defs)
  assert.deepEqual(next.required, ['answer'])
  assert.deepEqual((next.properties as Record<string, unknown>).answer, {
    ...field,
    description: '回答',
  })
  assert.equal(next['x-envelope'], true)
  assert.ok(Object.hasOwn(original.properties as object, 'text'))
  assert.throws(() => changeField(original, 'text', 'details', field, true), /重复/)
})
test('complex roots and duplicate properties require advanced source editing without data conversion', () => {
  assert.equal(fieldTableSupported({ type: 'object', properties: {}, oneOf: [{}] }), false)
  assert.throws(() => parseSchema('{"type":"object","type":"string"}'))
  const schema = { type: 'object', properties: {}, required: [], additionalProperties: false }
  const next = changeField(schema, '', '__proto__', newField(), true)
  assert.ok(Object.hasOwn(next.properties as object, '__proto__'))
  assert.equal(Object.getPrototypeOf(next), Object.prototype)
})
test('form patches retain YAML comments and untouched constraints when optional mappings were null', () => {
  const fields: AgentFormFields = {
    name: 'hello',
    displayName: '你好',
    executionKind: 'deterministic',
    instructions: '',
    modelAlias: '',
    classification: 'internal',
    deadline: '',
    environment: '',
    inputSchemaPath: 'input.json',
    outputSchemaPath: 'output.json',
    planPath: 'plan.json',
  }
  const original = '# keep this source note\n' + buildFormManifest(fields)
  const next = updateFormManifest(original, {
    ...fields,
    executionKind: 'model_chat',
    modelAlias: 'my-model',
    instructions: '请回答',
    deadline: '120',
  })
  assert.ok(next.startsWith('# keep this source note'))
  const document = parseDocument(next)
  assert.equal(document.getIn(['spec', 'model', 'ref']), 'my-model')
  assert.equal(document.getIn(['spec', 'limits', 'deadlineSeconds']), 120)
  assert.equal(document.getIn(['spec', 'input', 'classification']), 'internal')
  assert.throws(() => updateFormManifest('spec: [', fields), /YAML/)
})

test('new table fields carry every bound required by the owning Rust schema compiler', async () => {
  await import('../../../../tests/wasm-worker-fixture.ts')
  const { readFileSync } = await import('node:fs')
  const { compileAgentManifest } = await import('../../../shared/compiler/compiler.ts')
  const corpus = JSON.parse(
    readFileSync(
      new URL(
        '../../../../../../contracts/product-experience/agent-compiler/v2/corpus.json',
        import.meta.url,
      ),
      'utf8',
    ),
  )
  const manifest = buildFormManifest({
    name: 'table-defaults',
    displayName: '字段测试',
    executionKind: 'deterministic',
    instructions: '',
    modelAlias: '',
    classification: 'internal',
    deadline: '',
    environment: '',
    inputSchemaPath: 'input.json',
    outputSchemaPath: 'output.json',
    planPath: 'plan.json',
  })
  for (const type of ['string', 'number', 'integer', 'boolean', 'null', 'object', 'array']) {
    const source = JSON.stringify({
      $schema: 'https://json-schema.org/draft/2020-12/schema',
      type: 'object',
      properties: { field: newField(type) },
      required: [],
      additionalProperties: false,
    })
    const compiled = await compileAgentManifest({
      manifest,
      inputSchema: source,
      outputSchema: source,
      profile: corpus.profile,
      bindings: { model: null },
    })
    assert.equal(compiled.name, 'table-defaults', type)
  }
})
