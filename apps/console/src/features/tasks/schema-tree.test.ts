import { objectValue } from '../../../tests/fixtures/types.ts'
import type { Json } from '../../shared/api/types.ts'
import assert from 'node:assert/strict'
import { readFile } from 'node:fs/promises'
import test from 'node:test'
import {
  branchIndex,
  initialValue,
  treeSchema,
  validateTree,
  valueBounds,
} from '../../shared/schema/tree.ts'

const string = { type: 'string', minLength: 0, maxLength: 65_536, 'x-platform-max-bytes': 65_536 }
const root = (properties): Record<string, Json> => ({
  type: 'object',
  properties,
  required: Object.keys(properties),
  additionalProperties: false,
})

test('typed fallback resolves local defs, tagged unions, null and opaque const data', () => {
  const literal = { $ref: '#/$defs/Literal', nested: { $id: 'application data' } }
  const schema = root({
    message: { $ref: '#/$defs/Message' },
    choice: {
      oneOf: [
        root({
          tag: { const: 'yes', type: 'string' },
          amount: { type: 'integer', minimum: 1, maximum: 10 },
        }),
        root({ tag: { const: 'no', type: 'string' } }),
      ],
    },
    maybe: { oneOf: [string, { type: 'null' }] },
    literal: { const: literal },
  })
  schema.$defs = { Message: string, Literal: { const: literal } }
  const node = treeSchema(schema)
  const value = { message: 'hello', choice: { tag: 'yes', amount: 3 }, maybe: null, literal }
  assert.deepEqual(validateTree(node, value), {})
  assert.equal(branchIndex(node.properties.choice, { tag: 'no' }), 1)
  assert.deepEqual(objectValue(initialValue(node)).literal, literal)
  assert.equal(
    validateTree(node, { ...value, choice: { tag: 'yes', amount: 0 } })['/choice/amount'],
    'Use a value of at least 1.',
  )
  assert.equal(validateTree(node, { ...value, message: undefined })['/message'], 'Expected string.')
})

test('typed fallback covers values beyond common controls without truncation and enforces input bounds', () => {
  const schema = treeSchema(
    root({
      text: string,
      rows: {
        type: 'array',
        minItems: 0,
        maxItems: 300,
        items: { type: 'integer', multipleOf: 2 },
      },
    }),
  )
  assert.deepEqual(
    validateTree(schema, {
      text: 'x'.repeat(9_000),
      rows: Array.from({ length: 200 }, (_, i) => i * 2),
    }),
    {},
  )
  assert.match(valueBounds({ text: 'x'.repeat(65_537) })[''], /65536/)
  assert.match(valueBounds({ rows: Array(4097).fill(1) })['/rows'], /4096/)
  assert.match(
    valueBounds(Object.fromEntries(Array.from({ length: 1025 }, (_, i) => [String(i), 0])))[''],
    /1024/,
  )
  let deep = null
  for (let i = 0; i < 34; i++) deep = { child: deep }
  assert.ok(Object.values(valueBounds(deep)).some((error) => error.includes('depth 32')))
  assert.ok(valueBounds({ number: Number.NaN })['/number'])
})

test('exact nominal registry resolves offline; unknown digests, external URLs and cycles fail closed', async () => {
  const base = new URL('../../../../../contracts/platform-v1/', import.meta.url)
  const registry = JSON.parse(await readFile(new URL('schemas/nominal-types.json', base), 'utf8'))
  const entries: [string, Record<string, Json>][] = await Promise.all(
    registry.schemas.map(async (entry) => [
      entry.pinned_reference,
      JSON.parse(await readFile(new URL(entry.path, base), 'utf8')),
    ]),
  )
  const nominals = new Map(entries)
  for (const [reference] of nominals)
    assert.doesNotThrow(() => treeSchema({ $ref: reference }, nominals), reference)
  const digest = entries.find(([ref]) => ref.includes(':Digest@'))[0]
  assert.deepEqual(
    validateTree(treeSchema(root({ digest: { $ref: digest } }), nominals), {
      digest: `sha256:${'a'.repeat(64)}`,
    }),
    {},
  )
  for (const ref of [
    'https://example.invalid/schema',
    digest.replace(/[0-9a-f]$/, 'z'),
    '#/$defs/Missing',
  ])
    assert.throws(() => treeSchema(root({ value: { $ref: ref } }), nominals), /Unknown/)
  assert.throws(
    () =>
      treeSchema({
        ...root({ value: { $ref: '#/$defs/A' } }),
        $defs: { A: { $ref: '#/$defs/A' } },
      }),
    /Recursive/,
  )
  assert.throws(
    () => treeSchema(root({ unsafe: { type: 'string', contentEncoding: 'base64' } })),
    /Unsupported schema capability/,
  )
})

test('MCP oneOf choices and bounded string enum arrays have typed controls', () => {
  const schema = treeSchema(
    root({
      answer: {
        type: 'string',
        oneOf: [
          { const: 'yes', title: 'Yes please' },
          { const: 'no', title: 'No thanks' },
        ],
      },
      selected: { type: 'array', maxItems: 2, items: { anyOf: [{ const: 'a' }, { const: 'b' }] } },
    }),
  )
  assert.deepEqual(validateTree(schema, { answer: 'yes', selected: ['b', 'a'] }), {})
  assert.ok(validateTree(schema, { answer: 'bad', selected: ['a'] })['/answer'])
})
