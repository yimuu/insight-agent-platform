import assert from 'node:assert/strict'
import test from 'node:test'
import { analyzeTaskSchema, changeRaw, createDraft, FORM_LIMITS, schemaIdentity, validateDraft } from './schema-form.ts'

const digest = `sha256:${'a'.repeat(64)}`
const text = { type: 'string', minLength: 1, maxLength: 8, 'x-platform-max-bytes': 12 }
const object = (properties, required = []) => ({ $schema: 'https://json-schema.org/draft/2020-12/schema', type: 'object', properties, required, additionalProperties: false })
const shape = (properties, required = Object.keys(properties)) => {
  const analysis = analyzeTaskSchema(object(properties, required), digest)
  assert.deepEqual(analysis.issues, [])
  return { node: analysis.node, draft: createDraft(analysis.node) }
}
const write = (draft, field, raw) => { draft.children[field] = changeRaw(draft.children[field], raw) }

test('typed required fields preserve false and enum types while optional fields remain absent', () => {
  const { node, draft } = shape({ name: text, count: { type: 'integer', minimum: 1, maximum: 5 }, consent: { type: 'boolean' }, choice: { type: 'number', enum: [0, 1.5] }, optional: text }, ['name', 'count', 'consent', 'choice'])
  const invalid = validateDraft(node, draft)
  assert.deepEqual(Object.keys(invalid.errors), ['/name', '/count', '/consent', '/choice'])
  assert.equal(invalid.value, undefined)
  write(draft, 'name', 'answer'); write(draft, 'count', '2'); write(draft, 'consent', 'false'); write(draft, 'choice', '1')
  assert.deepEqual(validateDraft(node, draft).value, { name: 'answer', count: 2, consent: false, choice: 1.5 })
  draft.children.optional.included = true
  assert.match(validateDraft(node, draft).errors['/optional'], /at least/)
  write(draft, 'optional', 'hello')
  assert.equal(validateDraft(node, draft).value.optional, 'hello')
})

test('text limits count Unicode characters and UTF-8 bytes, with overflow never silently submitted', () => {
  const { node, draft } = shape({ text: { ...text, maxLength: 3, 'x-platform-max-bytes': 8 } })
  write(draft, 'text', '😀😀')
  assert.equal(validateDraft(node, draft).value.text, '😀😀')
  write(draft, 'text', '😀😀😀')
  assert.match(validateDraft(node, draft).errors['/text'], /UTF-8 bytes/)
  write(draft, 'text', '\ud800')
  assert.match(validateDraft(node, draft).errors['/text'], /Unicode/)
  write(draft, 'text', 'x'.repeat(FORM_LIMITS.text * 3))
  assert.equal(draft.children.text.raw.length, FORM_LIMITS.text * 2)
  assert.match(validateDraft(node, draft).errors['/text'], /Edit this field/)
  write(draft, 'text', 'ok')
  assert.equal(validateDraft(node, draft).value.text, 'ok')
})

test('numbers do not coerce blank/hex/NaN/unsafe integers and respect open and closed bounds', () => {
  const { node, draft } = shape({ count: { type: 'integer', exclusiveMinimum: 1, maximum: 3 }, number: { type: 'number', minimum: -2, exclusiveMaximum: 1.5 } })
  write(draft, 'number', '0.1')
  for (const invalid of ['', ' 2', '0x2', 'NaN', '2.5', '9007199254740993', '1']) {
    write(draft, 'count', invalid)
    assert.ok(validateDraft(node, draft).errors['/count'], invalid)
  }
  write(draft, 'count', '3'); write(draft, 'number', '1.5')
  assert.match(validateDraft(node, draft).errors['/number'], /less than/)
  write(draft, 'number', '-2e0')
  assert.deepEqual(validateDraft(node, draft).value, { count: 3, number: -2 })
})

test('arrays validate minimum, maximum, duplicate object values, nested fields and escaped paths', () => {
  const nested = { type: 'object', properties: { 'a/b~': text }, required: ['a/b~'], additionalProperties: false }
  const { node, draft } = shape({ entries: { type: 'array', minItems: 1, maxItems: 2, uniqueItems: true, items: nested } })
  assert.match(validateDraft(node, draft).errors['/entries'], /at least 1/)
  const itemNode = node.properties[0].node.items
  draft.children.entries.items.push(createDraft(itemNode))
  assert.match(validateDraft(node, draft).errors['/entries/0/a~1b~0'], /at least/)
  write(draft.children.entries.items[0], 'a/b~', 'same')
  draft.children.entries.items.push(structuredClone(draft.children.entries.items[0]))
  assert.match(validateDraft(node, draft).errors['/entries/1'], /unique/)
  draft.children.entries.items.push(createDraft(itemNode))
  assert.match(validateDraft(node, draft).errors['/entries'], /at most 2/)
})

test('unsupported features and resource bounds disable the whole form explicitly', () => {
  for (const keyword of ['$ref', '$defs', 'oneOf', 'anyOf', 'format', 'multipleOf', 'pattern', 'const']) {
    const analysis = analyzeTaskSchema(object({ value: { ...text, [keyword]: 'unsupported' } }), digest)
    assert.equal(analysis.node, undefined, keyword)
    assert.match(analysis.issues.join(' '), new RegExp(keyword.replaceAll('$', '\\$')))
  }
  for (const schema of [object({ value: { type: 'string' } }), object({ value: { type: 'array', minItems: 0, maxItems: 65, items: text } }), { ...object({}), additionalProperties: true }, object({ value: { type: 'array', minItems: 0, maxItems: 64, items: { type: 'array', minItems: 0, maxItems: 64, items: text } } })]) {
    assert.equal(analyzeTaskSchema(schema, digest).node, undefined)
  }
  assert.equal(analyzeTaskSchema(object({ text: { ...text, description: 'x'.repeat(FORM_LIMITS.schemaBytes) } }), digest).node, undefined)
})

test('envelope scope mismatch cannot submit; MCP root is implicitly closed and multi-select unique', () => {
  const schema = { schema_version: 1, profile: 'mcp.form-json-schema/2025-11-25', canonical_digest: digest, schema: { type: 'object', properties: { choices: { type: 'array', items: { type: 'string', enum: ['one', 'two'], enumNames: ['First', 'Second'] } } }, required: ['choices'] } }
  assert.match(analyzeTaskSchema(schema, 'another-digest').issues.join(), /does not match/)
  const { node, issues } = analyzeTaskSchema(schema, digest)
  assert.deepEqual(issues, [])
  const draft = createDraft(node)
  const itemNode = node.properties[0].node.items
  draft.children.choices.items = [changeRaw(createDraft(itemNode), '0'), changeRaw(createDraft(itemNode), '0')]
  assert.match(validateDraft(node, draft).errors['/choices/1'], /unique/)
  draft.children.choices.items[1].raw = '1'
  assert.deepEqual(validateDraft(node, draft).value, { choices: ['one', 'two'] })
  assert.notEqual(schemaIdentity(schema, digest), schemaIdentity(schema, 'new-digest'))
  assert.equal(schemaIdentity(schema, digest), schemaIdentity(structuredClone(schema), digest))
})

test('property names are data, and total response size is bounded', () => {
  const { node, draft } = shape(JSON.parse('{"__proto__":{"type":"boolean"}}'))
  write(draft, '__proto__', 'false')
  const value = validateDraft(node, draft).value
  assert.equal(Object.hasOwn(value, '__proto__'), true)
  assert.equal(value.__proto__, false)
  assert.equal(Object.getPrototypeOf(value), Object.prototype)
  const large = shape(Object.fromEntries(Array.from({ length: 10 }, (_, index) => [String(index), { type: 'string', minLength: 0, maxLength: 8192, 'x-platform-max-bytes': 8192 }])))
  Object.values(large.draft.children).forEach((item) => { item.raw = 'x'.repeat(8192) })
  assert.match(validateDraft(large.node, large.draft).errors[''], /response|Response/)
})
