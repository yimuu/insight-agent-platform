import '../../../tests/wasm-worker-fixture.ts'
import assert from 'node:assert/strict'
import { readFile } from 'node:fs/promises'
import test from 'node:test'
import { compileAgentManifest } from '../../shared/compiler/compiler.ts'
import { DEFAULT_INPUT_SCHEMA, DEFAULT_OUTPUT_SCHEMA } from './default-schemas.ts'

const corpusRoot = new URL(
  '../../../../../contracts/product-experience/agent-compiler/v2/',
  import.meta.url,
)

test('new conversation defaults compile with the owning Rust compiler and allow long text', async () => {
  const corpus = JSON.parse(await readFile(new URL('corpus.json', corpusRoot), 'utf8'))
  const fixture = corpus.cases.find((entry) => entry.case_id === 'model-chat-yaml')
  const compiled = await compileAgentManifest({
    manifest: await readFile(new URL(fixture.manifest, corpusRoot), 'utf8'),
    inputSchema: DEFAULT_INPUT_SCHEMA,
    outputSchema: DEFAULT_OUTPUT_SCHEMA,
    profile: corpus.profile,
    bindings: fixture.bindings,
  })
  assert.ok(compiled.resourceIntent)
  const input = JSON.parse(DEFAULT_INPUT_SCHEMA)
  const output = JSON.parse(DEFAULT_OUTPUT_SCHEMA)
  assert.deepEqual(input.required, ['message'])
  assert.deepEqual(output.required, ['answer'])
  assert.equal(input.additionalProperties, false)
  assert.equal(output.additionalProperties, false)
  // Regressions: the former 128-character / 512-byte defaults reject both examples.
  for (const story of [
    '春风吹过山林，旅人终于找到了回家的路。'.repeat(100),
    'A long story. '.repeat(500),
  ]) {
    assert.ok([...story].length > 128)
    assert.ok(new TextEncoder().encode(story).length > 512)
    assert.ok([...story].length <= output.properties.answer.maxLength)
    assert.ok(
      new TextEncoder().encode(story).length <= output.properties.answer['x-platform-max-bytes'],
    )
  }
  // Input never advertises more UTF-8 bytes than the conversation submission boundary.
  assert.equal(input.properties.message['x-platform-max-bytes'], 16_384)
  // Even worst-case JSON escaping plus its object envelope stays comfortably inline.
  const worstCase = JSON.stringify({ answer: '\u0001'.repeat(output.properties.answer.maxLength) })
  assert.ok(new TextEncoder().encode(worstCase).length < 262_144)
})
