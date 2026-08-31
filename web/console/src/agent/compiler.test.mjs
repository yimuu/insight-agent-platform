import assert from 'node:assert/strict'
import { createHash } from 'node:crypto'
import { readFile } from 'node:fs/promises'
import test from 'node:test'

import {
  AgentCompilerError,
  compileAgentManifest,
  compilerConformanceProjection,
  verifyAgentAuthoringProfile,
} from './compiler.ts'

const corpusRoot = new URL('../../../../contracts/product-experience/agent-compiler/v1/', import.meta.url)

async function text(relative) {
  return readFile(new URL(relative, corpusRoot), 'utf8')
}

test('TypeScript adapter matches every Rust owner conformance fixture byte for byte', async () => {
  const corpus = JSON.parse(await text('corpus.json'))
  assert.equal(corpus.schema_version, 1)
  for (const fixture of corpus.cases) {
    const compiled = await compileAgentManifest({
      manifest: await text(fixture.manifest),
      inputSchema: await text(fixture.input_schema),
      outputSchema: await text(fixture.output_schema),
      profile: corpus.profile,
      bindings: fixture.bindings,
    })
    assert.deepEqual(
      await compilerConformanceProjection(compiled),
      fixture.expected,
      fixture.case_id,
    )
  }
})

test('TypeScript adapter rejects unsafe YAML and impossible deterministic schemas', async () => {
  const corpus = JSON.parse(await text('corpus.json'))
  const manifest = await text('deterministic.yaml')
  for (const unsafe of [
    manifest.replace('kind: Agent', 'kind: Agent\nkind: Agent'),
    manifest.replace('metadata:\n', 'defaults: &defaults {name: echo-agent}\nmetadata:\n  <<: *defaults\n'),
    manifest.replace('name: echo-agent', 'name: !tenant echo-agent'),
  ]) {
    await assert.rejects(
      compileAgentManifest({
        manifest: unsafe,
        inputSchema: await text('schema-message.json'),
        outputSchema: await text('schema-message.json'),
        profile: corpus.profile,
        bindings: { model: null },
      }),
      (error) => error instanceof AgentCompilerError && error.code === 'agent_manifest_invalid',
    )
  }
  await assert.rejects(
    compileAgentManifest({
      manifest,
      inputSchema: await text('schema-message.json'),
      outputSchema: await text('schema-answer.json'),
      profile: corpus.profile,
      bindings: { model: null },
    }),
    (error) => error instanceof AgentCompilerError && error.code === 'agent_compile_failed',
  )
})

test('browser authoring profile is digest protected and has no fallback authority', async () => {
  const corpus = JSON.parse(await text('corpus.json'))
  const profile = { schema_version: 1, ...corpus.profile, models: [] }
  const canonical = (value) => {
    if (Array.isArray(value)) return `[${value.map(canonical).join(',')}]`
    if (value !== null && typeof value === 'object') {
      return `{${Object.keys(value).sort().map((key) => `${JSON.stringify(key)}:${canonical(value[key])}`).join(',')}}`
    }
    return JSON.stringify(value)
  }
  profile.profile_digest = `sha256:${createHash('sha256').update(canonical(profile)).digest('hex')}`
  await verifyAgentAuthoringProfile(profile)
  profile.default_deadline_seconds += 1
  await assert.rejects(verifyAgentAuthoringProfile(profile), /digest/)
})
