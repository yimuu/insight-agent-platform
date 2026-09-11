import '../../../tests/wasm-worker-fixture.ts'
import assert from 'node:assert/strict'
import { createHash } from 'node:crypto'
import { readFile } from 'node:fs/promises'
import test from 'node:test'
import { compile_agent } from './generated/insight_platform_agent_compiler_wasm.js'

import {
  AgentCompilerError,
  inspectAgentSources,
  compileCapturedAgentSources,
  compileAgentManifest,
  compilerConformanceProjection,
  verifyAgentAuthoringProfile,
} from './compiler.ts'

const corpusRoot = new URL(
  '../../../../../contracts/product-experience/agent-compiler/v2/',
  import.meta.url,
)

async function text(relative) {
  return readFile(new URL(relative, corpusRoot), 'utf8')
}

test('WASM adapter preserves the Rust v2 corpus and binds the new complete source Artifact', async () => {
  const corpus = JSON.parse(await text('corpus.json'))
  assert.equal(corpus.schema_version, 2)
  for (const fixture of corpus.cases) {
    const compiled = await compileAgentManifest({
      manifest: await text(fixture.manifest),
      inputSchema: await text(fixture.input_schema),
      outputSchema: await text(fixture.output_schema),
      profile: corpus.profile,
      bindings: fixture.bindings,
    })
    const actual = await compilerConformanceProjection(compiled)
    assert.ok(actual && typeof actual === 'object' && !Array.isArray(actual))
    assert.ok(
      compiled.resourceIntent &&
        typeof compiled.resourceIntent === 'object' &&
        !Array.isArray(compiled.resourceIntent),
    )
    const authoringArtifact = compiled.resourceIntent.authoring_artifact
    assert.ok(
      authoringArtifact &&
        typeof authoringArtifact === 'object' &&
        !Array.isArray(authoringArtifact),
    )
    const { resource_intent_digest: oldArtifactIntent, ...expected } = fixture.expected
    const { resource_intent_digest: bundleArtifactIntent, ...preserved } = actual
    assert.deepEqual(preserved, expected, fixture.case_id)
    assert.notEqual(bundleArtifactIntent, oldArtifactIntent)
    assert.equal(authoringArtifact.content_digest, compiled.sourceBundleDigest)
    assert.equal(
      authoringArtifact.byte_length,
      new TextEncoder().encode(compiled.sourceBundle).length,
    )
    assert.equal(
      compiled.sourceBundleDigest,
      `sha256:${createHash('sha256').update(compiled.sourceBundle).digest('hex')}`,
    )
  }
})

test('TypeScript adapter rejects unsafe YAML and impossible deterministic schemas', async () => {
  const corpus = JSON.parse(await text('corpus.json'))
  const manifest = await text('deterministic.yaml')
  for (const unsafe of [
    manifest.replace('kind: Agent', 'kind: Agent\nkind: Agent'),
    manifest.replace(
      'metadata:\n',
      'defaults: &defaults {name: echo-agent}\nmetadata:\n  <<: *defaults\n',
    ),
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
      manifest: manifest.replace(
        'output:\n    schema: schema-message.json',
        'output:\n    schema: schema-answer.json',
      ),
      inputSchema: await text('schema-message.json'),
      outputSchema: await text('schema-answer.json'),
      profile: corpus.profile,
      bindings: { model: null },
    }),
    (error) => error instanceof AgentCompilerError && error.code === 'agent_compile_failed',
  )
})

test('actual WASM preserves local schema definitions, embedded IDs and literal ref data in echo and full Plan compilation', async () => {
  const corpus = JSON.parse(await text('corpus.json'))
  const manifest = await text('deterministic.yaml')
  const schema = {
    $schema: 'https://json-schema.org/draft/2020-12/schema',
    $id: 'https://fixture.invalid/local-definitions',
    type: 'object',
    additionalProperties: false,
    properties: {
      message: { $ref: '#/$defs/Message' },
      literal: { $ref: '#/$defs/Literal' },
    },
    required: ['message', 'literal'],
    $defs: {
      Message: {
        $id: 'https://fixture.invalid/message',
        type: 'string',
        minLength: 1,
        maxLength: 128,
        'x-platform-max-bytes': 512,
      },
      Literal: {
        type: 'object',
        additionalProperties: false,
        properties: {
          $ref: { type: 'string', minLength: 1, maxLength: 64, 'x-platform-max-bytes': 64 },
        },
        required: ['$ref'],
        // This apparent self-reference is application data, never a schema cycle.
        const: { $ref: '#/$defs/Literal' },
      },
    },
  }
  const schemaSource = `${JSON.stringify(schema, null, 2)}\n`
  const input = {
    manifest,
    inputSchema: schemaSource,
    outputSchema: schemaSource,
    profile: corpus.profile,
    bindings: { model: null, slots: [] },
  }
  const compiled = await compileAgentManifest(input)
  const bundle = JSON.parse(compiled.sourceBundle)
  const plan = JSON.parse(compiled.typedPlan)
  const frozenSchema = plan.schema_documents[plan.nodes.finish.value.schema_digest].schema
  assert.equal(compiled.executionKind, 'deterministic')
  assert.equal(bundle.sources.files['schema-message.json'], schemaSource)
  assert.deepEqual(frozenSchema, schema)
  assert.deepEqual(frozenSchema.$defs.Literal.const, { $ref: '#/$defs/Literal' })

  // The full source-bundle ABI invokes the same Rust compiler as the Worker adapter.
  const response = JSON.parse(
    new TextDecoder().decode(compile_agent(new TextEncoder().encode(compiled.sourceBundle))),
  )
  assert.equal(response.outcome, 'compiled')
  const decoded = (bytes) => new TextDecoder().decode(new Uint8Array(bytes))
  assert.equal(decoded(response.compilation.source_bundle_bytes), compiled.sourceBundle)
  assert.equal(decoded(response.compilation.compiled.typed_plan_bytes), compiled.typedPlan)
  assert.equal(
    decoded(response.compilation.compiled.canonical_manifest_bytes),
    compiled.canonicalManifest,
  )
  assert.equal(response.compilation.source_bundle_digest, compiled.sourceBundleDigest)
  assert.equal(response.compilation.compiled.typed_plan_digest, compiled.typedPlanDigest)
  for (const [bytes, digest] of [
    [compiled.sourceBundle, compiled.sourceBundleDigest],
    [compiled.typedPlan, compiled.typedPlanDigest],
    [compiled.canonicalManifest, compiled.manifestDigest],
  ])
    assert.equal(digest, `sha256:${createHash('sha256').update(bytes).digest('hex')}`)

  // Reuse the actual Rust-produced Plan as editable full Plan source, without JS lowering.
  const fullPlan = await compileAgentManifest({
    ...input,
    manifest: manifest.replace('kind: deterministic', 'kind: full_plan\n    plan: echo-plan.json'),
    plan: compiled.typedPlan,
  })
  assert.equal(fullPlan.executionKind, 'full_plan')
  assert.equal(fullPlan.typedPlan, compiled.typedPlan)
  assert.equal(fullPlan.typedPlanDigest, compiled.typedPlanDigest)
  assert.equal(
    JSON.parse(fullPlan.sourceBundle).sources.files['echo-plan.json'],
    compiled.typedPlan,
  )
  assert.equal(JSON.parse(fullPlan.sourceBundle).sources.files['schema-message.json'], schemaSource)
})

test('browser authoring profile is digest protected and has no fallback authority', async () => {
  const corpus = JSON.parse(await text('corpus.json'))
  const profile = { schema_version: 1, ...corpus.profile, models: [] }
  const canonical = (value) => {
    if (Array.isArray(value)) return `[${value.map(canonical).join(',')}]`
    if (value !== null && typeof value === 'object') {
      return `{${Object.keys(value)
        .sort()
        .map((key) => `${JSON.stringify(key)}:${canonical(value[key])}`)
        .join(',')}}`
    }
    return JSON.stringify(value)
  }
  profile.profile_digest = `sha256:${createHash('sha256').update(canonical(profile)).digest('hex')}`
  await verifyAgentAuthoringProfile(profile)
  profile.default_deadline_seconds += 1
  await assert.rejects(verifyAgentAuthoringProfile(profile), /digest/)
})

test('source-only WASM preflight rejects local faults without profile or bindings and compiles the same captured snapshot', async () => {
  const corpus = JSON.parse(await text('corpus.json'))
  const input = {
    manifest: await text('deterministic.yaml'),
    inputSchema: await text('schema-message.json'),
    outputSchema: await text('schema-message.json'),
  }
  const captured = await inspectAgentSources(input)
  input.manifest = '{ changed after source capture'
  input.inputSchema = '{ changed after source capture'
  const compiled = await compileCapturedAgentSources(captured.sources, corpus.profile, {
    model: null,
    slots: [],
  })
  assert.equal(
    JSON.parse(compiled.sourceBundle).sources.files['agent.yaml'],
    captured.sources.files['agent.yaml'],
  )
  for (const invalid of [
    { ...input },
    {
      manifest: await text('model-chat.yaml'),
      inputSchema: '{ invalid schema',
      outputSchema: await text('schema-answer.json'),
    },
    {
      manifest: (await text('deterministic.yaml')).replace(
        'kind: deterministic',
        'kind: full_plan\n    plan: graph.json',
      ),
      inputSchema: await text('schema-message.json'),
      outputSchema: await text('schema-message.json'),
      plan: '{ invalid Plan',
    },
  ])
    await assert.rejects(inspectAgentSources(invalid), AgentCompilerError)
})
