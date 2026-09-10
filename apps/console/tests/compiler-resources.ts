interface ResourceProbe {
  outcome: string
  response_digest: string
  response_bytes: number
  source_inspection: {
    input_digest: string
    input_bytes: number
    outcome: string
    inspect_nanoseconds: number[]
    response_digest: string
    response_bytes: number
  }
}
// Actual native/WASM byte-boundary measurements. This is a local functional resource probe,
// not browser-process memory, provider latency or production capacity qualification.
import assert from 'node:assert/strict'
import { createHash } from 'node:crypto'
import { spawnSync } from 'node:child_process'
import { mkdtemp, readFile, readdir, rm, writeFile } from 'node:fs/promises'
import { tmpdir } from 'node:os'
import { join, resolve } from 'node:path'
import { fileURLToPath } from 'node:url'
import { isMainThread, parentPort, Worker, workerData } from 'node:worker_threads'

const root = fileURLToPath(new URL('../../../', import.meta.url))
const wasmPath = new URL(
  '../src/shared/compiler/generated/insight_platform_agent_compiler_wasm_bg.wasm',
  import.meta.url,
)
const hash = (bytes) => `sha256:${createHash('sha256').update(bytes).digest('hex')}`

if (!isMainThread) {
  const { initSync, compile_sources, inspect_agent_sources } =
    await import('../src/shared/compiler/generated/insight_platform_agent_compiler_wasm.js')
  const exports = initSync({ module: await readFile(wasmPath) })
  const before = exports.memory.buffer.byteLength
  const timings = []
  let peak = before
  let responseDigest
  let responseBytes
  let outcome
  for (let i = 0; i < workerData.iterations; i += 1) {
    const start = process.hrtime.bigint()
    const response = compile_sources(workerData.bytes)
    timings.push(Number(process.hrtime.bigint() - start))
    peak = Math.max(peak, exports.memory.buffer.byteLength)
    const actual = hash(response)
    if (responseDigest) assert.equal(actual, responseDigest)
    responseDigest = actual
    responseBytes = response.byteLength
    outcome = JSON.parse(new TextDecoder().decode(response)).outcome
  }
  const inspectionBefore = exports.memory.buffer.byteLength
  const inspectionTimings = []
  let inspectionPeak = inspectionBefore
  let inspectionDigest
  let inspectionResponseBytes
  let inspectionOutcome
  for (let i = 0; i < workerData.iterations; i += 1) {
    const start = process.hrtime.bigint()
    const response = inspect_agent_sources(workerData.inspectionBytes)
    inspectionTimings.push(Number(process.hrtime.bigint() - start))
    inspectionPeak = Math.max(inspectionPeak, exports.memory.buffer.byteLength)
    const actual = hash(response)
    if (inspectionDigest) assert.equal(actual, inspectionDigest)
    inspectionDigest = actual
    inspectionResponseBytes = response.byteLength
    inspectionOutcome = JSON.parse(new TextDecoder().decode(response)).outcome
  }
  parentPort.postMessage({
    response_digest: responseDigest,
    response_bytes: responseBytes,
    outcome,
    compile_nanoseconds: timings,
    linear_memory_initial_bytes: before,
    linear_memory_high_water_bytes: peak,
    source_inspection: {
      input_digest: hash(workerData.inspectionBytes),
      input_bytes: workerData.inspectionBytes.byteLength,
      response_digest: inspectionDigest,
      response_bytes: inspectionResponseBytes,
      outcome: inspectionOutcome,
      inspect_nanoseconds: inspectionTimings,
      linear_memory_initial_bytes: inspectionBefore,
      linear_memory_high_water_bytes: inspectionPeak,
    },
  })
} else {
  const [nativeArgument, outputArgument] = process.argv.slice(2)
  assert.ok(
    nativeArgument && outputArgument,
    'usage: node compiler-resources.ts NATIVE_BINARY REPORT_JSON',
  )
  const native = resolve(nativeArgument)
  const output = resolve(outputArgument)
  function nativeCall(args) {
    const result = spawnSync(native, args, {
      encoding: 'utf8',
      timeout: 60_000,
      maxBuffer: 1_048_576,
    })
    if (result.error) throw result.error
    assert.equal(result.status, 0, result.stderr)
    return JSON.parse(result.stdout)
  }
  const limits = nativeCall(['--limits'])
  const corpusRoot = join(root, 'contracts/product-experience/agent-compiler/v2')
  const corpus = JSON.parse(await readFile(join(corpusRoot, 'corpus.json'), 'utf8'))
  const cases = []
  for (const fixture of corpus.cases) {
    const files = Object.fromEntries(
      await Promise.all(
        [...new Set([fixture.manifest, fixture.input_schema, fixture.output_schema])].map(
          async (path) => [path, await readFile(join(corpusRoot, path), 'utf8')],
        ),
      ),
    )
    cases.push({
      name: fixture.case_id,
      expected: 'compiled',
      request: {
        schema_version: 1,
        sources: { manifest_path: fixture.manifest, files },
        profile: corpus.profile,
        bindings: fixture.bindings,
      },
    })
  }
  const large = structuredClone(cases[0].request)
  const manifestPath = large.sources.manifest_path
  large.sources.files[manifestPath] += ' '.repeat(
    limits.maximum_source_file_bytes - Buffer.byteLength(large.sources.files[manifestPath]),
  )
  cases.push({ name: 'maximum-single-source-file', expected: 'compiled', request: large })
  const extraSource = structuredClone(cases[0].request)
  extraSource.sources.files['unexpected-source.txt'] = 'unused'
  cases.push({ name: 'unexpected-source-rejection', expected: 'rejected', request: extraSource })
  cases.push({
    name: 'request-byte-bound-rejection',
    expected: 'rejected',
    bytes: Buffer.alloc(limits.maximum_request_bytes + 1, 32),
  })
  cases.push({
    name: 'deep-json-rejection',
    expected: 'rejected',
    bytes: Buffer.from(`{"sources":${'['.repeat(128)}0${']'.repeat(128)}}`),
  })

  const sourceFiles = []
  async function sourceTree(relative) {
    for (const entry of (await readdir(join(root, relative), { withFileTypes: true })).sort(
      (a, b) => a.name.localeCompare(b.name),
    )) {
      if (['target', 'generated', 'node_modules', '.git'].includes(entry.name)) continue
      const path = `${relative}/${entry.name}`
      if (entry.isDirectory()) await sourceTree(path)
      else if (entry.isFile())
        sourceFiles.push({ path, digest: hash(await readFile(join(root, path))) })
    }
  }
  for (const path of [
    'crates/authoring/platform-agent-compiler',
    'crates/authoring/platform-agent-compiler-wasm',
    'crates/definitions/platform-plan',
    'crates/foundation/platform-contracts',
  ])
    await sourceTree(path)
  for (const path of [
    'Cargo.toml',
    'Cargo.lock',
    'rust-toolchain.toml',
    'apps/console/scripts/build-agent-compiler.ts',
    'apps/console/tests/compiler-resources.ts',
    'tools/rust/platform-contract-tooling/src/bin/agent_compiler_resources.rs',
  ]) {
    sourceFiles.push({ path, digest: hash(await readFile(join(root, path))) })
  }
  const directory = await mkdtemp(join(tmpdir(), 'insight-compiler-resources-'))
  const observations = []
  try {
    for (const fixture of cases) {
      const bytes = fixture.bytes ?? Buffer.from(JSON.stringify(fixture.request))
      const path = join(directory, 'input.json')
      await writeFile(path, bytes, { mode: 0o600 })
      // Malformed byte-boundary cases go directly to both owning parsers. The harness
      // does not parse, repair or synthesize bindings for invalid source requests.
      const inspectionBytes = fixture.request
        ? Buffer.from(JSON.stringify({ schema_version: 1, sources: fixture.request.sources }))
        : bytes
      const inspectionPath = join(directory, 'inspection.json')
      await writeFile(inspectionPath, inspectionBytes, { mode: 0o600 })
      const iterations = 5
      const nativeResult = nativeCall([path, String(iterations), inspectionPath])
      const wasmResult = await new Promise<ResourceProbe>((resolveWorker, rejectWorker) => {
        const worker = new Worker(new URL(import.meta.url), {
          workerData: { bytes, inspectionBytes, iterations },
        })
        const timer = setTimeout(() => {
          void worker.terminate()
          rejectWorker(new Error('WASM resource probe exceeded its 60s harness deadline'))
        }, 60_000)
        worker.once('message', (result) => {
          clearTimeout(timer)
          void worker.terminate()
          resolveWorker(result)
        })
        worker.once('error', (error) => {
          clearTimeout(timer)
          void worker.terminate()
          rejectWorker(error)
        })
        worker.once('exit', (code) => {
          if (code !== 0) {
            clearTimeout(timer)
            rejectWorker(new Error(`WASM resource probe exited ${code}`))
          }
        })
      })
      assert.equal(nativeResult.outcome, fixture.expected, fixture.name)
      assert.equal(wasmResult.outcome, fixture.expected, fixture.name)
      assert.equal(nativeResult.response_digest, wasmResult.response_digest, fixture.name)
      assert.equal(nativeResult.response_bytes, wasmResult.response_bytes, fixture.name)
      for (const result of [nativeResult, wasmResult]) {
        assert.equal(result.source_inspection.input_digest, hash(inspectionBytes), fixture.name)
        assert.equal(result.source_inspection.input_bytes, inspectionBytes.byteLength, fixture.name)
        assert.equal(
          result.source_inspection.outcome,
          fixture.expected === 'compiled' ? 'inspected' : 'rejected',
          fixture.name,
        )
        assert.equal(result.source_inspection.inspect_nanoseconds.length, iterations, fixture.name)
      }
      assert.equal(
        nativeResult.source_inspection.response_digest,
        wasmResult.source_inspection.response_digest,
        fixture.name,
      )
      assert.equal(
        nativeResult.source_inspection.response_bytes,
        wasmResult.source_inspection.response_bytes,
        fixture.name,
      )
      observations.push({ case: fixture.name, native: nativeResult, wasm: wasmResult })
    }
    const report = {
      schema_version: 1,
      kind: 'insight.compiler.local-resource-observations/v1',
      recorded_at: new Date().toISOString(),
      platform: process.platform,
      architecture: process.arch,
      node_version: process.version,
      compiler_semantic_identity: limits.compiler_semantic_identity,
      native_binary_digest: hash(await readFile(native)),
      wasm_binary_digest: hash(await readFile(wasmPath)),
      source_files: sourceFiles,
      source_digest: hash(JSON.stringify(sourceFiles)),
      iterations_per_case: 5,
      harness_case_deadline_milliseconds: 60_000,
      qualification_scope:
        'local native/WASM compilation and source inspection parity with separate timings and WASM linear-memory observations; no production SLA or whole-browser memory claim',
      observations,
    }
    await writeFile(output, `${JSON.stringify(report, null, 2)}\n`, { mode: 0o600, flag: 'wx' })
    console.log(
      `Verified ${observations.length} actual native/WASM cases; observations written to ${output}`,
    )
  } finally {
    await rm(directory, { recursive: true, force: true })
  }
}
