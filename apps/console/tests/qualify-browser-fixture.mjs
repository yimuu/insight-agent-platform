import { fileURLToPath, pathToFileURL } from 'node:url'
import { resolve } from 'node:path'
import { performance } from 'node:perf_hooks'
import { BrowserProcessError, qualificationSignals, spawnFixtureChild, waitForFixtureChild, withinSignal } from './browser-process.mjs'

const runId = 'run_0198f1c3-8f49-7c3e-b1f3-773c28367b90'
const emptyRunId = 'run_0198f1c3-8f49-7c3e-b1f3-773c28367b95'
const taskId = 'int_0198f1c3-8f49-7c3e-b1f3-773c28367b91'
const token = 'fixture-token-not-a-credential'
const directory = fileURLToPath(new URL('.', import.meta.url))
const delay = (milliseconds) => new Promise((resolveDelay) => setTimeout(resolveDelay, milliseconds))

export function fixtureReadyOrigin(output) {
  const lines = output.split('\n').slice(0, -1).filter(line => line.startsWith('console fixture ready '))
  if (lines.length === 0) return null
  if (lines.length !== 1) throw new BrowserProcessError('invalid_fixture_endpoint')
  const match = /^console fixture ready http:\/\/127\.0\.0\.1:([1-9][0-9]{0,4}) run=([^ ]+) task=([^ ]+)$/.exec(lines[0])
  if (!match || Number(match[1]) > 65535 || match[2] !== runId || match[3] !== taskId) throw new BrowserProcessError('invalid_fixture_endpoint')
  return `http://127.0.0.1:${match[1]}`
}

export async function waitForFixture(output, owner, timeoutMs, signal) {
  const deadline = performance.now() + timeoutMs
  while (performance.now() < deadline) {
    owner.assertRunning()
    const origin = fixtureReadyOrigin(output())
    if (origin) return origin
    await withinSignal(delay(Math.min(50, deadline - performance.now())), AbortSignal.any([signal, owner.stopped]))
  }
  throw new BrowserProcessError('fixture_startup_timeout', owner.diagnostics())
}

async function runJourney(origin, signal) {
  const owner = spawnFixtureChild(process.execPath, [`${directory}fixture-browser-journey.mjs`], {
    env: {
      ...process.env,
      INSIGHT_CONSOLE_ACCESS_TOKEN: token,
      INSIGHT_CONSOLE_AUTHORING_JOURNEY: '1',
      INSIGHT_CONSOLE_EXPECTED_RESULT_TEXT: 'completed',
      INSIGHT_CONSOLE_GATEWAY_ORIGIN: origin,
      INSIGHT_CONSOLE_MANAGEMENT_GATEWAY_ORIGIN: origin,
      INSIGHT_CONSOLE_RUN_ID: runId,
      INSIGHT_CONSOLE_EMPTY_RUN_ID: emptyRunId,
      INSIGHT_CONSOLE_EXPECT_SLOW_LOADING: '1',
      INSIGHT_CONSOLE_TASK_ID: taskId,
      INSIGHT_CONSOLE_TASK_SAFE_PROMPT_KEY: 'interaction.confirm_release',
      INSIGHT_CONSOLE_TASK_RESPONSE: JSON.stringify({
        classification: 'internal',
        schema_digest: `sha256:${'a'.repeat(64)}`,
        value: { kind: 'inline', value: { message: 'after task' } },
      }),
    },
    stdio: ['ignore', 'pipe', 'pipe'],
  })
  const child = owner.child
  let stdout = ''
  let stderr = ''
  child.stdout.on('data', (chunk) => { stdout = `${stdout}${chunk}`.slice(-64 * 1024) })
  child.stderr.on('data', (chunk) => { stderr = `${stderr}${chunk}`.slice(-64 * 1024) })
  let evidence
  let failure
  try {
    const status = await waitForFixtureChild(owner, 90_000, signal)
    if (status.code !== 0) throw new Error(`browser journey failed (${JSON.stringify(status)})\n${stderr}\n${stdout}`)
    evidence = JSON.parse(stdout)
    if (evidence.kind !== 'insight.console.synthetic-gateway-journey/v1' || evidence.status !== 'passed') throw new Error('browser journey did not return closed Passed evidence')
  } catch (error) { failure = error } finally {
    // Give the journey its own bounded browser + server cleanup interval before escalation.
    try { await owner.terminate({ graceMs: 10_000 }) } catch (error) {
      failure = new Error(`${failure?.message ?? 'browser journey cleanup failed'}\nSafe cleanup failure: ${error instanceof BrowserProcessError ? error.reason : 'journey_cleanup_failed'}`)
    }
  }
  if (failure) throw failure
  return evidence
}

function verifyRequestLog(output) {
  const records = output
    .split('\n')
    .filter((line) => line.startsWith('{'))
    .map((line) => JSON.parse(line))
  const readiness = records.filter((record) => record.path === '/readyz')
  const publicRequests = records.filter((record) => typeof record.path === 'string' && record.path.startsWith('/v1/'))
  const mutationIndex = publicRequests.findIndex((record) => record.method === 'POST' && record.path === `/v1/tasks/${taskId}:submit-input`)
  const mutation = publicRequests[mutationIndex]
  const runReadsAfterMutation = publicRequests.slice(mutationIndex + 1).filter((record) =>
    record.method === 'GET' && record.path === `/v1/runs/${runId}`
  )
  const postTaskRunResponses = records.filter((record) =>
    record.fixture_observation === 'post_task_run_response'
  )
  const overlappingRunReads = records.filter((record) =>
    record.fixture_observation === 'overlapping_post_task_run_read'
  )
  if (readiness.length < 2 || readiness.some((record) => record.authorization_present)) {
    throw new Error('readiness requests must remain unauthenticated before and after reload')
  }
  if (!publicRequests.length || publicRequests.some((record) => !record.authorization_present)) {
    throw new Error('every public authority request must carry the in-memory bearer credential')
  }
  if (!mutation || mutation.if_match !== `"${taskId}-1"` || !mutation.idempotency_key_present) {
    throw new Error('Task mutation did not preserve exact If-Match and Receipt headers')
  }
  if (runReadsAfterMutation.length < 3) {
    throw new Error('browser journey did not refresh the Run authority through running, succeeded, and reload re-read')
  }
  if (JSON.stringify(postTaskRunResponses.slice(0, 3).map((record) => record.state)) !== JSON.stringify(['running', 'succeeded', 'succeeded'])) {
    throw new Error('browser journey did not observe ordered running, succeeded, then reload succeeded Run responses')
  }
  if (overlappingRunReads.length > 0) {
    throw new Error('browser journey issued overlapping Run authority reads')
  }
  for (const observation of ['full_plan_source_uploaded', 'exact_plan_uploaded', 'unique_full_plan_agent_created', 'exact_full_plan_run_input']) {
    if (records.filter(record => record.fixture_observation === observation).length !== 1) throw new Error(`Missing unique authoring evidence: ${observation}`)
  }
  if (output.includes(token)) throw new Error('fixture request log exposed the bearer token')
  return records.filter((record) => Number.isInteger(record.request_count)).length
}

async function main() {
  const signals = qualificationSignals()
  const fixtureOwner = spawnFixtureChild(process.execPath, [`${directory}fixture-server.mjs`], {
    env: {
      ...process.env,
      INSIGHT_CONSOLE_FIXTURE_PORT: '0',
      INSIGHT_CONSOLE_FIXTURE_SLOW_RESPONSE_MS: '750',
    },
    stdio: ['ignore', 'pipe', 'pipe'],
  })
  const fixture = fixtureOwner.child
  let fixtureOutput = ''
  let fixtureError = ''
  fixture.stdout.on('data', (chunk) => { fixtureOutput = `${fixtureOutput}${chunk}`.slice(-256 * 1024) })
  fixture.stderr.on('data', (chunk) => { fixtureError = `${fixtureError}${chunk}`.slice(-64 * 1024) })
  let evidence
  let failure
  try {
    const origin = await waitForFixture(() => fixtureOutput, fixtureOwner, 10_000, signals.signal)
    const journey = await runJourney(origin, signals.signal)
    // stdout is asynchronous relative to the fixture socket close. Let the
    // final request-log chunk reach this process before closing the evidence.
    await delay(100)
    const requestCount = verifyRequestLog(fixtureOutput)
    if (signals.signal.aborted) throw new BrowserProcessError('interrupted')
    evidence = {
      kind: 'insight.console.browser-fixture-qualification/v1',
      status: 'passed',
      request_count: requestCount,
      journey_checks: journey.checks,
      authoring: journey.authoring,
    }
  } catch (error) { failure = error } finally {
    try { await fixtureOwner.terminate() } catch (error) {
      failure = new Error(`${failure?.message ?? 'fixture cleanup failed'}\nSafe cleanup failure: ${error instanceof BrowserProcessError ? error.reason : 'fixture_cleanup_failed'}`)
    }
    signals.dispose()
  }
  if (signals.signal.aborted && !failure) failure = new BrowserProcessError('interrupted')
  if (failure) throw failure
  if (fixtureError.includes(token)) throw new BrowserProcessError('fixture_diagnostic_contains_token')
  process.stdout.write(`${JSON.stringify(evidence)}\n`)
}

if (process.argv[1] && import.meta.url === pathToFileURL(resolve(process.argv[1])).href) main().catch((error) => {
  process.stderr.write(`${error instanceof Error ? error.stack ?? error.message : String(error)}\n`)
  process.exit(process.exitCode ?? 1)
})
