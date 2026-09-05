import { spawn } from 'node:child_process'
import { createServer } from 'node:net'
import { fileURLToPath } from 'node:url'

const runId = 'run_0198f1c3-8f49-7c3e-b1f3-773c28367b90'
const emptyRunId = 'run_0198f1c3-8f49-7c3e-b1f3-773c28367b95'
const taskId = 'int_0198f1c3-8f49-7c3e-b1f3-773c28367b91'
const token = 'fixture-token-not-a-credential'
const directory = fileURLToPath(new URL('.', import.meta.url))
const delay = (milliseconds) => new Promise((resolveDelay) => setTimeout(resolveDelay, milliseconds))

function waitForChild(child, timeoutMilliseconds) {
  if (child.exitCode !== null || child.signalCode !== null) {
    return Promise.resolve({ code: child.exitCode, signal: child.signalCode })
  }
  return new Promise((resolveExit) => {
    const finish = (status) => {
      clearTimeout(timer)
      child.removeListener('exit', onExit)
      resolveExit(status)
    }
    const onExit = (code, signal) => finish({ code, signal })
    const timer = setTimeout(() => {
      child.kill('SIGKILL')
      finish({ code: null, signal: 'TIMEOUT' })
    }, timeoutMilliseconds)
    child.once('exit', onExit)
  })
}

async function unusedPort() {
  const server = createServer()
  await new Promise((resolveListen, rejectListen) => {
    server.once('error', rejectListen)
    server.listen(0, '127.0.0.1', resolveListen)
  })
  const address = server.address()
  if (!address || typeof address === 'string') throw new Error('could not reserve a fixture port')
  const { port } = address
  await new Promise((resolveClose) => server.close(resolveClose))
  return port
}

async function waitForFixture(output, process, deadline) {
  while (Date.now() < deadline) {
    if (output().includes('console fixture ready ')) return
    if (process.exitCode !== null) throw new Error(`Console fixture exited before readiness (${process.exitCode})`)
    await delay(50)
  }
  throw new Error('Console fixture did not become ready')
}

async function runJourney(origin) {
  const child = spawn(process.execPath, [`${directory}real-gateway-journey.mjs`], {
    env: {
      ...process.env,
      INSIGHT_CONSOLE_ACCESS_TOKEN: token,
      INSIGHT_CONSOLE_AUTHORING_JOURNEY: '1',
      INSIGHT_CONSOLE_EXPECTED_RESULT_TEXT: 'completed',
      INSIGHT_CONSOLE_GATEWAY_ORIGIN: origin,
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
  let stdout = ''
  let stderr = ''
  child.stdout.on('data', (chunk) => { stdout = `${stdout}${chunk}`.slice(-64 * 1024) })
  child.stderr.on('data', (chunk) => { stderr = `${stderr}${chunk}`.slice(-64 * 1024) })
  const status = await waitForChild(child, 90_000)
  if (status.code !== 0) throw new Error(`browser journey failed (${JSON.stringify(status)})\n${stderr}\n${stdout}`)
  const evidence = JSON.parse(stdout)
  if (evidence.kind !== 'insight.console.real-gateway-journey/v1' || evidence.status !== 'passed') {
    throw new Error('browser journey did not return closed Passed evidence')
  }
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
  if (!mutation || mutation.if_match !== '"task-v1"' || !mutation.idempotency_key_present) {
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
  if (output.includes(token)) throw new Error('fixture request log exposed the bearer token')
  return records.filter((record) => Number.isInteger(record.request_count)).length
}

async function main() {
  const port = await unusedPort()
  const origin = `http://127.0.0.1:${port}`
  const fixture = spawn(process.execPath, [`${directory}fixture-server.mjs`], {
    env: {
      ...process.env,
      INSIGHT_CONSOLE_FIXTURE_PORT: String(port),
      INSIGHT_CONSOLE_FIXTURE_SLOW_RESPONSE_MS: '750',
    },
    stdio: ['ignore', 'pipe', 'pipe'],
  })
  let fixtureOutput = ''
  let fixtureError = ''
  fixture.stdout.on('data', (chunk) => { fixtureOutput = `${fixtureOutput}${chunk}`.slice(-256 * 1024) })
  fixture.stderr.on('data', (chunk) => { fixtureError = `${fixtureError}${chunk}`.slice(-64 * 1024) })
  try {
    await waitForFixture(() => fixtureOutput, fixture, Date.now() + 10_000)
    const journey = await runJourney(origin)
    // stdout is asynchronous relative to the fixture socket close. Let the
    // final request-log chunk reach this process before closing the evidence.
    await delay(100)
    const requestCount = verifyRequestLog(fixtureOutput)
    process.stdout.write(`${JSON.stringify({
      kind: 'insight.console.browser-fixture-qualification/v1',
      status: 'passed',
      request_count: requestCount,
      journey_checks: journey.checks,
    })}\n`)
  } finally {
    fixture.kill('SIGTERM')
    await waitForChild(fixture, 5_000)
  }
  if (fixtureError) process.stderr.write(fixtureError)
}

main().catch((error) => {
  process.stderr.write(`${error instanceof Error ? error.stack ?? error.message : String(error)}\n`)
  process.exit(1)
})
