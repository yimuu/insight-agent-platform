import { createReadStream, existsSync, statSync } from 'node:fs'
import { createServer } from 'node:http'
import { extname, join, normalize, resolve } from 'node:path'
import { fileURLToPath } from 'node:url'

const root = resolve(fileURLToPath(new URL('../dist', import.meta.url)))
const port = Number(process.env.INSIGHT_CONSOLE_FIXTURE_PORT ?? 4173)
const host = '127.0.0.1'
const traceId = '0123456789abcdef0123456789abcdef'
const runId = 'run_0198f1c3-8f49-7c3e-b1f3-773c28367b90'
const emptyRunId = 'run_0198f1c3-8f49-7c3e-b1f3-773c28367b95'
const taskId = 'int_0198f1c3-8f49-7c3e-b1f3-773c28367b91'
const deploymentId = 'adep_0198f1c3-8f49-7c3e-b1f3-773c28367b92'
const outputId = 'val_0198f1c3-8f49-7c3e-b1f3-773c28367b93'
let taskState = 'pending'
let runState = 'waiting'
let taskVersion = 1
let requestCount = 0
const slowResponseMilliseconds = Number(process.env.INSIGHT_CONSOLE_FIXTURE_SLOW_RESPONSE_MS ?? 0)

const delay = (milliseconds) => new Promise((resolveDelay) => setTimeout(resolveDelay, milliseconds))

function headers(extra = {}) {
  return {
    'cache-control': 'no-store, private, max-age=0',
    'trace-id': traceId,
    ...extra,
  }
}

function sendJson(response, status, value, extra = {}) {
  const body = JSON.stringify(value)
  response.writeHead(status, headers({
    'content-type': 'application/json',
    'content-length': Buffer.byteLength(body),
    ...extra,
  }))
  response.end(body)
}

function problem(response, status, code, detail, retryable = false) {
  sendJson(response, status, {
    code,
    detail,
    retryable,
    status,
    trace_id: traceId,
    title: 'Fixture Problem',
  })
}

function runView(selectedRunId = runId) {
  const empty = selectedRunId === emptyRunId
  const state = empty ? 'running' : runState
  const terminal = state === 'succeeded'
  return {
    schema_version: 1,
    run_id: selectedRunId,
    agent_deployment_id: deploymentId,
    state,
    version: terminal ? 2 : 1,
    input_value_id: 'val_0198f1c3-8f49-7c3e-b1f3-773c28367b94',
    output_value_id: terminal ? outputId : null,
    pause_generation: 0,
    cancel_generation: 0,
    deadline: '2030-01-01T00:00:00Z',
    started_at: '2026-08-29T00:00:00Z',
    terminal_at: terminal ? '2026-08-29T00:00:02Z' : null,
    created_at: '2026-08-29T00:00:00Z',
    updated_at: terminal ? '2026-08-29T00:00:02Z' : '2026-08-29T00:00:01Z',
    etag: terminal ? '"run-v2"' : '"run-v1"',
  }
}

function taskView() {
  return {
    schema_version: 1,
    task_id: taskId,
    task_kind: 'interaction',
    state: taskState,
    generation: 1,
    version: taskVersion,
    safe_prompt_key: 'interaction.confirm_release',
    response_schema_digest: `sha256:${'a'.repeat(64)}`,
    owner: { kind: 'run', run_id: runId },
    deadline: '2030-01-01T00:00:00Z',
    responded_at: taskState === 'responded' ? '2026-08-29T00:00:02Z' : null,
    created_at: '2026-08-29T00:00:01Z',
    updated_at: taskState === 'responded' ? '2026-08-29T00:00:02Z' : '2026-08-29T00:00:01Z',
    etag: taskState === 'responded' ? '"task-v2"' : '"task-v1"',
  }
}

async function readBody(request, maximum = 64 * 1024) {
  const chunks = []
  let size = 0
  for await (const chunk of request) {
    size += chunk.length
    if (size > maximum) throw new Error('request_too_large')
    chunks.push(chunk)
  }
  return Buffer.concat(chunks).toString('utf8')
}

function serveStatic(requestPath, response) {
  const relative = requestPath === '/' ? 'index.html' : requestPath.slice(1)
  const candidate = normalize(join(root, relative))
  const path = candidate.startsWith(`${root}/`) && existsSync(candidate) && statSync(candidate).isFile()
    ? candidate
    : join(root, 'index.html')
  const mediaType = {
    '.css': 'text/css; charset=utf-8',
    '.html': 'text/html; charset=utf-8',
    '.js': 'text/javascript; charset=utf-8',
  }[extname(path)] ?? 'application/octet-stream'
  response.writeHead(200, { 'content-type': mediaType, 'content-length': statSync(path).size })
  createReadStream(path).pipe(response)
}

const server = createServer(async (request, response) => {
  requestCount += 1
  const url = new URL(request.url ?? '/', `http://${host}:${port}`)
  const authorizationPresent = typeof request.headers.authorization === 'string'
  process.stdout.write(`${JSON.stringify({
    authorization_present: authorizationPresent,
    idempotency_key_present: typeof request.headers['idempotency-key'] === 'string',
    if_match: request.headers['if-match'] ?? null,
    last_event_id: request.headers['last-event-id'] ?? null,
    method: request.method,
    path: url.pathname,
    request_count: requestCount,
  })}\n`)

  if (url.pathname === '/readyz') {
    response.writeHead(200, { 'content-type': 'text/plain', 'content-length': 5 })
    response.end('ready')
    return
  }
  if (url.pathname.startsWith('/v1/') && !authorizationPresent) {
    problem(response, 401, 'authentication_required', 'A bearer token is required.')
    return
  }
  if (request.method === 'GET' && [runId, emptyRunId].some((id) => url.pathname === `/v1/runs/${id}`)) {
    if (slowResponseMilliseconds > 0) await delay(slowResponseMilliseconds)
    const selectedRunId = url.pathname.split('/').at(-1)
    sendJson(response, 200, runView(selectedRunId), { etag: runView(selectedRunId).etag })
    return
  }
  if (request.method === 'GET' && url.pathname === `/v1/runs/${emptyRunId}/events`) {
    response.writeHead(200, headers({
      'content-type': 'text/event-stream',
      'content-length': 0,
    }))
    response.end()
    return
  }
  if (request.method === 'GET' && url.pathname === `/v1/runs/${runId}/events`) {
    const cursor = request.headers['last-event-id']
    const events = cursor === 'cur-browser-1'
      ? runState === 'succeeded'
        ? 'id: cur-browser-2\nevent: run.succeeded\ndata: {"run_id":"' + runId + '","state":"succeeded","credential":"browser-secret-must-not-render"}\n\n'
        : ''
      : 'id: cur-browser-1\nevent: interaction.required\ndata: {"task_id":"' + taskId + '","safe_prompt_key":"interaction.confirm_release","access_token":"browser-token-must-not-render","raw_prompt":"browser-prompt-must-not-render"}\n\n'
    response.writeHead(200, headers({
      'content-type': 'text/event-stream',
      'content-length': Buffer.byteLength(events),
    }))
    response.end(events)
    return
  }
  if (request.method === 'GET' && url.pathname === `/v1/runs/${runId}/result`) {
    if (runState !== 'succeeded') {
      problem(response, 409, 'run_not_terminal', 'The Run has not reached a terminal state.')
      return
    }
    sendJson(response, 200, {
      schema_version: 1,
      run_id: runId,
      output_value_id: outputId,
      value: { message: 'completed', tool_output: 'browser-tool-output-must-not-render' },
    })
    return
  }
  if (request.method === 'POST' && url.pathname.startsWith(`/v1/runs/${runId}:`)) {
    const action = url.pathname.split(':').at(-1)
    if (action === 'pause') return problem(response, 409, 'invalid_state_transition', 'Run cannot pause while waiting.')
    if (action === 'cancel') return problem(response, 412, 'precondition_failed', 'Run ETag changed before cancellation.')
    return problem(response, 429, 'capacity_exhausted', 'Control capacity is temporarily exhausted.', true)
  }
  if (request.method === 'GET' && url.pathname === `/v1/tasks/${taskId}`) {
    sendJson(response, 200, taskView(), { etag: taskView().etag })
    return
  }
  if (request.method === 'POST' && url.pathname === `/v1/tasks/${taskId}:submit-input`) {
    if (request.headers['if-match'] !== '"task-v1"') {
      problem(response, 412, 'precondition_failed', 'Task ETag is stale.')
      return
    }
    if (!String(request.headers['idempotency-key'] ?? '').startsWith('console-task-submit-input-')) {
      problem(response, 409, 'idempotency_conflict', 'Receipt is missing or invalid.')
      return
    }
    JSON.parse(await readBody(request))
    taskState = 'responded'
    taskVersion = 2
    runState = 'succeeded'
    sendJson(response, 200, taskView(), { etag: taskView().etag })
    return
  }
  if (request.method === 'GET' && url.pathname === '/v1/operations/job_capacity') {
    problem(response, 429, 'capacity_exhausted', 'Operation read capacity is temporarily exhausted.', true)
    return
  }
  if (url.pathname.startsWith('/v1/')) {
    problem(response, 404, 'not_found', 'Fixture authority object not found.')
    return
  }
  serveStatic(url.pathname, response)
})

if (!existsSync(join(root, 'index.html'))) {
  throw new Error('Build web/console before starting the browser fixture')
}

server.listen(port, host, () => {
  process.stdout.write(`console fixture ready http://${host}:${port} run=${runId} task=${taskId}\n`)
})

for (const signal of ['SIGINT', 'SIGTERM']) {
  process.on(signal, () => server.close(() => process.exit(0)))
}
