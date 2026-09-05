import { createReadStream, existsSync, statSync } from 'node:fs'
import { createHash } from 'node:crypto'
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
const agentId = 'agt_0198f1c3-8f49-7c3e-b1f3-773c28367ba0'
const authoredRunId = 'run_0198f1c3-8f49-7c3e-b1f3-773c28367ba1'
const agentDeploymentId = 'adep_0198f1c3-8f49-7c3e-b1f3-773c28367ba2'
const policyRevisionId = 'prev_0198f1c3-8f49-7c3e-b1f3-773c28367ba3'
const policyDeploymentId = 'pdep_0198f1c3-8f49-7c3e-b1f3-773c28367ba4'
let taskState = 'pending'
let runState = 'waiting'
let taskVersion = 1
let runReadsAfterTaskResponse = 0
let postTaskRunReadInFlight = false
let authoredAgentReady = false
let authoredDocument = null
let authoredDisplayName = 'Hello Agent'
let authoredValidation = null
let authoredResourceEtag = '"agent-v1"'
const preparedArtifacts = new Map()
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

function canonical(value) {
  if (Array.isArray(value)) return `[${value.map(canonical).join(',')}]`
  if (value !== null && typeof value === 'object') {
    return `{${Object.keys(value).sort().map((key) => `${JSON.stringify(key)}:${canonical(value[key])}`).join(',')}}`
  }
  return JSON.stringify(value)
}

const digest = (value) => `sha256:${createHash('sha256').update(typeof value === 'string' ? value : canonical(value)).digest('hex')}`
const policyBinding = {
  deployment: { deployment_id: policyDeploymentId, resource_kind: 'policy_deployment', deployment_digest: `sha256:${'b'.repeat(64)}` },
  revision: { revision_id: policyRevisionId, resource_kind: 'policy_revision', semantic_digest: `sha256:${'c'.repeat(64)}` },
}
const authoringProfile = {
  schema_version: 1,
  default_deadline_seconds: 120,
  default_environment: 'development',
  policy_versions: [policyBinding.revision],
  deployment_policies: [policyBinding],
  execution_profile: policyBinding,
  model_loop: { maximum_rounds: 1, maximum_capability_calls: 1, maximum_parallel_calls_per_round: 1, token_budget: 2304 },
  models: [],
}
authoringProfile.profile_digest = digest(authoringProfile)

function authoredResource() {
  return {
    schema_version: 1,
    resource_id: agentId,
    resource_kind: 'agent',
    lifecycle_state: 'active',
    gate_state: 'enabled',
    draft_generation: 1,
    version: Number(authoredResourceEtag.match(/\d+/)?.[0] ?? 1),
    draft: { display_name: authoredDisplayName, document: authoredDocument, validation: authoredValidation },
    etag: authoredResourceEtag,
  }
}

function authoredRunView() {
  return {
    schema_version: 1,
    run_id: authoredRunId,
    agent_deployment_id: agentDeploymentId,
    state: 'succeeded',
    version: 2,
    input_value_id: 'val_0198f1c3-8f49-7c3e-b1f3-773c28367ba5',
    output_value_id: 'val_0198f1c3-8f49-7c3e-b1f3-773c28367ba6',
    pause_generation: 0,
    cancel_generation: 0,
    deadline: '2030-01-01T00:00:00Z',
    started_at: '2026-09-01T00:00:00Z',
    terminal_at: '2026-09-01T00:00:01Z',
    created_at: '2026-09-01T00:00:00Z',
    updated_at: '2026-09-01T00:00:01Z',
    etag: '"authored-run-v2"',
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
  if (request.method === 'GET' && url.pathname === '/v1/agent-authoring-profile') {
    sendJson(response, 200, authoringProfile)
    return
  }
  if (request.method === 'GET' && url.pathname === '/v1/agents') {
    sendJson(response, 200, {
      schema_version: 1,
      items: authoredAgentReady ? [{
        schema_version: 1,
        name: 'hello-agent',
        display_name: authoredDisplayName,
        agent_id: agentId,
        state: 'ready',
        environment: 'development',
        updated_at: '2026-09-01T00:00:02Z',
        published_at: '2026-09-01T00:00:02Z',
        required_features: [],
        latest_run_state: null,
      }] : [],
      next_cursor: null,
    })
    return
  }
  if (request.method === 'POST' && url.pathname === '/v1/artifacts:prepare-upload') {
    const body = JSON.parse(await readBody(request))
    const index = body.purpose === 'authoring_document' ? 1 : 2
    const artifactId = `art_0198f1c3-8f49-7c3e-b1f3-773c28367ba${index + 6}`
    const operationId = `job_0198f1c3-8f49-7c3e-b1f3-773c28367ba${index + 8}`
    preparedArtifacts.set(artifactId, { ...body, operationId })
    sendJson(response, 201, {
      schema_version: 1,
      artifact_id: artifactId,
      operation_id: operationId,
      upload_grant_id: `grt_0198f1c3-8f49-7c3e-b1f3-773c28367bb${index}`,
      artifact_etag: '"artifact-v1"',
      upload_target: { url: `https://objects.example/${artifactId}`, completion_proof: `proof-${index}` },
      upload_expires_at: '2030-01-01T00:00:00Z',
    }, { etag: '"artifact-v1"' })
    return
  }
  const completeArtifact = url.pathname.match(/^\/v1\/artifacts\/(art_[^/]+):complete-upload$/)
  if (request.method === 'POST' && completeArtifact) {
    await readBody(request)
    const prepared = preparedArtifacts.get(completeArtifact[1])
    sendJson(response, 202, { schema_version: 1, artifact_id: completeArtifact[1], artifact_etag: '"artifact-v2"', operation_id: prepared.operationId })
    return
  }
  const readArtifact = url.pathname.match(/^\/v1\/artifacts\/(art_[^/]+)$/)
  if (request.method === 'GET' && readArtifact && preparedArtifacts.has(readArtifact[1])) {
    const prepared = preparedArtifacts.get(readArtifact[1])
    sendJson(response, 200, {
      schema_version: 1,
      artifact_id: readArtifact[1],
      purpose: prepared.purpose,
      classification: prepared.classification,
      state: 'ready',
      version: 2,
      expected_size_bytes: prepared.expected_size_bytes,
      declared_media_type: prepared.declared_media_type,
      verified_media_type: prepared.declared_media_type,
      content: {
        artifact_id: readArtifact[1],
        content_digest: prepared.expected_digest,
        byte_length: prepared.expected_size_bytes,
        media_type: prepared.declared_media_type,
        classification: prepared.classification,
        display_name: prepared.display_name,
      },
      retain_until: '2030-01-01T00:00:00Z',
      created_at: '2026-09-01T00:00:00Z',
      updated_at: '2026-09-01T00:00:01Z',
      etag: '"artifact-v2"',
    })
    return
  }
  if (request.method === 'POST' && url.pathname === '/v1/agents') {
    const body = JSON.parse(await readBody(request))
    authoredDisplayName = body.display_name
    authoredDocument = body.document
    authoredResourceEtag = '"agent-v1"'
    sendJson(response, 201, authoredResource(), { etag: authoredResourceEtag })
    return
  }
  if (request.method === 'POST' && url.pathname === `/v1/agents/${agentId}/draft:validate`) {
    authoredValidation = { validator_digest: `sha256:${'d'.repeat(64)}` }
    authoredResourceEtag = '"agent-v2"'
    sendJson(response, 202, { operation_id: 'job_0198f1c3-8f49-7c3e-b1f3-773c28367bb3', state: 'ready' })
    return
  }
  if (request.method === 'GET' && url.pathname === `/v1/agents/${agentId}`) {
    sendJson(response, 200, authoredResource(), { etag: authoredResourceEtag })
    return
  }
  if (request.method === 'POST' && url.pathname === `/v1/agents/${agentId}/draft:publish`) {
    const body = JSON.parse(await readBody(request))
    authoredResourceEtag = '"agent-v3"'
    sendJson(response, 200, {
      schema_version: 1,
      resource_id: agentId,
      resource_kind: 'agent',
      draft_generation: 1,
      version: 3,
      published_versions: [
        { resource_version_id: 'aif_0198f1c3-8f49-7c3e-b1f3-773c28367bb4', revision_no: 1, content_digest: body.interface_content_digest, artifact_id: body.artifact_id, etag: '"interface"' },
        { resource_version_id: 'arev_0198f1c3-8f49-7c3e-b1f3-773c28367bb5', revision_no: 1, content_digest: body.plan_content_digest, artifact_id: body.artifact_id, etag: '"plan"' },
      ],
      etag: authoredResourceEtag,
    }, { etag: authoredResourceEtag })
    return
  }
  if (request.method === 'POST' && url.pathname === `/v1/agents/${agentId}/deployments`) {
    const body = JSON.parse(await readBody(request))
    authoredResourceEtag = '"agent-v4"'
    sendJson(response, 201, { schema_version: 1, deployment_id: agentDeploymentId, resource_id: agentId, resource_kind: 'agent', resource_version_id: body.resource_version_id, environment: body.environment, closure_digest: `sha256:${'e'.repeat(64)}`, closure: body.closure, created_at: '2026-09-01T00:00:02Z', etag: '"deployment"' })
    return
  }
  if (request.method === 'POST' && url.pathname === `/v1/agents/${agentId}/deployments/${agentDeploymentId}:activate`) {
    authoredAgentReady = true
    authoredResourceEtag = '"agent-v5"'
    sendJson(response, 200, authoredResource(), { etag: authoredResourceEtag })
    return
  }
  if (request.method === 'POST' && url.pathname === '/v1/runs') {
    await readBody(request)
    sendJson(response, 201, authoredRunView(), { etag: authoredRunView().etag })
    return
  }
  if (request.method === 'GET' && url.pathname === `/v1/runs/${authoredRunId}`) {
    sendJson(response, 200, authoredRunView(), { etag: authoredRunView().etag })
    return
  }
  if (request.method === 'GET' && url.pathname === `/v1/runs/${authoredRunId}/events`) {
    response.writeHead(200, headers({ 'content-type': 'text/event-stream', 'content-length': 0 }))
    response.end()
    return
  }
  if (request.method === 'GET' && url.pathname === `/v1/runs/${authoredRunId}/result`) {
    sendJson(response, 200, { schema_version: 1, run_id: authoredRunId, value: { kind: 'inline', value: { message: 'north-star-complete' } } })
    return
  }
  const operationRead = url.pathname.match(/^\/v1\/operations\/(job_[^/]+)$/)
  if (request.method === 'GET' && operationRead && operationRead[1] !== 'job_capacity') {
    sendJson(response, 200, { operation_id: operationRead[1], tenant_id: 'ten_0198f1c3-8f49-7c3e-b1f3-773c28367bbc', kind: 'resource_validation', target: { kind: 'agent', agent_id: agentId }, state: 'succeeded', progress: null, result: { result_digest: `sha256:${'f'.repeat(64)}` }, error: null, created_at: '2026-09-01T00:00:00Z', updated_at: '2026-09-01T00:00:01Z', etag: '"operation-v2"' })
    return
  }
  if (request.method === 'GET' && [runId, emptyRunId].some((id) => url.pathname === `/v1/runs/${id}`)) {
    const selectedRunId = url.pathname.split('/').at(-1)
    const postTaskRunRead = selectedRunId === runId && taskState === 'responded'
    if (postTaskRunRead && postTaskRunReadInFlight) {
      process.stdout.write(`${JSON.stringify({ fixture_observation: 'overlapping_post_task_run_read' })}\n`)
      problem(response, 409, 'overlapping_run_read', 'Run authority reads must complete before the next refresh.')
      return
    }
    if (postTaskRunRead) postTaskRunReadInFlight = true
    try {
      if (slowResponseMilliseconds > 0) await delay(slowResponseMilliseconds)
      if (postTaskRunRead && runState === 'running') {
        runReadsAfterTaskResponse += 1
        if (runReadsAfterTaskResponse >= 2) runState = 'succeeded'
      }
      const view = runView(selectedRunId)
      sendJson(response, 200, view, { etag: view.etag })
      if (postTaskRunRead) {
        process.stdout.write(`${JSON.stringify({
          fixture_observation: 'post_task_run_response',
          run_read_ordinal: runReadsAfterTaskResponse,
          state: view.state,
        })}\n`)
      }
    } finally {
      if (postTaskRunRead) postTaskRunReadInFlight = false
    }
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
    runState = 'running'
    runReadsAfterTaskResponse = 0
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
