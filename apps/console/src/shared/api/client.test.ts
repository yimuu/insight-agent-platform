import test from 'node:test'
import assert from 'node:assert/strict'
import { PlatformClient, PlatformProblem } from './client.ts'

test('Run signal uses its exact 204 public port with stable Receipt and AbortSignal', async (context) => {
  const requests = []
  let status = 204
  context.mock.method(globalThis, 'fetch', async (url, init) => {
    requests.push({ url, init })
    return new Response(status === 204 ? null : '{}', { status })
  })
  const client = new PlatformClient('https://platform.example', 'token')
  const controller = new AbortController()
  const body = {
    payload: {
      classification: 'internal',
      schema_digest: `sha256:${'a'.repeat(64)}`,
      value: { kind: 'inline', value: { approved: false } },
    },
  }
  for (let i = 0; i < 2; i++)
    assert.equal(
      (
        await client.signalRun('run_/exact', 'approval', body, 'same-receipt', {
          signal: controller.signal,
        })
      ).data,
      null,
    )
  assert.ok(
    requests.every(
      ({ url, init }) =>
        url.endsWith('/runs/run_%2Fexact/signals/approval') &&
        init.method === 'POST' &&
        init.headers.get('idempotency-key') === 'same-receipt' &&
        !init.headers.has('if-match') &&
        init.signal instanceof AbortSignal &&
        !init.signal.aborted &&
        init.cache === 'no-store',
    ),
  )
  assert.deepEqual(JSON.parse(requests[0].init.body), body)
  status = 200
  await assert.rejects(
    client.signalRun('run_exact', 'approval', { payload: null }, 'same-receipt'),
    /success status/,
  )
})

test('client normalizes /v1 endpoint and sends bounded public auth headers', async (context) => {
  const calls = []
  context.mock.method(globalThis, 'fetch', async (url, init) => {
    calls.push({ url, init })
    return new Response(JSON.stringify({ run_id: 'run_example' }), {
      status: 200,
      headers: { etag: '"v1"', 'trace-id': '0123456789abcdef0123456789abcdef' },
    })
  })
  const client = new PlatformClient('https://platform.example/v1', 'memory-only-token')
  const response = await client.getRun('run_example')
  assert.equal(calls[0].url, 'https://platform.example/v1/runs/run_example')
  assert.equal(calls[0].init.headers.get('authorization'), 'Bearer memory-only-token')
  assert.equal(calls[0].init.credentials, 'omit')
  assert.equal(calls[0].init.redirect, 'error')
  assert.equal(response.etag, '"v1"')
})

test('readiness never sends the OIDC token', async (context) => {
  let init
  context.mock.method(globalThis, 'fetch', async (_url, requestInit) => {
    init = requestInit
    return new Response('ready', { status: 200 })
  })
  const client = new PlatformClient('http://127.0.0.1:8080', 'private-token')
  assert.equal(await client.readiness(), true)
  assert.equal(new Headers(init.headers).has('authorization'), false)
})

test('closed problem preserves code, retryability, and trace without exposing arbitrary body', async (context) => {
  context.mock.method(
    globalThis,
    'fetch',
    async () =>
      new Response(
        JSON.stringify({
          code: 'capacity_exhausted',
          detail: 'Try later',
          retryable: true,
          trace_id: 'fedcba9876543210fedcba9876543210',
        }),
        { status: 429 },
      ),
  )
  const client = new PlatformClient('https://platform.example', '')
  await assert.rejects(client.getTask('int_example'), (error) => {
    assert.ok(error instanceof PlatformProblem)
    assert.equal(error.status, 429)
    assert.equal(error.code, 'capacity_exhausted')
    assert.equal(error.retryable, true)
    assert.equal(error.traceId, 'fedcba9876543210fedcba9876543210')
    return true
  })
})

test('task mutations send exact If-Match and Receipt headers', async (context) => {
  let init
  context.mock.method(globalThis, 'fetch', async (_url, requestInit) => {
    init = requestInit
    return new Response('{}', { status: 200 })
  })
  const client = new PlatformClient('https://platform.example', 'token')
  await client.taskAction('int_example', 'approve', '"task-v3"', 'console-receipt')
  assert.equal(init.method, 'POST')
  assert.equal(init.headers.get('if-match'), '"task-v3"')
  assert.equal(init.headers.get('idempotency-key'), 'console-receipt')
})

test('product lists keep opaque cursors in protocol metadata', async (context) => {
  let calledUrl
  context.mock.method(globalThis, 'fetch', async (url) => {
    calledUrl = url
    return new Response(JSON.stringify({ schema_version: 1, items: [], next_cursor: null }), {
      status: 200,
    })
  })
  const client = new PlatformClient('https://platform.example', 'token')
  await client.listAgents('opaque+/cursor==')
  assert.equal(
    calledUrl,
    'https://platform.example/v1/agents?page_size=25&cursor=opaque%2B%2Fcursor%3D%3D',
  )
})

test('Task inbox/form use exact filters, preserve empty-page continuation and pass cancellation', async (context) => {
  const calls = []
  context.mock.method(globalThis, 'fetch', async (url, init) => {
    calls.push({ url, init })
    return new Response(
      JSON.stringify({ schema_version: 1, items: [], next_cursor: 'opaque+/next==' }),
      { status: 200 },
    )
  })
  const client = new PlatformClient('https://platform.example', 'token')
  const abort = new AbortController()
  const page = await client.listTasks(
    { state: 'pending', kind: 'human_work', runId: 'run_id', cursor: 'opaque+/cursor==' },
    { signal: abort.signal },
  )
  assert.equal(
    calls[0].url,
    'https://platform.example/v1/tasks?page_size=25&state=pending&kind=human_work&run_id=run_id&cursor=opaque%2B%2Fcursor%3D%3D',
  )
  assert.equal(page.data.next_cursor, 'opaque+/next==')
  await client.getTask('int_/id', { signal: abort.signal })
  await client.getTaskForm('int_/id', { signal: abort.signal })
  await client.taskAction(
    'int_/id',
    'submit-input',
    '"task-v2"',
    'stable-receipt',
    {
      classification: 'internal',
      schema_digest: 'digest',
      value: { kind: 'inline', value: { accepted: false } },
    },
    { signal: abort.signal },
  )
  assert.equal(calls[1].url, 'https://platform.example/v1/tasks/int_%2Fid?purpose=respondable')
  assert.equal(calls[2].url, 'https://platform.example/v1/tasks/int_%2Fid/form')
  assert.ok(
    calls.every(
      ({ init }) =>
        init.signal instanceof AbortSignal && !init.signal.aborted && init.cache === 'no-store',
    ),
  )
  assert.equal(JSON.parse(calls[3].init.body).value.value.accepted, false)
})

test('signed Artifact upload omits bearer authority and rejects insecure targets', async (context) => {
  let init
  context.mock.method(globalThis, 'fetch', async (_url, requestInit) => {
    init = requestInit
    return new Response('', { status: 200 })
  })
  const client = new PlatformClient('https://platform.example', 'memory-only-token')
  await client.putArtifactObject(
    'https://objects.example/upload?signature=secret',
    new Uint8Array([1, 2]),
    'application/json',
  )
  assert.equal(new Headers(init.headers).has('authorization'), false)
  assert.equal(init.credentials, 'omit')
  await assert.rejects(
    client.putArtifactObject(
      'http://objects.example/upload',
      new Uint8Array([1]),
      'application/json',
    ),
    /unsafe target/,
  )
})

test('disposing a session cancels requests even when the caller has its own abort signal', async (context) => {
  let observed: AbortSignal | null = null
  context.mock.method(
    globalThis,
    'fetch',
    (_url: string, init: RequestInit) =>
      new Promise<Response>((_resolve, reject) => {
        observed = init.signal!
        observed.addEventListener('abort', () => reject(observed!.reason), { once: true })
      }),
  )
  const client = new PlatformClient('https://platform.example', 'private-token')
  const caller = new AbortController()
  const pending = client.getRun('run_example', { signal: caller.signal })
  client.dispose()
  await assert.rejects(pending, { name: 'AbortError' })
  assert.equal(observed!.aborted, true)
  assert.equal(caller.signal.aborted, false)
})
