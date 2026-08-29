import test from 'node:test'
import assert from 'node:assert/strict'
import { PlatformClient, PlatformProblem } from './client.ts'

test('client normalizes /v1 endpoint and sends bounded public auth headers', async (context) => {
  const calls = []
  context.mock.method(globalThis, 'fetch', async (url, init) => {
    calls.push({ url, init })
    return new Response(JSON.stringify({ run_id: 'run_example' }), { status: 200, headers: { etag: '"v1"', 'trace-id': '0123456789abcdef0123456789abcdef' } })
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
  context.mock.method(globalThis, 'fetch', async (_url, requestInit) => { init = requestInit; return new Response('ready', { status: 200 }) })
  const client = new PlatformClient('http://127.0.0.1:8080', 'private-token')
  assert.equal(await client.readiness(), true)
  assert.equal(new Headers(init.headers).has('authorization'), false)
})

test('closed problem preserves code, retryability, and trace without exposing arbitrary body', async (context) => {
  context.mock.method(globalThis, 'fetch', async () => new Response(JSON.stringify({ code: 'capacity_exhausted', detail: 'Try later', retryable: true, trace_id: 'fedcba9876543210fedcba9876543210' }), { status: 429 }))
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
  context.mock.method(globalThis, 'fetch', async (_url, requestInit) => { init = requestInit; return new Response('{}', { status: 200 }) })
  const client = new PlatformClient('https://platform.example', 'token')
  await client.taskAction('int_example', 'approve', '"task-v3"', 'console-receipt')
  assert.equal(init.method, 'POST')
  assert.equal(init.headers.get('if-match'), '"task-v3"')
  assert.equal(init.headers.get('idempotency-key'), 'console-receipt')
})
