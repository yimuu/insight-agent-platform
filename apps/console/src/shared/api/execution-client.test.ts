import { test } from 'node:test'
import assert from 'node:assert/strict'
import { PlatformClient } from './client.ts'

test('execution reads scope both Run and source, preserve abort and use no-store cookie auth', async (context) => {
  let captured: { url: string; init: RequestInit } | undefined
  context.mock.method(globalThis, 'fetch', async (url: string, init: RequestInit) => {
    captured = { url, init }
    return new Response(JSON.stringify({ source_id: 'source' }), { status: 200 })
  })
  const client = new PlatformClient('https://platform.example', '', 'cookie')
  const controller = new AbortController()
  await client.getExecutionDetail('run/exact', 'model_turn', 'source/exact', controller.signal)
  assert.ok(captured!.url.endsWith('/runs/run%2Fexact/executions/model_turn/source%2Fexact'))
  assert.equal(captured!.init.body, undefined)
  assert.equal(captured!.init.cache, 'no-store')
  assert.equal(captured!.init.credentials, 'same-origin')
  controller.abort()
  assert.equal(captured!.init.signal!.aborted, true)
})
