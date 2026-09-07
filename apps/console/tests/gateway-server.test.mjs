import assert from 'node:assert/strict'
import { mkdtempSync, mkdirSync, readFileSync, rmSync, symlinkSync, writeFileSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import test from 'node:test'
import { checkedBundleRoot } from './gateway-server.mjs'

test('accepts an explicit regular candidate bundle root', () => {
  const root = mkdtempSync(join(tmpdir(), 'insight-console-candidate-'))
  writeFileSync(join(root, 'index.html'), '<!doctype html><title>candidate console</title>')
  try {
    const checked = checkedBundleRoot(root)
    assert.match(readFileSync(join(checked, 'index.html'), 'utf8'), /candidate console/)
  } finally {
    rmSync(root, { recursive: true })
  }
})

test('rejects symbolic links anywhere in a candidate bundle', () => {
  const root = mkdtempSync(join(tmpdir(), 'insight-console-candidate-'))
  const outside = mkdtempSync(join(tmpdir(), 'insight-console-outside-'))
  try {
    writeFileSync(join(root, 'index.html'), '<!doctype html>')
    mkdirSync(join(root, 'assets'))
    writeFileSync(join(outside, 'asset.js'), 'throw new Error("outside")')
    symlinkSync(join(outside, 'asset.js'), join(root, 'assets', 'asset.js'))
    assert.throws(
      () => checkedBundleRoot(root),
      /must not contain symbolic links/,
    )
  } finally {
    rmSync(root, { recursive: true })
    rmSync(outside, { recursive: true })
  }
})

test('transparent local proxy preserves both physical Gateway surfaces and opaque command/SSE headers', async () => {
  const { createServer } = await import('node:http')
  const { startGatewayConsoleServer } = await import('./gateway-server.mjs')
  const root = mkdtempSync(join(tmpdir(), 'insight-console-dual-gateway-'))
  writeFileSync(join(root, 'index.html'), '<!doctype html>')
  const calls = []
  const upstream = (surface) => createServer(async (request, response) => {
    const chunks = []
    for await (const chunk of request) chunks.push(chunk)
    calls.push({ surface, method: request.method, path: request.url, headers: request.headers, body: Buffer.concat(chunks).toString() })
    if (request.url.startsWith('/v1/unknown')) { response.writeHead(404, { 'cache-control': 'no-store', etag: '"actual-404"' }); response.end('actual upstream missing'); return }
    if (request.url.startsWith('/v1/runs/run/events')) {
      response.writeHead(200, { 'content-type': 'text/event-stream', 'cache-control': 'no-store', 'x-insight-run-replay-floor': '0', 'x-insight-run-high-water': '1' })
      response.write('id: opaque+cursor\nevent: run.created\n')
      response.end('data: {"actual":true}\n\n')
      return
    }
    response.writeHead(200, { 'cache-control': 'no-store', etag: '"actual-upstream-etag"', 'content-type': 'application/json' })
    response.end(JSON.stringify({ surface }))
  })
  const runtime = upstream('runtime')
  const management = upstream('management')
  let proxy
  try {
    await Promise.all([runtime, management].map((server) => new Promise((resolve) => server.listen(0, '127.0.0.1', resolve))))
    proxy = await startGatewayConsoleServer({ bundleRoot: root, gatewayOrigin: `http://127.0.0.1:${runtime.address().port}`, managementGatewayOrigin: `http://127.0.0.1:${management.address().port}` })
    for (const [path, method, surface] of [
      ['/v1/agent-authoring-profile', 'GET', 'management'], ['/v1/agents?cursor=opaque%2B%2F', 'GET', 'management'],
      ['/v1/agent-authoring-bindings:resolve', 'POST', 'management'], ['/v1/agents/agent:publish', 'POST', 'management'],
      ['/v1/operations/job', 'GET', 'management'], ['/v1/tasks/task:submit-input', 'POST', 'runtime'], ['/v1/artifacts/art:upload', 'POST', 'runtime'],
    ]) {
      const headers = { authorization: 'Bearer actual-test-token', 'idempotency-key': 'same-key', 'if-match': '"same-etag"', 'content-type': 'application/json' }
      const response = await fetch(`${proxy.origin}${path}`, { method, headers, ...(method === 'POST' ? { body: '{"exact":true}' } : {}) })
      assert.equal(response.status, 200)
      assert.equal(response.headers.get('etag'), '"actual-upstream-etag"')
      assert.equal(response.headers.get('cache-control'), 'no-store')
      assert.deepEqual(await response.json(), { surface })
      const forwarded = calls.at(-1)
      assert.equal(forwarded.path, path)
      assert.equal(forwarded.method, method)
      for (const name of ['authorization', 'idempotency-key', 'if-match']) assert.equal(forwarded.headers[name], headers[name])
      if (method === 'POST') assert.equal(forwarded.body, '{"exact":true}')
    }
    const response = await fetch(`${proxy.origin}/v1/runs/run/events`, { headers: { 'last-event-id': 'opaque+/cursor=' } })
    assert.equal(calls.at(-1).surface, 'runtime')
    assert.equal(calls.at(-1).headers['last-event-id'], 'opaque+/cursor=')
    assert.equal(response.headers.get('x-insight-run-high-water'), '1')
    assert.equal(await response.text(), 'id: opaque+cursor\nevent: run.created\ndata: {"actual":true}\n\n')
    const missing = await fetch(`${proxy.origin}/v1/unknown`)
    assert.equal(missing.status, 404)
    assert.equal(missing.headers.get('etag'), '"actual-404"')
    assert.equal(await missing.text(), 'actual upstream missing')
    for (const unsafe of ['https://127.0.0.1:80', 'http://example.com', 'http://user@127.0.0.1', 'http://127.0.0.1/path']) {
      await assert.rejects(startGatewayConsoleServer({ bundleRoot: root, gatewayOrigin: proxy.origin, managementGatewayOrigin: unsafe }), /origin-only loopback/)
    }
  } finally {
    if (proxy) await proxy.close()
    await Promise.all([runtime, management].map((server) => new Promise((resolve) => server.close(resolve))))
    rmSync(root, { recursive: true })
  }
})

test('disconnecting a Console event reader closes the actual upstream stream', async () => {
  const { createServer, get } = await import('node:http')
  const { startGatewayConsoleServer } = await import('./gateway-server.mjs')
  const root = mkdtempSync(join(tmpdir(), 'insight-console-disconnect-'))
  writeFileSync(join(root, 'index.html'), '<!doctype html>')
  let observeClose
  const upstreamClosed = new Promise((resolve) => { observeClose = resolve })
  const upstream = createServer((_request, response) => {
    response.once('close', observeClose)
    response.writeHead(200, { 'content-type': 'text/event-stream', 'cache-control': 'no-store' })
    response.write('data: {"committed":true}\n\n')
  })
  let proxy
  let client
  let timer
  try {
    await new Promise((resolve) => upstream.listen(0, '127.0.0.1', resolve))
    const origin = `http://127.0.0.1:${upstream.address().port}`
    proxy = await startGatewayConsoleServer({ bundleRoot: root, gatewayOrigin: origin, managementGatewayOrigin: origin })
    await new Promise((resolve, reject) => {
      client = get(`${proxy.origin}/v1/runs/run/events`, (response) => {
        response.once('error', reject)
        response.once('data', (bytes) => {
          assert.equal(bytes.toString(), 'data: {"committed":true}\n\n')
          response.destroy()
          resolve()
        })
      })
      client.once('error', reject)
    })
    await Promise.race([
      upstreamClosed,
      new Promise((_, reject) => { timer = setTimeout(() => reject(new Error('disconnected reader retained its upstream stream')), 2_000) }),
    ])
  } finally {
    clearTimeout(timer)
    client?.destroy()
    upstream.closeAllConnections()
    if (proxy) await proxy.close()
    await new Promise((resolve) => upstream.close(resolve))
    rmSync(root, { recursive: true })
  }
})
