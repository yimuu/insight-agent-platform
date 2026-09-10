import assert from 'node:assert/strict'
import { mkdtempSync, rmSync, writeFileSync } from 'node:fs'
import { createServer, request } from 'node:http'
import { connect } from 'node:net'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import test from 'node:test'
import { nativeTransportLimits } from './config.mjs'
import { startConsoleServer } from './gateway-server.mjs'

const listen = (server) => new Promise((resolve) => server.listen(0, '127.0.0.1', resolve))
const wait = (ms) => new Promise((resolve) => setTimeout(resolve, ms))
async function fixture(t, handler, overrides = {}) {
  const root = mkdtempSync(join(tmpdir(), 'insight-console-transport-'))
  writeFileSync(join(root, 'index.html'), '<!doctype html><title>actual bundle</title>')
  const gateway = createServer(handler)
  await listen(gateway)
  const origin = `http://127.0.0.1:${gateway.address().port}`
  const proxy = await startConsoleServer({ bundleRoot: root, config: {
    schema_version: 1, topology: 'native', listen_host: '127.0.0.1', listen_port: 0,
    runtime_origin: origin, management_origin: origin, ...nativeTransportLimits, ...overrides,
  } })
  t.after(async () => {
    await proxy.close()
    gateway.closeAllConnections()
    await new Promise((resolve) => gateway.close(resolve))
    rmSync(root, { recursive: true })
  })
  return proxy
}

function send(origin, { path = '/v1/agents', method = 'POST', headers = {}, chunks = [] } = {}) {
  return new Promise((resolve, reject) => {
    const client = request(`${origin}${path}`, { method, headers, agent: false }, (response) => {
      const chunks = []
      response.on('data', (chunk) => chunks.push(chunk))
      response.once('end', () => resolve({ status: response.statusCode, headers: response.headers, body: Buffer.concat(chunks).toString() }))
      response.once('error', reject)
    })
    client.once('error', reject)
    for (const chunk of chunks) client.write(chunk)
    client.end()
  })
}

test('body overflow refuses both declared and chunked mutations before opening any upstream request', async (t) => {
  let calls = 0
  const proxy = await fixture(t, (_request, response) => { calls++; response.end('unexpected') }, { max_request_bytes: 8 })
  for (const headers of [{ 'content-length': '9' }, { 'transfer-encoding': 'chunked' }]) {
    const reply = await send(proxy.origin, { headers, chunks: ['1234', '56789'] })
    assert.equal(reply.status, 413)
    assert.equal(JSON.parse(reply.body).code, 'request_too_large')
  }
  assert.equal(calls, 0)
})

test('whole-process buffering rejects concurrent body admission and releases capacity after cancellation', async (t) => {
  let calls = 0
  const proxy = await fixture(t, (input, response) => { calls++; input.resume(); input.once('end', () => response.end('accepted')) },
    { max_request_bytes: 16384, max_buffered_request_bytes: 16384 })
  const unfinished = request(`${proxy.origin}/v1/agents`, { method: 'POST', headers: { 'content-length': '2' }, agent: false })
  unfinished.on('error', () => {})
  unfinished.write('a')
  t.after(() => unfinished.destroy())
  await wait(30)
  const saturated = await send(proxy.origin, { chunks: ['b'] })
  assert.equal(saturated.status, 503)
  assert.equal(JSON.parse(saturated.body).code, 'transport_capacity_exhausted')
  assert.equal(calls, 0)
  unfinished.destroy()
  await wait(30)
  const next = await send(proxy.origin, { chunks: ['c'] })
  assert.equal(next.status, 200)
  assert.equal(next.body, 'accepted')
  assert.equal(calls, 1)
})

test('request-body deadline does not issue a partial upstream mutation', async (t) => {
  let calls = 0
  const proxy = await fixture(t, (_request, response) => { calls++; response.end() }, { request_timeout_ms: 60 })
  const reply = await new Promise((resolve, reject) => {
    const client = request(`${proxy.origin}/v1/agents`, { method: 'POST', headers: { 'content-length': '2' }, agent: false }, (response) => {
      response.resume()
      response.once('end', () => { client.destroy(); resolve(response.statusCode) })
    })
    client.once('error', reject)
    client.write('a')
  })
  assert.equal(reply, 408)
  assert.equal(calls, 0)
})

test('Gateway response-header deadline closes the one attempted mutation without retry', async (t) => {
  let calls = 0
  let closed
  const upstreamClosed = new Promise((resolve) => { closed = resolve })
  const proxy = await fixture(t, (input, response) => {
    calls++
    input.resume()
    response.once('close', closed)
  }, { upstream_header_timeout_ms: 50 })
  const reply = await send(proxy.origin, { chunks: ['complete mutation'] })
  assert.equal(reply.status, 504)
  assert.equal(JSON.parse(reply.body).code, 'gateway_header_timeout')
  await upstreamClosed
  assert.equal(calls, 1)
})

test('lost mutation response is not retried and upstream failures disclose no raw details', async (t) => {
  let calls = 0
  const proxy = await fixture(t, (input) => {
    calls++
    input.resume()
    input.once('end', () => input.socket.destroy())
  })
  const reply = await send(proxy.origin, { headers: { authorization: 'Bearer private', 'idempotency-key': 'same-request' }, chunks: ['complete mutation'] })
  assert.equal(reply.status, 503)
  assert.equal(JSON.parse(reply.body).code, 'gateway_unavailable')
  assert.doesNotMatch(reply.body, /private|ECONNRESET|127\.0\.0\.1/)
  assert.equal(calls, 1)
})

test('dynamic Connection tokens are removed both ways while Receipt and authorization remain opaque', async (t) => {
  let received
  const proxy = await fixture(t, (input, response) => {
    received = input.headers
    input.resume()
    response.writeHead(202, {
      connection: 'close, X-Private-Hop', 'x-private-hop': 'never-forward',
      receipt: 'opaque-receipt', etag: '"exact-etag"', 'content-type': 'application/json',
    })
    response.end('{"receipt":"unchanged"}')
  })
  const reply = await send(proxy.origin, { headers: {
    connection: 'close, X-Request-Hop', 'x-request-hop': 'never-forward',
    authorization: 'Bearer opaque', 'last-event-id': 'opaque+/cursor=', 'if-match': '"exact"',
  } })
  assert.equal(received['x-request-hop'], undefined)
  assert.equal(received.authorization, 'Bearer opaque')
  assert.equal(received['last-event-id'], 'opaque+/cursor=')
  assert.equal(received['if-match'], '"exact"')
  assert.equal(reply.headers['x-private-hop'], undefined)
  assert.equal(reply.headers.receipt, 'opaque-receipt')
  assert.equal(reply.headers.etag, '"exact-etag"')
  assert.equal(reply.status, 202)
  assert.equal(reply.body, '{"receipt":"unchanged"}')
})

test('a flowing SSE survives the request deadline and idle cancellation releases its upstream', async (t) => {
  let closed
  const upstreamClosed = new Promise((resolve) => { closed = resolve })
  const proxy = await fixture(t, (_input, response) => {
    response.writeHead(200, { 'content-type': 'text/event-stream' })
    let count = 0
    const timer = setInterval(() => {
      response.write(`data: ${++count}\n\n`)
      if (count === 6) clearInterval(timer)
    }, 20)
    response.once('close', () => { clearInterval(timer); closed() })
  }, { request_timeout_ms: 30, upstream_header_timeout_ms: 100, idle_timeout_ms: 80 })
  const chunks = []
  await new Promise((resolve, reject) => {
    const client = request(`${proxy.origin}/v1/runs/run/events`, { agent: false }, (response) => {
      response.on('data', (chunk) => chunks.push(chunk.toString()))
      response.once('aborted', resolve)
      response.once('error', (error) => { if (error.code !== 'ECONNRESET') reject(error) })
      response.once('end', () => reject(new Error('an idle event stream was silently completed')))
    })
    client.once('error', reject)
    client.end()
  })
  await upstreamClosed
  assert.equal(chunks.join(''), Array.from({ length: 6 }, (_, index) => `data: ${index + 1}\n\n`).join(''))
})

test('absolute request targets and CONNECT cannot choose a third upstream', async (t) => {
  let calls = 0
  const proxy = await fixture(t, (_input, response) => { calls++; response.end() })
  const port = Number(new URL(proxy.origin).port)
  for (const target of ['GET http://example.invalid/v1/agents HTTP/1.1', 'GET //example.invalid/v1/agents HTTP/1.1', 'CONNECT example.invalid:443 HTTP/1.1']) {
    const reply = await new Promise((resolve, reject) => {
      const client = connect({ host: '127.0.0.1', port }, () => client.write(`${target}\r\nHost: ignored.invalid\r\nConnection: close\r\n\r\n`))
      let body = ''
      client.on('data', (chunk) => { body += chunk })
      client.once('end', () => resolve(body))
      client.once('error', reject)
    })
    assert.match(reply, /^HTTP\/1\.1 400 /)
  }
  assert.equal(calls, 0)
})

test('a paused SSE reader backpressures the real producer and cancellation releases the connection slot', async (t) => {
  let written = 0
  let closed
  const upstreamClosed = new Promise((resolve) => { closed = resolve })
  const maximumProduced = 64 * 1024 * 1024
  const proxy = await fixture(t, (_input, response) => {
    response.writeHead(200, { 'content-type': 'text/event-stream' })
    const chunk = Buffer.alloc(65536, 65)
    const produce = () => {
      while (written < maximumProduced && !response.destroyed) {
        written += chunk.length
        if (!response.write(chunk)) { response.once('drain', produce); return }
      }
    }
    response.once('close', closed)
    produce()
  }, { max_connections: 1, idle_timeout_ms: 5000 })
  let client
  const reader = await new Promise((resolve, reject) => {
    client = request(`${proxy.origin}/v1/runs/run/events`, { agent: false }, (response) => { response.pause(); resolve(response) })
    client.once('error', reject)
    client.end()
  })
  t.after(() => { reader.destroy(); client.destroy() })
  let before = -1
  for (let attempt = 0; attempt < 20; attempt++) {
    await wait(100)
    if (written === before) break
    before = written
  }
  assert.ok(written > 0 && written < maximumProduced, 'a paused reader must stop the actual Gateway producer before its finite cap')
  assert.equal(written, before, 'producer continued without downstream demand')
  await assert.rejects(send(proxy.origin), /ECONNRESET|socket hang up/)
  reader.destroy()
  client.destroy()
  await upstreamClosed
  const staticReply = await send(proxy.origin, { path: '/', method: 'GET' })
  assert.equal(staticReply.status, 200)
  assert.match(staticReply.body, /actual bundle/)
})

test('request and upstream response header bytes obey the configured parser bound', async (t) => {
  let calls = 0
  const proxy = await fixture(t, (_input, response) => {
    calls++
    response.writeHead(200, { 'x-oversized-header': 'x'.repeat(2048) })
    response.end('unreachable')
  }, { max_header_bytes: 1024 })
  const oversizedRequest = await send(proxy.origin, { headers: { 'x-oversized-header': 'x'.repeat(2048) } })
  assert.equal(oversizedRequest.status, 431)
  assert.equal(calls, 0)
  const oversizedResponse = await send(proxy.origin)
  assert.equal(oversizedResponse.status, 503)
  assert.equal(JSON.parse(oversizedResponse.body).code, 'gateway_unavailable')
  assert.equal(calls, 1)
})

test('admitted request storage is zeroed after transmission, cancellation and overflow', async (t) => {
  const secret = 'private-body-for-memory-erasure'
  const delivered = []
  const proxy = await fixture(t, async (input, response) => {
    const chunks = []
    for await (const chunk of input) chunks.push(chunk)
    delivered.push(Buffer.concat(chunks).toString())
    response.end('accepted')
  }, { max_request_bytes: 137, max_buffered_request_bytes: 137 })
  // Observe the backing allocations without exporting a test-only transport interface. Keeping
  // these references also prevents later allocator reuse from obscuring the erasure result.
  const originalAllocate = Buffer.allocUnsafe
  const allocations = []
  Buffer.allocUnsafe = function (size) {
    const bytes = originalAllocate(size)
    if (size === 137) allocations.push(bytes)
    return bytes
  }
  t.after(() => { Buffer.allocUnsafe = originalAllocate })
  assert.equal((await send(proxy.origin, { chunks: [secret] })).status, 200)
  assert.deepEqual(delivered, [secret])
  assert.ok(allocations.length > 0)
  assert.ok(allocations.every((bytes) => bytes.every((byte) => byte === 0)))
  for (const outcome of ['cancel', 'overflow']) {
    const firstAllocation = allocations.length
    let complete
    const completed = new Promise((resolve) => { complete = resolve })
    const client = request(`${proxy.origin}/v1/agents`, { method: 'POST', agent: false }, (response) => {
      response.resume()
      response.once('end', () => complete(response.statusCode))
    })
    client.on('error', () => {})
    t.after(() => client.destroy())
    client.write(secret)
    await wait(30)
    assert.ok(allocations.slice(firstAllocation).some((bytes) => bytes.includes(secret)), 'the test must observe the actual admitted private body')
    if (outcome === 'cancel') client.destroy()
    else { client.end('x'.repeat(138)); assert.equal(await completed, 413) }
    await wait(30)
    assert.ok(allocations.slice(firstAllocation).every((bytes) => bytes.every((byte) => byte === 0)))
  }
  assert.deepEqual(delivered, [secret])
})
