import assert from 'node:assert/strict'
import test from 'node:test'
import { mkdtempSync, readFileSync, rmSync, writeFileSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { spawnSync } from 'node:child_process'
import { createServer } from 'node:https'
import { tcpPort } from '../tests/fixtures/types.ts'
import { nativeTransportLimits } from './config.ts'
import { startConsoleServer } from './gateway-server.ts'

async function fixture(t, overrides = {}) {
  const root = mkdtempSync(join(tmpdir(), 'console-object-'))
  writeFileSync(join(root, 'index.html'), '<!doctype html>')
  const generated = spawnSync(
    'openssl',
    [
      'req',
      '-x509',
      '-newkey',
      'rsa:2048',
      '-nodes',
      '-days',
      '1',
      '-subj',
      '/CN=localhost',
      '-addext',
      'subjectAltName=DNS:localhost',
      '-addext',
      'basicConstraints=critical,CA:TRUE',
      '-keyout',
      join(root, 'key.pem'),
      '-out',
      join(root, 'ca.pem'),
    ],
    { encoding: 'utf8' },
  )
  assert.equal(generated.status, 0, generated.stderr)
  const ca = readFileSync(join(root, 'ca.pem'), 'utf8')
  const calls = []
  const behavior = { status: 200 }
  const storage = createServer(
    { key: readFileSync(join(root, 'key.pem')), cert: ca },
    async (request, response) => {
      const bytes = []
      for await (const part of request) bytes.push(part)
      calls.push({
        path: request.url,
        method: request.method,
        headers: request.headers,
        bytes: Buffer.concat(bytes),
      })
      response.writeHead(behavior.status, {
        location: 'https://forbidden.example/',
        'set-cookie': 'private=value',
      })
      response.end('private upstream diagnostic must not escape')
    },
  )
  await new Promise<void>((resolve) => storage.listen(0, '127.0.0.1', resolve))
  const storageOrigin = `https://localhost:${tcpPort(storage)}`
  const config = {
    schema_version: 3,
    topology: 'native',
    listen_host: '127.0.0.1',
    listen_port: 0,
    runtime_origin: 'http://127.0.0.1:1',
    management_origin: 'http://127.0.0.1:1',
    ...nativeTransportLimits,
    upload_origin: storageOrigin,
    upload_path_prefix: '/test-bucket/',
    upload_ca_pem: ca,
    ...overrides,
  }
  const proxy = await startConsoleServer({ bundleRoot: root, config })
  t.after(async () => {
    await proxy.close()
    storage.closeAllConnections()
    await new Promise((resolve) => storage.close(resolve))
    rmSync(root, { recursive: true, force: true })
  })
  const signature = new URLSearchParams({
    'X-Amz-Algorithm': 'AWS4-HMAC-SHA256',
    'X-Amz-Credential': 'scoped/test/s3/aws4_request',
    'X-Amz-Date': '20260911T000000Z',
    'X-Amz-Expires': '900',
    'X-Amz-SignedHeaders': 'host',
    'X-Amz-Signature': 'a'.repeat(64),
  })
  const target = `${storageOrigin}/test-bucket/staging/object.json?${signature}`
  const put = (value = target, options = {}) =>
    fetch(`${proxy.origin}/_console/v1/object-upload`, {
      method: 'PUT',
      headers: {
        'X-Insight-Upload-Target': value,
        'Content-Type': 'application/json',
        Authorization: 'Bearer must-not-forward',
        Cookie: 'private=session',
        Origin: proxy.origin,
      },
      body: Buffer.from([0, 255, 1, 42]),
      ...options,
    })
  return { put, calls, behavior, proxy, storageOrigin, target, config, root }
}

test('same-origin PUT preserves signed host/path/query and exact bytes using the configured CA only', async (t) => {
  const f = await fixture(t)
  const response = await f.put()
  assert.equal(response.status, 204)
  assert.equal(await response.text(), '')
  assert.equal(response.headers.get('set-cookie'), null)
  assert.equal(response.headers.get('location'), null)
  assert.equal(f.calls.length, 1)
  const call = f.calls[0]
  assert.equal(call.path, new URL(f.target).pathname + new URL(f.target).search)
  assert.equal(call.headers.host, new URL(f.storageOrigin).host)
  assert.deepEqual(call.bytes, Buffer.from([0, 255, 1, 42]))
  for (const forbidden of ['authorization', 'cookie', 'origin', 'x-insight-upload-target'])
    assert.equal(call.headers[forbidden], undefined)
  assert.equal(call.headers['content-type'], 'application/json')
  assert.equal(call.headers['content-length'], '4')
  f.behavior.status = 307
  const redirect = await f.put()
  assert.equal(redirect.status, 502)
  assert.equal(f.calls.length, 2)
  assert.equal(redirect.headers.get('location'), null)
  assert.ok(!(await redirect.text()).includes('private upstream'))
  f.behavior.status = 403
  assert.equal((await f.put()).status, 403)
})

test('unapproved origins, bucket escape, unsigned targets, duplicate signature fields, cross-site and non-PUT fail before storage', async (t) => {
  const f = await fixture(t)
  for (const target of [
    f.target.replace('https:', 'http:'),
    f.target.replace('localhost', '127.0.0.1'),
    f.target.replace('/test-bucket/', '/another-bucket/'),
    f.target.replace('/staging/', '/../'),
    f.target.replace('/staging/', '/%2e%2e/'),
    f.target.replace('/staging/', '/%2F/'),
    f.target + '&X-Amz-Signature=' + 'b'.repeat(64),
    f.target + '&x-amz-signature=' + 'b'.repeat(64),
    f.target.replace('https://', 'https://user:pass@'),
    f.target + '#fragment',
    f.target.split('?')[0],
  ]) {
    assert.equal((await f.put(target)).status, 400)
  }
  assert.equal((await f.put(f.target, { method: 'POST' })).status, 400)
  assert.equal(
    (
      await f.put(f.target, {
        headers: { 'X-Insight-Upload-Target': f.target, Origin: 'https://evil.example' },
      })
    ).status,
    400,
  )
  assert.equal(
    (
      await f.put(f.target, {
        headers: { 'X-Insight-Upload-Target': f.target, 'Sec-Fetch-Site': 'cross-site' },
      })
    ).status,
    400,
  )
  assert.equal(f.calls.length, 0)
})

test('object upload shares bounded buffering and releases it after oversized requests', async (t) => {
  const f = await fixture(t, { max_request_bytes: 4, max_buffered_request_bytes: 4 })
  assert.equal((await f.put(f.target, { body: 'too large' })).status, 413)
  assert.equal(f.calls.length, 0)
  assert.equal((await f.put()).status, 204)
  assert.equal((await f.put()).status, 204)
})

test('object TLS still rejects wrong hostnames and untrusted certificate roots', async (t) => {
  const f = await fixture(t)
  const other = await fixture(t)
  for (const config of [
    { ...f.config, upload_origin: f.storageOrigin.replace('localhost', '127.0.0.1') },
    { ...f.config, upload_ca_pem: other.config.upload_ca_pem },
  ]) {
    const proxy = await startConsoleServer({ bundleRoot: f.root, config })
    try {
      const response = await fetch(`${proxy.origin}/_console/v1/object-upload`, {
        method: 'PUT',
        headers: {
          'X-Insight-Upload-Target': f.target.replace(f.storageOrigin, config.upload_origin),
        },
        body: '{}',
      })
      assert.equal(response.status, 503)
      assert.equal((await response.json()).code, 'object_storage_unavailable')
    } finally {
      await proxy.close()
    }
  }
  assert.equal(f.calls.length, 0)
})
