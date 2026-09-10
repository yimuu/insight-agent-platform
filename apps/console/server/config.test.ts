import assert from 'node:assert/strict'
import { mkdtempSync, rmSync, symlinkSync, writeFileSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import test from 'node:test'
import {
  checkedTransportConfig,
  decodeTransportConfig,
  loadTransportConfig,
  maximumConfigBytes,
  nativeTransportLimits,
} from './config.ts'

const config = () => ({
  schema_version: 1,
  topology: 'native',
  listen_host: '127.0.0.1',
  listen_port: 0,
  runtime_origin: 'http://127.0.0.1:8200',
  management_origin: 'http://localhost:8201',
  ...nativeTransportLimits,
})
const decode = (value) => decodeTransportConfig(Buffer.from(value))

test('current transport envelope is exact, bounded and rejects duplicate decoded names', () => {
  const input = config()
  assert.deepEqual(decode(JSON.stringify(input)), input)
  for (const value of [
    JSON.stringify(input).replace('"schema_version":1', '"schema_version":1,"schema_version":1'),
    JSON.stringify(input).replace(
      '"schema_version":1',
      '"schema_version":1,"schema_versi\\u006fn":1',
    ),
    JSON.stringify({ ...input, unknown: 1 }),
    JSON.stringify({ ...input, schema_version: 2 }),
    JSON.stringify(input).replace('"schema_version":1,', ''),
    JSON.stringify(input).replace('"schema_version":1', '"schema_version":{}'),
    JSON.stringify(input).replace('{', '{\u00a0'),
    `${JSON.stringify(input)}true`,
    `${JSON.stringify(input).slice(0, -1)},}`,
  ])
    assert.throws(() => decode(value), /Invalid Console transport configuration/)
  assert.throws(() => decodeTransportConfig(Buffer.alloc(maximumConfigBytes + 1)), /configuration/)
  assert.throws(() => decodeTransportConfig(Buffer.from([0xff])), /configuration/)
  for (const [key, values] of Object.entries({
    listen_port: [-1, 65536, 1.2],
    max_request_bytes: [0, 16 * 1024 * 1024 + 1],
    max_buffered_request_bytes: [1, 64 * 1024 * 1024 + 1],
    request_timeout_ms: [0, 300001],
    upstream_header_timeout_ms: [0, 60001],
    idle_timeout_ms: [0, 300001],
    max_connections: [0, 4097],
    max_header_bytes: [1023, 65537],
  }))
    for (const value of values)
      assert.throws(() => checkedTransportConfig({ ...input, [key]: value }), /configuration/)
})

test('deployment origin validation permits explicit DNS and verified HTTPS without native widening', () => {
  for (const topology of ['compose', 'kubernetes_local']) {
    const input = {
      ...config(),
      topology,
      listen_host: '0.0.0.0',
      listen_port: 8080,
      runtime_origin: 'http://gateway-runtime:8080',
      management_origin: 'https://gateway-management.svc:8443',
    }
    assert.deepEqual(checkedTransportConfig(input), input)
    for (const origin of [
      'https://user:password@gateway',
      'http://@gateway',
      'https://@gateway',
      'http://gateway/path',
      'http://gateway?x=y',
      'http://gateway#fragment',
      'http://gateway%2eexample',
      'http://gateway\\elsewhere',
      'file:///tmp/gateway',
      'http://gateway:0',
      'http://gateway/../',
      'http://gateway name',
    ]) {
      assert.throws(
        () => checkedTransportConfig({ ...input, runtime_origin: origin }),
        /configuration/,
      )
    }
    assert.throws(() => checkedTransportConfig({ ...input, listen_port: 0 }), /configuration/)
    assert.throws(
      () => checkedTransportConfig({ ...input, rejectUnauthorized: false }),
      /configuration/,
    )
  }
  for (const origin of ['http://gateway-runtime:8080', 'https://localhost:8443']) {
    assert.throws(
      () => checkedTransportConfig({ ...config(), runtime_origin: origin }),
      /loopback HTTP/,
    )
  }
  assert.throws(
    () => checkedTransportConfig({ ...config(), listen_host: '0.0.0.0' }),
    /configuration/,
  )
})

test('process config reader is bounded and refuses a symlink or nonregular path', () => {
  const root = mkdtempSync(join(tmpdir(), 'insight-console-config-'))
  try {
    const path = join(root, 'console.json')
    writeFileSync(path, JSON.stringify(config()))
    assert.deepEqual(loadTransportConfig(path), config())
    symlinkSync(path, join(root, 'link.json'))
    assert.throws(() => loadTransportConfig(join(root, 'link.json')), /configuration/)
    assert.throws(() => loadTransportConfig(root), /configuration/)
    assert.throws(() => loadTransportConfig('relative.json'), /configuration/)
    writeFileSync(path, Buffer.alloc(maximumConfigBytes + 1))
    assert.throws(() => loadTransportConfig(path), /configuration/)
  } finally {
    rmSync(root, { recursive: true })
  }
})
