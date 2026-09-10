import { closeSync, constants, fstatSync, openSync, readSync } from 'node:fs'
import { isIP } from 'node:net'
import { isAbsolute } from 'node:path'

/**
 * Current-only physical transport configuration. No business or credential fields.
 * @typedef {object} ConsoleTransportConfigV1
 * @property {1} schema_version
 * @property {'native'|'compose'|'kubernetes_local'} topology
 * @property {string} listen_host
 * @property {number} listen_port
 * @property {string} runtime_origin
 * @property {string} management_origin
 * @property {number} max_request_bytes
 * @property {number} max_buffered_request_bytes
 * @property {number} request_timeout_ms
 * @property {number} upstream_header_timeout_ms
 * @property {number} idle_timeout_ms
 * @property {number} max_connections
 * @property {number} max_header_bytes
 */

export const maximumConfigBytes = 32 * 1024
export const nativeTransportLimits = Object.freeze({
  max_request_bytes: 1024 * 1024,
  max_buffered_request_bytes: 8 * 1024 * 1024,
  request_timeout_ms: 30_000,
  upstream_header_timeout_ms: 10_000,
  idle_timeout_ms: 60_000,
  max_connections: 128,
  max_header_bytes: 16_384,
})
const fields = new Set([
  'schema_version', 'topology', 'listen_host', 'listen_port', 'runtime_origin', 'management_origin',
  ...Object.keys(nativeTransportLimits),
])
const loopbackHosts = new Set(['127.0.0.1', '[::1]', 'localhost'])

function invalid() { throw new Error('Invalid Console transport configuration') }

function checkedOrigin(value, topology) {
  if (typeof value !== 'string' || value.length > 2048 || !/^https?:\/\/[^/?#\\%@\s]+\/?$/.test(value)) invalid()
  let origin
  try { origin = new URL(value) } catch { invalid() }
  if (origin.username || origin.password || origin.pathname !== '/' || origin.search || origin.hash || !origin.hostname || origin.port === '0') invalid()
  const host = origin.hostname.startsWith('[') ? origin.hostname.slice(1, -1) : origin.hostname
  if (!isIP(host) && (!/^[a-z0-9.-]+$/.test(host) || host.length > 253 || host.split('.').some((label) => !/^[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?$/.test(label)))) invalid()
  if (topology === 'native' && (origin.protocol !== 'http:' || !loopbackHosts.has(origin.hostname))) {
    throw new Error('Console native Gateway origins must be origin-only loopback HTTP URLs')
  }
  return origin.origin
}

/** @returns {Readonly<ConsoleTransportConfigV1>} */
export function checkedTransportConfig(input) {
  if (!input || typeof input !== 'object' || Array.isArray(input) || Object.keys(input).length !== fields.size || Object.keys(input).some((key) => !fields.has(key))) invalid()
  if (input.schema_version !== 1 || !['native', 'compose', 'kubernetes_local'].includes(input.topology)) invalid()
  if (typeof input.listen_host !== 'string' || !isIP(input.listen_host)) invalid()
  if (input.topology === 'native' && !['127.0.0.1', '::1'].includes(input.listen_host)) invalid()
  const bounded = (name, minimum, maximum) => {
    if (!Number.isSafeInteger(input[name]) || input[name] < minimum || input[name] > maximum) invalid()
  }
  bounded('listen_port', input.topology === 'native' ? 0 : 1, 65535)
  bounded('max_request_bytes', 1, 16 * 1024 * 1024)
  bounded('max_buffered_request_bytes', input.max_request_bytes, 64 * 1024 * 1024)
  bounded('request_timeout_ms', 1, 300_000)
  bounded('upstream_header_timeout_ms', 1, 60_000)
  bounded('idle_timeout_ms', 1, 300_000)
  bounded('max_connections', 1, 4096)
  bounded('max_header_bytes', 1024, 65536)
  return Object.freeze({ ...input,
    runtime_origin: checkedOrigin(input.runtime_origin, input.topology),
    management_origin: checkedOrigin(input.management_origin, input.topology),
  })
}

// Only a flat scalar object is accepted. Tokenizing this closed envelope before JSON decoding
// prevents JSON.parse's last-key-wins treatment of duplicate (including escaped) field names.
export function decodeTransportConfig(bytes) {
  if (!Buffer.isBuffer(bytes) || bytes.length === 0 || bytes.length > maximumConfigBytes) invalid()
  let source
  try { source = new TextDecoder('utf-8', { fatal: true }).decode(bytes) } catch { invalid() }
  // eslint-disable-next-line no-control-regex -- Strict JSON forbids unescaped control bytes.
  const scalar = /[\x20\t\r\n]*("(?:[^"\\\x00-\x1f]|\\(?:["\\/bfnrt]|u[0-9a-fA-F]{4}))*"|-?(?:0|[1-9][0-9]*)(?:\.[0-9]+)?(?:[eE][+-]?[0-9]+)?)/y
  let position = 0
  const skip = () => { while (/[\x20\t\r\n]/.test(source[position] ?? '') && position < source.length) position++ }
  const token = () => {
    scalar.lastIndex = position
    const match = scalar.exec(source)
    if (!match) invalid()
    position = scalar.lastIndex
    return JSON.parse(match[1])
  }
  skip()
  if (source[position++] !== '{') invalid()
  const values = Object.create(null)
  skip()
  if (source[position] === '}') invalid()
  for (;;) {
    const name = token()
    if (typeof name !== 'string' || !fields.has(name) || Object.hasOwn(values, name)) invalid()
    skip()
    if (source[position++] !== ':') invalid()
    values[name] = token()
    skip()
    const separator = source[position++]
    if (separator === '}') break
    if (separator !== ',') invalid()
  }
  skip()
  if (position !== source.length) invalid()
  return checkedTransportConfig(values)
}

export function loadTransportConfig(path) {
  if (typeof path !== 'string' || !isAbsolute(path)) invalid()
  let descriptor
  try {
    descriptor = openSync(path, constants.O_RDONLY | constants.O_NOFOLLOW | constants.O_NONBLOCK)
    const metadata = fstatSync(descriptor)
    if (!metadata.isFile() || metadata.size > maximumConfigBytes) invalid()
    const bytes = Buffer.alloc(maximumConfigBytes + 1)
    let length = 0
    while (length < bytes.length) {
      const count = readSync(descriptor, bytes, length, bytes.length - length, null)
      if (count === 0) break
      length += count
    }
    return decodeTransportConfig(bytes.subarray(0, length))
  } catch { invalid() } finally { if (descriptor !== undefined) closeSync(descriptor) }
}
