import { closeSync, constants, fstatSync, openSync, readSync } from 'node:fs'
import { isIP } from 'node:net'
import { isAbsolute } from 'node:path'

export interface ConsoleTransportConfigV1 {
  schema_version: 1
  topology: 'native' | 'compose' | 'kubernetes_local'
  listen_host: string
  listen_port: number
  runtime_origin: string
  management_origin: string
  max_request_bytes: number
  max_buffered_request_bytes: number
  request_timeout_ms: number
  upstream_header_timeout_ms: number
  idle_timeout_ms: number
  max_connections: number
  max_header_bytes: number
}

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
  'schema_version',
  'topology',
  'listen_host',
  'listen_port',
  'runtime_origin',
  'management_origin',
  ...Object.keys(nativeTransportLimits),
])
const loopbackHosts = new Set(['127.0.0.1', '[::1]', 'localhost'])

function invalid(): never {
  throw new Error('Invalid Console transport configuration')
}

function checkedOrigin(value: unknown, topology: ConsoleTransportConfigV1['topology']) {
  if (
    typeof value !== 'string' ||
    value.length > 2048 ||
    !/^https?:\/\/[^/?#\\%@\s]+\/?$/.test(value)
  )
    invalid()
  let origin
  try {
    origin = new URL(value)
  } catch {
    invalid()
  }
  if (
    origin.username ||
    origin.password ||
    origin.pathname !== '/' ||
    origin.search ||
    origin.hash ||
    !origin.hostname ||
    origin.port === '0'
  )
    invalid()
  const host = origin.hostname.startsWith('[') ? origin.hostname.slice(1, -1) : origin.hostname
  if (
    !isIP(host) &&
    (!/^[a-z0-9.-]+$/.test(host) ||
      host.length > 253 ||
      host.split('.').some((label) => !/^[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?$/.test(label)))
  )
    invalid()
  if (
    topology === 'native' &&
    (origin.protocol !== 'http:' || !loopbackHosts.has(origin.hostname))
  ) {
    throw new Error('Console native Gateway origins must be origin-only loopback HTTP URLs')
  }
  return origin.origin
}

export function checkedTransportConfig(value: unknown): Readonly<ConsoleTransportConfigV1> {
  if (!value || typeof value !== 'object' || Array.isArray(value)) invalid()
  const input = value as Record<string, unknown>
  if (
    Object.keys(input).length !== fields.size ||
    Object.keys(input).some((key) => !fields.has(key))
  )
    invalid()
  if (input.schema_version !== 1) invalid()
  const topology = input.topology
  if (topology !== 'native' && topology !== 'compose' && topology !== 'kubernetes_local') invalid()
  const listen_host = input.listen_host
  if (typeof listen_host !== 'string' || !isIP(listen_host)) invalid()
  if (topology === 'native' && !['127.0.0.1', '::1'].includes(listen_host)) invalid()
  const bounded = (name: string, minimum: number, maximum: number): number => {
    const item = input[name]
    if (typeof item !== 'number' || !Number.isSafeInteger(item) || item < minimum || item > maximum)
      invalid()
    return item
  }
  const max_request_bytes = bounded('max_request_bytes', 1, 16 * 1024 * 1024)
  return Object.freeze({
    schema_version: 1,
    topology,
    listen_host,
    listen_port: bounded('listen_port', topology === 'native' ? 0 : 1, 65535),
    runtime_origin: checkedOrigin(input.runtime_origin, topology),
    management_origin: checkedOrigin(input.management_origin, topology),
    max_request_bytes,
    max_buffered_request_bytes: bounded(
      'max_buffered_request_bytes',
      max_request_bytes,
      64 * 1024 * 1024,
    ),
    request_timeout_ms: bounded('request_timeout_ms', 1, 300_000),
    upstream_header_timeout_ms: bounded('upstream_header_timeout_ms', 1, 60_000),
    idle_timeout_ms: bounded('idle_timeout_ms', 1, 300_000),
    max_connections: bounded('max_connections', 1, 4096),
    max_header_bytes: bounded('max_header_bytes', 1024, 65536),
  })
}

// Only a flat scalar object is accepted. Tokenizing this closed envelope before JSON decoding
// prevents JSON.parse's last-key-wins treatment of duplicate (including escaped) field names.
export function decodeTransportConfig(bytes: unknown) {
  if (!Buffer.isBuffer(bytes) || bytes.length === 0 || bytes.length > maximumConfigBytes) invalid()
  let source: string
  try {
    source = new TextDecoder('utf-8', { fatal: true }).decode(bytes)
  } catch {
    invalid()
  }
  const scalar =
    // oxlint-disable-next-line no-control-regex -- Strict JSON forbids unescaped control bytes.
    /[\x20\t\r\n]*("(?:[^"\\\x00-\x1f]|\\(?:["\\/bfnrt]|u[0-9a-fA-F]{4}))*"|-?(?:0|[1-9][0-9]*)(?:\.[0-9]+)?(?:[eE][+-]?[0-9]+)?)/y
  let position = 0
  const skip = () => {
    while (/[\x20\t\r\n]/.test(source[position] ?? '') && position < source.length) position++
  }
  const token = (): unknown => {
    scalar.lastIndex = position
    const match = scalar.exec(source)
    if (!match) invalid()
    position = scalar.lastIndex
    return JSON.parse(match[1])
  }
  skip()
  if (source[position++] !== '{') invalid()
  const values: Record<string, unknown> = Object.create(null)
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

export function loadTransportConfig(path: unknown) {
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
  } catch {
    invalid()
  } finally {
    if (descriptor !== undefined) closeSync(descriptor)
  }
}
