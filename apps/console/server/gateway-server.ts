import type { IncomingHttpHeaders, IncomingMessage, ServerResponse } from 'node:http'
import type { ConsoleTransportConfigV3 } from './config.ts'

interface BufferedBody {
  pages: Buffer[]
  length: number
  release: () => void
}
export interface ConsoleServer {
  origin: string
  close: () => Promise<void>
}

import {
  createReadStream,
  existsSync,
  lstatSync,
  readdirSync,
  realpathSync,
  statSync,
} from 'node:fs'
import { X509Certificate } from 'node:crypto'
import { createServer, request as httpRequest } from 'node:http'
import { request as httpsRequest } from 'node:https'
import { extname, join, normalize, resolve } from 'node:path'
import { fileURLToPath } from 'node:url'
import { checkedTransportConfig } from './config.ts'

export const defaultBundleRoot = fileURLToPath(new URL('../dist', import.meta.url))
const hopByHopHeaders = new Set([
  'connection',
  'keep-alive',
  'proxy-authenticate',
  'proxy-authorization',
  'proxy-connection',
  'te',
  'trailer',
  'transfer-encoding',
  'upgrade',
])

export function checkedBundleRoot(value: string) {
  const requested = resolve(value)
  const metadata = lstatSync(requested)
  if (!metadata.isDirectory() || metadata.isSymbolicLink())
    throw new Error('Console bundle root must be a real directory, not a symbolic link')
  const canonicalRoot = realpathSync(requested)
  const pending = [canonicalRoot]
  while (pending.length > 0) {
    for (const entry of readdirSync(pending.pop()!, { withFileTypes: true })) {
      const path = join(entry.parentPath, entry.name)
      const child = lstatSync(path)
      if (child.isSymbolicLink()) throw new Error('Console bundle must not contain symbolic links')
      if (child.isDirectory()) pending.push(path)
      else if (!child.isFile())
        throw new Error('Console bundle must contain only directories and regular files')
    }
  }
  const index = join(canonicalRoot, 'index.html')
  if (!existsSync(index) || !lstatSync(index).isFile())
    throw new Error('Console bundle must contain a regular index.html')
  return canonicalRoot
}

function staticPath(bundleRoot: string, requestPath: string) {
  const relative = requestPath === '/' ? 'index.html' : requestPath.slice(1)
  const candidate = normalize(join(bundleRoot, relative))
  if (
    candidate.startsWith(`${bundleRoot}/`) &&
    existsSync(candidate) &&
    statSync(candidate).isFile()
  )
    return candidate
  return join(bundleRoot, 'index.html')
}

function serveStatic(
  bundleRoot: string,
  method: string,
  requestPath: string,
  response: ServerResponse,
) {
  const path = staticPath(bundleRoot, requestPath)
  const mediaTypes: Record<string, string> = {
    '.css': 'text/css; charset=utf-8',
    '.html': 'text/html; charset=utf-8',
    '.js': 'text/javascript; charset=utf-8',
    '.wasm': 'application/wasm',
    '.svg': 'image/svg+xml',
    '.json': 'application/json',
    '.png': 'image/png',
  }
  const mediaType = mediaTypes[extname(path)] ?? 'application/octet-stream'
  response.writeHead(200, {
    'cache-control': path.endsWith('index.html')
      ? 'no-store'
      : 'public, max-age=31536000, immutable',
    'content-length': statSync(path).size,
    'content-type': mediaType,
  })
  if (method === 'HEAD') {
    response.end()
    return
  }
  const source = createReadStream(path)
  source.once('error', () => response.destroy())
  response.once('close', () => source.destroy())
  source.pipe(response)
}

function forwardedHeaders(headers: IncomingHttpHeaders) {
  const excluded = new Set(hopByHopHeaders)
  // Connection can nominate arbitrary additional hop-by-hop fields on either side.
  const connection = headers.connection
  for (const token of (Array.isArray(connection) ? connection.join(',') : (connection ?? '')).split(
    ',',
  ))
    excluded.add(token.trim().toLowerCase())
  return Object.fromEntries(
    Object.entries(headers).filter(([name, value]) => !excluded.has(name) && value !== undefined),
  )
}

class TransportFailure extends Error {
  status: number
  code: string
  constructor(status: number, code: string) {
    super(code)
    this.status = status
    this.code = code
  }
}

function identityAssertion(origin: URL, cookie: string): Promise<string> {
  return new Promise((resolve, reject) => {
    const send = origin.protocol === 'https:' ? httpsRequest : httpRequest
    const upstream = send(
      origin,
      {
        path: '/internal/token',
        method: 'GET',
        headers: { cookie },
        agent: false,
        rejectUnauthorized: true,
        maxHeaderSize: 8192,
      },
      (response) => {
        const chunks: Buffer[] = []
        let length = 0
        response.on('data', (chunk: Buffer) => {
          length += chunk.length
          if (length > 8192) upstream.destroy(new Error('oversized identity reply'))
          else chunks.push(chunk)
        })
        response.on('error', () => reject(new TransportFailure(503, 'identity_unavailable')))
        response.on('end', () => {
          if (response.statusCode !== 200) {
            reject(
              new TransportFailure(
                response.statusCode === 401 ? 401 : 503,
                response.statusCode === 401 ? 'authentication_required' : 'identity_unavailable',
              ),
            )
            return
          }
          try {
            const value = JSON.parse(Buffer.concat(chunks).toString('utf8'))
            if (
              typeof value.access_token !== 'string' ||
              !/^[A-Za-z0-9_-]+\.[A-Za-z0-9_-]+\.[A-Za-z0-9_-]+$/.test(value.access_token)
            )
              throw new Error('invalid assertion')
            resolve(value.access_token)
          } catch {
            reject(new TransportFailure(503, 'identity_unavailable'))
          }
        })
      },
    )
    upstream.setTimeout(5000, () => upstream.destroy(new Error('identity timeout')))
    upstream.on('error', () => reject(new TransportFailure(503, 'identity_unavailable')))
    upstream.end()
  })
}

function fail(response: ServerResponse, status: number, code: string) {
  if (response.destroyed || response.writableEnded) return
  if (response.headersSent) {
    response.destroy()
    return
  }
  const body = JSON.stringify({
    code,
    status,
    title: 'Console transport request failed',
    retryable: status === 503 || status === 504,
  })
  response.writeHead(status, {
    'content-type': 'application/json',
    'content-length': Buffer.byteLength(body),
    'cache-control': 'no-store',
    connection: 'close',
  })
  response.end(body)
}

function collectBody(
  request: IncomingMessage,
  response: ServerResponse,
  config: ConsoleTransportConfigV3,
  budget: { bytes: number },
) {
  const pages: Buffer[] = []
  let reserved = 0
  let received = 0
  let usedInLastPage = 0
  let released = false
  const release = () => {
    if (released) return
    released = true
    budget.bytes -= reserved
    for (const page of pages) page.fill(0)
    pages.length = 0
  }
  const collected = new Promise<BufferedBody>((resolveBody, rejectBody) => {
    let done = false
    const finish = (error?: Error) => {
      if (done) return
      done = true
      clearTimeout(timer)
      request.off('data', receive)
      request.off('end', onEnd)
      request.off('error', onError)
      response.off('close', onClose)
      if (error) {
        request.pause()
        release()
        rejectBody(error)
        return
      }
      resolveBody({ pages, length: received, release })
    }
    const onEnd = () => finish()
    const onError = () => finish(new TransportFailure(400, 'request_interrupted'))
    const onClose = () => finish(new TransportFailure(400, 'request_interrupted'))
    const receive = (chunk: Buffer) => {
      if (received + chunk.length > config.max_request_bytes) {
        finish(new TransportFailure(413, 'request_too_large'))
        return
      }
      let position = 0
      while (position < chunk.length) {
        if (pages.length === 0 || usedInLastPage === pages.at(-1)!.length) {
          const capacity = Math.min(16_384, config.max_request_bytes - received)
          if (budget.bytes + capacity > config.max_buffered_request_bytes) {
            finish(new TransportFailure(503, 'transport_capacity_exhausted'))
            return
          }
          budget.bytes += capacity
          reserved += capacity
          pages.push(Buffer.allocUnsafe(capacity))
          usedInLastPage = 0
        }
        const page = pages.at(-1)!
        const count = Math.min(chunk.length - position, page.length - usedInLastPage)
        chunk.copy(page, usedInLastPage, position, position + count)
        usedInLastPage += count
        received += count
        position += count
      }
    }
    const timer = setTimeout(
      () => finish(new TransportFailure(408, 'request_timeout')),
      config.request_timeout_ms,
    )
    request.on('data', receive)
    request.once('end', onEnd)
    request.once('error', onError)
    response.once('close', onClose)
    if (
      request.headers['content-length'] !== undefined &&
      Number(request.headers['content-length']) > config.max_request_bytes
    )
      finish(new TransportFailure(413, 'request_too_large'))
  })
  return collected
}

function proxy(
  request: IncomingMessage,
  response: ServerResponse,
  upstream: URL,
  requestPath: string,
  body: BufferedBody,
  config: ConsoleTransportConfigV3,
  objectUpload = false,
) {
  const headers: IncomingHttpHeaders = objectUpload
    ? { 'content-type': request.headers['content-type'] ?? 'application/octet-stream' }
    : forwardedHeaders(request.headers)
  if (!requestPath.startsWith('/_console/v1/auth/')) delete headers.cookie
  headers.host = upstream.host
  // Transfer framing belongs to this hop. The validated body bytes are otherwise unchanged.
  delete headers['content-length']
  if (
    body.length > 0 ||
    request.headers['content-length'] !== undefined ||
    request.headers['transfer-encoding'] !== undefined
  )
    headers['content-length'] = String(body.length)
  const requestUpstream = upstream.protocol === 'https:' ? httpsRequest : httpRequest
  let incoming: IncomingMessage | undefined
  let headerTimer: ReturnType<typeof setTimeout> | undefined
  let idleTimer: ReturnType<typeof setTimeout> | undefined
  let pageIndex = 0
  let bytesRemaining = body.length
  const forwarded = requestUpstream(
    upstream,
    {
      path: requestPath,
      method: request.method,
      headers,
      agent: false,
      maxHeaderSize: config.max_header_bytes,
      // Explicitly retain system trust and hostname verification, regardless of a caller's
      // NODE_TLS_REJECT_UNAUTHORIZED setting. No transport config can disable verification.
      rejectUnauthorized: true,
      ...(objectUpload ? { ca: config.upload_ca_pem } : {}),
    },
    (upstreamResponse) => {
      incoming = upstreamResponse
      clearTimeout(headerTimer)
      if (objectUpload) {
        const status = upstreamResponse.statusCode ?? 502
        if (status >= 200 && status < 300) {
          response.writeHead(204, { 'cache-control': 'no-store', 'content-length': 0 })
          response.end()
        } else {
          fail(response, status === 403 ? 403 : 502, 'object_upload_rejected')
        }
        upstreamResponse.destroy()
        return
      }
      resetIdle()
      upstreamResponse.on('data', resetIdle)
      upstreamResponse.once('error', () => fail(response, 503, 'gateway_response_interrupted'))
      upstreamResponse.once('aborted', () => fail(response, 503, 'gateway_response_interrupted'))
      response.writeHead(
        upstreamResponse.statusCode ?? 502,
        forwardedHeaders(upstreamResponse.headers),
      )
      // pipe observes downstream write(false)/drain, bounding read-ahead for event streams.
      upstreamResponse.pipe(response)
    },
  )
  const stop = () => {
    clearTimeout(headerTimer)
    clearTimeout(idleTimer)
    forwarded.destroy()
    incoming?.destroy()
    body.release()
  }
  const resetIdle = () => {
    clearTimeout(idleTimer)
    idleTimer = setTimeout(() => {
      fail(response, 504, 'gateway_idle_timeout')
      stop()
    }, config.idle_timeout_ms)
  }
  response.once('close', stop)
  response.once('finish', stop)
  forwarded.once('error', () => {
    fail(response, 503, objectUpload ? 'object_storage_unavailable' : 'gateway_unavailable')
    stop()
  })
  forwarded.once('upgrade', (_reply, socket) => {
    socket.destroy()
    fail(response, 502, 'gateway_protocol_invalid')
    stop()
  })
  headerTimer = setTimeout(() => {
    fail(response, 504, 'gateway_header_timeout')
    stop()
  }, config.upstream_header_timeout_ms)
  // Retain the whole admitted request until transmission finishes. No request is opened until
  // its complete body passed both per-request and whole-process capacity checks.
  const send = () => {
    while (pageIndex < body.pages.length) {
      const page = body.pages[pageIndex++]
      const count = Math.min(bytesRemaining, page.length)
      bytesRemaining -= count
      if (!forwarded.write(page.subarray(0, count))) {
        forwarded.once('drain', send)
        return
      }
    }
    forwarded.end(() => body.release())
  }
  send()
}

// This route transports the existing upload capability; it never signs or persists one.
function objectTarget(request: IncomingMessage, config: ConsoleTransportConfigV3): URL {
  const invalid = () => {
    throw new TransportFailure(400, 'invalid_object_upload')
  }
  if (!config.upload_origin) throw new TransportFailure(503, 'object_transport_unconfigured')
  if (request.method !== 'PUT' || request.url !== '/_console/v1/object-upload') invalid()
  if (request.headers['sec-fetch-site'] && request.headers['sec-fetch-site'] !== 'same-origin')
    invalid()
  const origin = request.headers.origin
  if (origin && new URL(origin).host !== request.headers.host) invalid()
  const raw = request.headers['x-insight-upload-target']
  if (typeof raw !== 'string' || raw.length > 8192 || /[\\\s]/.test(raw)) invalid()
  // Duplicate headers must not be hidden by Node's normalized representation.
  if (
    request.rawHeaders.filter(
      (part, i) => i % 2 === 0 && part.toLowerCase() === 'x-insight-upload-target',
    ).length !== 1
  )
    invalid()
  const target = new URL(raw as string)
  if (
    target.protocol !== 'https:' ||
    target.origin !== config.upload_origin ||
    target.username ||
    target.password ||
    target.hash ||
    !target.pathname.startsWith(config.upload_path_prefix) ||
    target.pathname.length <= config.upload_path_prefix.length ||
    /%(?:2e|2f|5c|00)/i.test(target.pathname)
  )
    invalid()
  const seen = new Set<string>()
  for (const [key] of target.searchParams) {
    if (seen.has(key.toLowerCase())) invalid()
    seen.add(key.toLowerCase())
  }
  if (
    target.searchParams.get('X-Amz-Algorithm') !== 'AWS4-HMAC-SHA256' ||
    !target.searchParams.get('X-Amz-Credential') ||
    !target.searchParams.get('X-Amz-Date') ||
    !/^[1-9][0-9]*$/.test(target.searchParams.get('X-Amz-Expires') ?? '') ||
    !/^[a-f0-9]{64}$/.test(target.searchParams.get('X-Amz-Signature') ?? '') ||
    !target.searchParams.get('X-Amz-SignedHeaders')?.split(';').includes('host')
  )
    invalid()
  return target
}

export async function startConsoleServer({
  config: input,
  bundleRoot: requestedBundleRoot = defaultBundleRoot,
}: {
  config: unknown
  bundleRoot?: string
}): Promise<ConsoleServer> {
  const config = checkedTransportConfig(input)
  if (config.upload_origin && !new X509Certificate(config.upload_ca_pem).ca)
    throw new Error('Console object transport requires a CA certificate')
  const bundleRoot = checkedBundleRoot(requestedBundleRoot)
  const runtime = new URL(config.runtime_origin)
  const management = new URL(config.management_origin)
  const identity = config.identity_origin ? new URL(config.identity_origin) : null
  const budget = { bytes: 0, requests: 0 }
  const server = createServer(
    {
      maxHeaderSize: config.max_header_bytes,
      requestTimeout: config.request_timeout_ms,
      headersTimeout: config.request_timeout_ms,
      connectionsCheckingInterval: Math.min(config.request_timeout_ms, 1000),
    },
    (request, response) => {
      if (++budget.requests > config.max_connections) {
        budget.requests--
        fail(response, 503, 'transport_capacity_exhausted')
        return
      }
      let accounted = true
      const release = () => {
        if (accounted) {
          accounted = false
          budget.requests--
        }
      }
      response.once('close', release)
      response.once('finish', release)
      // HTTP origin-form only. Absolute and network-path request targets must never select a host.
      const target = request.url ?? ''
      // eslint-disable-next-line no-control-regex -- HTTP request targets cannot carry raw controls.
      if (!target.startsWith('/') || target.startsWith('//') || /[\\#\x00-\x20\x7f]/.test(target)) {
        fail(response, 400, 'invalid_request_target')
        return
      }
      const pathname = target.split('?', 1)[0]
      if (pathname.startsWith('/_console/v1/auth/')) {
        if (
          !['session', 'setup', 'login', 'logout'].some(
            (action) => target === `/_console/v1/auth/${action}`,
          )
        ) {
          fail(response, 404, 'not_found')
          return
        }
        if (!identity) {
          if (target !== '/_console/v1/auth/session' || request.method !== 'GET') {
            fail(response, 404, 'not_found')
            return
          }
          response.writeHead(200, {
            'content-type': 'application/json',
            'cache-control': 'no-store',
          })
          response.end(
            JSON.stringify({
              schema_version: 1,
              authentication: 'bearer',
              setup_required: false,
              authenticated: false,
              display_name: null,
              expires_at: null,
            }),
          )
          return
        }
        collectBody(request, response, { ...config, max_request_bytes: 4096 }, budget)
          .then((body) => {
            if (response.destroyed) {
              body.release()
              return
            }
            proxy(request, response, identity, target, body, config)
          })
          .catch((error) =>
            fail(
              response,
              error instanceof TransportFailure ? error.status : 503,
              'identity_unavailable',
            ),
          )
        return
      }
      if (pathname === '/_console/v1/object-upload') {
        let upstream: URL
        try {
          upstream = objectTarget(request, config)
        } catch (error) {
          fail(
            response,
            error instanceof TransportFailure ? error.status : 400,
            error instanceof TransportFailure ? error.code : 'invalid_object_upload',
          )
          return
        }
        collectBody(request, response, config, budget)
          .then((body) => {
            if (response.destroyed) {
              body.release()
              return
            }
            proxy(
              request,
              response,
              upstream,
              upstream.pathname + upstream.search,
              body,
              config,
              true,
            )
          })
          .catch((error) =>
            fail(
              response,
              error instanceof TransportFailure ? error.status : 503,
              error instanceof TransportFailure ? error.code : 'transport_unavailable',
            ),
          )
        return
      }
      if (pathname === '/readyz' || pathname === '/v1' || pathname.startsWith('/v1/')) {
        const noun = pathname.split('/')[2]?.split(':')[0]
        const upstream =
          pathname === '/readyz' || ['runs', 'tasks', 'artifacts', 'conversations'].includes(noun)
            ? runtime
            : management
        collectBody(request, response, config, budget)
          .then(async (body) => {
            if (response.destroyed) {
              body.release()
              return
            }
            try {
              if (identity && pathname !== '/readyz' && !request.headers.authorization) {
                if (!['GET', 'HEAD'].includes(request.method ?? '')) {
                  let sameOrigin = false
                  try {
                    sameOrigin = new URL(request.headers.origin ?? '').host === request.headers.host
                  } catch {
                    /* rejected below */
                  }
                  if (!sameOrigin) throw new TransportFailure(403, 'origin_rejected')
                }
                request.headers.authorization = `Bearer ${await identityAssertion(identity, request.headers.cookie ?? '')}`
              }
              if (response.destroyed) {
                body.release()
                return
              }
              proxy(request, response, upstream, target, body, config)
            } catch (error) {
              body.release()
              throw error
            }
          })
          .catch((error) =>
            fail(
              response,
              error instanceof TransportFailure ? error.status : 503,
              error instanceof TransportFailure ? error.code : 'transport_unavailable',
            ),
          )
        return
      }
      if (request.method !== 'GET' && request.method !== 'HEAD') {
        response.writeHead(405, { allow: 'GET, HEAD', 'content-length': 0, connection: 'close' })
        response.end()
        return
      }
      // Static requests have no body; do not leave an unbounded unread upload on this connection.
      if (
        request.headers['transfer-encoding'] ||
        Number(request.headers['content-length'] ?? 0) > 0
      ) {
        fail(response, 400, 'unexpected_request_body')
        return
      }
      try {
        serveStatic(bundleRoot, request.method, pathname, response)
      } catch {
        fail(response, 503, 'bundle_unavailable')
      }
    },
  )
  server.maxConnections = config.max_connections
  server.setTimeout(config.idle_timeout_ms, (socket) => socket.destroy())
  server.keepAliveTimeout = Math.min(5000, config.idle_timeout_ms)
  server.on('upgrade', (_request, socket) =>
    socket.end('HTTP/1.1 400 Bad Request\r\nConnection: close\r\nContent-Length: 0\r\n\r\n'),
  )
  server.on('connect', (_request, socket) =>
    socket.end('HTTP/1.1 400 Bad Request\r\nConnection: close\r\nContent-Length: 0\r\n\r\n'),
  )
  await new Promise<void>((resolveListen, rejectListen) => {
    server.once('error', rejectListen)
    server.listen(config.listen_port, config.listen_host, resolveListen)
  })
  const address = server.address()
  if (!address || typeof address === 'string')
    throw new Error('Console transport did not bind a TCP port')
  const host = config.listen_host.includes(':') ? `[${config.listen_host}]` : config.listen_host
  return {
    origin: `http://${host}:${address.port}`,
    close: () =>
      new Promise<void>((resolveClose, rejectClose) => {
        server.close((error) => (error ? rejectClose(error) : resolveClose()))
        server.closeAllConnections()
      }),
  }
}
