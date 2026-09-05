import { createReadStream, existsSync, lstatSync, readdirSync, realpathSync, statSync } from 'node:fs'
import { createServer, request as httpRequest } from 'node:http'
import { extname, join, normalize, resolve } from 'node:path'
import { fileURLToPath, pathToFileURL } from 'node:url'

const defaultBundleRoot = resolve(fileURLToPath(new URL('../dist', import.meta.url)))
const loopbackHosts = new Set(['127.0.0.1', '::1', 'localhost'])
const maximumRequestBytes = 1024 * 1024
const hopByHopHeaders = new Set([
  'connection',
  'keep-alive',
  'proxy-authenticate',
  'proxy-authorization',
  'te',
  'trailer',
  'transfer-encoding',
  'upgrade',
])

function checkedUpstream(value) {
  const upstream = new URL(value)
  if (upstream.protocol !== 'http:' || !loopbackHosts.has(upstream.hostname) || upstream.username || upstream.password || upstream.pathname !== '/' || upstream.search || upstream.hash) {
    throw new Error('INSIGHT_CONSOLE_GATEWAY_ORIGIN must be an origin-only loopback HTTP URL')
  }
  return upstream
}

export function checkedBundleRoot(value) {
  const requested = resolve(value)
  const metadata = lstatSync(requested)
  if (!metadata.isDirectory() || metadata.isSymbolicLink()) {
    throw new Error('Console bundle root must be a real directory, not a symbolic link')
  }
  const canonicalRoot = realpathSync(requested)
  const pending = [canonicalRoot]
  while (pending.length > 0) {
    const directory = pending.pop()
    for (const entry of readdirSync(directory, { withFileTypes: true })) {
      const path = join(directory, entry.name)
      const child = lstatSync(path)
      if (child.isSymbolicLink()) throw new Error('Console bundle must not contain symbolic links')
      if (child.isDirectory()) pending.push(path)
      else if (!child.isFile()) throw new Error('Console bundle must contain only directories and regular files')
    }
  }
  const index = join(canonicalRoot, 'index.html')
  if (!existsSync(index) || !lstatSync(index).isFile()) throw new Error('Console bundle must contain a regular index.html')
  return canonicalRoot
}

function staticPath(bundleRoot, requestPath) {
  const relative = requestPath === '/' ? 'index.html' : requestPath.slice(1)
  const candidate = normalize(join(bundleRoot, relative))
  if (candidate.startsWith(`${bundleRoot}/`) && existsSync(candidate) && statSync(candidate).isFile()) return candidate
  return join(bundleRoot, 'index.html')
}

function serveStatic(bundleRoot, method, requestPath, response) {
  const path = staticPath(bundleRoot, requestPath)
  const mediaType = {
    '.css': 'text/css; charset=utf-8',
    '.html': 'text/html; charset=utf-8',
    '.js': 'text/javascript; charset=utf-8',
  }[extname(path)] ?? 'application/octet-stream'
  response.writeHead(200, {
    'cache-control': path.endsWith('index.html') ? 'no-store' : 'public, max-age=31536000, immutable',
    'content-length': statSync(path).size,
    'content-type': mediaType,
  })
  if (method === 'HEAD') {
    response.end()
    return
  }
  createReadStream(path).pipe(response)
}

function proxy(request, response, upstream, requestUrl) {
  const target = new URL(upstream)
  target.pathname = requestUrl.pathname
  target.search = requestUrl.search
  const headers = {}
  for (const [name, value] of Object.entries(request.headers)) {
    if (!hopByHopHeaders.has(name) && name !== 'host' && value !== undefined) headers[name] = value
  }
  headers.host = upstream.host

  const forwarded = httpRequest(target, { method: request.method, headers }, (upstreamResponse) => {
    const responseHeaders = {}
    for (const [name, value] of Object.entries(upstreamResponse.headers)) {
      if (!hopByHopHeaders.has(name) && value !== undefined) responseHeaders[name] = value
    }
    response.writeHead(upstreamResponse.statusCode ?? 502, responseHeaders)
    upstreamResponse.pipe(response)
  })
  forwarded.on('error', () => {
    if (!response.headersSent) {
      const body = JSON.stringify({
        code: 'gateway_unavailable',
        detail: 'The configured local Gateway is unavailable.',
        retryable: true,
        status: 503,
        title: 'Gateway unavailable',
      })
      response.writeHead(503, { 'content-length': Buffer.byteLength(body), 'content-type': 'application/json' })
      response.end(body)
    } else {
      response.destroy()
    }
  })

  let received = 0
  request.on('data', (chunk) => {
    received += chunk.length
    if (received > maximumRequestBytes) {
      request.destroy()
      forwarded.destroy(new Error('request_too_large'))
      return
    }
    forwarded.write(chunk)
  })
  request.on('end', () => forwarded.end())
  request.on('error', () => forwarded.destroy())
}

export async function startGatewayConsoleServer({ gatewayOrigin, port = 0, bundleRoot: requestedBundleRoot } = {}) {
  const bundleRoot = checkedBundleRoot(requestedBundleRoot ?? process.env.INSIGHT_CONSOLE_BUNDLE_ROOT ?? defaultBundleRoot)
  const upstream = checkedUpstream(gatewayOrigin ?? process.env.INSIGHT_CONSOLE_GATEWAY_ORIGIN ?? '')
  const server = createServer((request, response) => {
    const url = new URL(request.url ?? '/', 'http://127.0.0.1')
    if (url.pathname === '/readyz' || url.pathname === '/v1' || url.pathname.startsWith('/v1/')) {
      proxy(request, response, upstream, url)
      return
    }
    if (request.method !== 'GET' && request.method !== 'HEAD') {
      response.writeHead(405, { allow: 'GET, HEAD', 'content-length': 0 })
      response.end()
      return
    }
    serveStatic(bundleRoot, request.method, url.pathname, response)
  })
  await new Promise((resolveListen, rejectListen) => {
    server.once('error', rejectListen)
    server.listen(port, '127.0.0.1', resolveListen)
  })
  const address = server.address()
  if (!address || typeof address === 'string') throw new Error('Gateway Console server did not bind a TCP port')
  return {
    origin: `http://127.0.0.1:${address.port}`,
    close: () => new Promise((resolveClose, rejectClose) => server.close((error) => error ? rejectClose(error) : resolveClose())),
  }
}

async function main() {
  const server = await startGatewayConsoleServer({ port: Number(process.env.INSIGHT_CONSOLE_PORT ?? 4173) })
  process.stdout.write(`${JSON.stringify({ kind: 'insight.console.gateway-server/v1', origin: server.origin })}\n`)
  for (const signal of ['SIGINT', 'SIGTERM']) process.once(signal, () => server.close().finally(() => process.exit(0)))
}

if (import.meta.url === pathToFileURL(process.argv[1] ?? '').href) main().catch((error) => {
  process.stderr.write(`${error instanceof Error ? error.message : String(error)}\n`)
  process.exit(1)
})
