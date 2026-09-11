import { createServer } from 'node:http'
import {
  readFileSync,
  writeFileSync,
  symlinkSync,
  linkSync,
  renameSync,
  mkdirSync,
  existsSync,
} from 'node:fs'
import { join } from 'node:path'
const { mode, recordPath, releasePath, pauseAfterRecord, ignoreTerm } = JSON.parse(
  readFileSync(new URL('./fixture.json', import.meta.url), 'utf8'),
)
const profile = process.argv
  .find((arg) => arg.startsWith('--user-data-dir='))
  .slice('--user-data-dir='.length)
const pageOrigin = process.argv.at(-1)
const record: { profile: string; pid: number; requests: string[]; port?: number; moved?: string } =
  { profile, pid: process.pid, requests: [] }
const save = () => {
  writeFileSync(recordPath + '.pending', JSON.stringify(record))
  renameSync(recordPath + '.pending', recordPath)
}
save()
if (pauseAfterRecord) {
  const deadline = performance.now() + 2000
  while (!existsSync(releasePath)) {
    if (performance.now() >= deadline) process.exit(9)
    await new Promise((resolve) => setTimeout(resolve, 10))
  }
}
process.stderr.write('test-secret-canary https://private.invalid/secret\n')
if (mode === 'exit') process.exit(7)
if (mode === 'signal') process.kill(process.pid, 'SIGTERM')
const sockets = new Set<import('node:net').Socket>()
let port
const server = createServer((request, response) => {
  record.requests.push(request.url)
  save()
  if (mode === 'headers') return
  if (mode === 'body') {
    response.writeHead(200, { 'content-type': 'application/json' })
    response.write('[')
    return
  }
  if (mode === 'redirect' && request.url === '/json/version') {
    response.writeHead(302, { location: '/redirected' })
    response.end()
    return
  }
  if (mode === 'huge') {
    response.writeHead(200, { 'content-type': 'application/json' })
    response.end('[' + ' '.repeat(65536) + ']')
    return
  }
  if (mode === 'declared-huge') {
    response.writeHead(200, { 'content-length': '65537' })
    response.end()
    return
  }
  if (mode === 'encoded') {
    response.writeHead(200, { 'content-encoding': 'gzip' })
    response.end('untrusted')
    return
  }
  if (mode === 'malformed') {
    response.end('{')
    return
  }
  if (mode === 'null') {
    response.end('null')
    return
  }
  if (mode === 'invalid-utf8') {
    response.end(Buffer.from([0xff]))
    return
  }
  const browserSocket = 'ws://127.0.0.1:' + port + '/devtools/browser/test-browser'
  const page = {
    type: 'page',
    id: 'test-page',
    url: pageOrigin + '/',
    webSocketDebuggerUrl: 'ws://127.0.0.1:' + port + '/devtools/page/test-page',
  }
  if (mode === 'foreign-page')
    page.webSocketDebuggerUrl = 'ws://example.invalid:12345/devtools/page/test-page'
  if (mode === 'wrong-origin') page.url = 'http://127.0.0.1:12346/'
  response.setHeader('content-type', 'application/json')
  response.end(
    JSON.stringify(
      request.url === '/json/version'
        ? {
            webSocketDebuggerUrl:
              mode === 'wrong-browser'
                ? 'ws://example.invalid:12345/devtools/browser/test-browser'
                : browserSocket,
          }
        : mode === 'duplicate-page'
          ? [page, page]
          : [page],
    ),
  )
})
server.on('connection', (socket) => {
  sockets.add(socket)
  socket.on('close', () => sockets.delete(socket))
})
server.listen(0, '127.0.0.1', () => {
  const address = server.address()
  if (!address || typeof address === 'string') throw new Error('Expected TCP address')
  port = address.port
  record.port = port
  save()
  let text = port + '\n/devtools/browser/test-browser\n'
  if (mode === 'missing') return
  if (mode === 'file-empty') text = ''
  if (mode === 'file-port') text = '65536\n/devtools/browser/test-browser\n'
  if (mode === 'file-leading-zero') text = '0' + port + '\n/devtools/browser/test-browser\n'
  if (mode === 'file-path') text = port + '\nhttps://private.invalid/\n'
  if (mode === 'file-null') text = 'null'
  if (mode === 'file-huge') text = 'x'.repeat(257)
  if (mode === 'replace-profile' || mode === 'replace-profile-invalid') {
    renameSync(profile, profile + '-moved')
    mkdirSync(profile)
    record.moved = profile + '-moved'
    save()
    if (mode === 'replace-profile') return
    text = 'null'
  }
  const path = join(profile, 'DevToolsActivePort')
  if (mode === 'file-link' || mode === 'file-hardlink') {
    const target = join(profile, 'target')
    writeFileSync(target, text)
    if (mode === 'file-link') symlinkSync(target, path)
    else linkSync(target, path)
  } else writeFileSync(path, text)
})
process.on('SIGTERM', () => {
  if (ignoreTerm) return
  for (const socket of sockets) socket.destroy()
  server.close(() => process.exit(0))
})
