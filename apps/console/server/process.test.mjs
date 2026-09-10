import assert from 'node:assert/strict'
import { spawn, spawnSync } from 'node:child_process'
import { copyFileSync, mkdirSync, mkdtempSync, readFileSync, rmSync, writeFileSync } from 'node:fs'
import { createServer as createHttpServer } from 'node:http'
import { createServer as createHttpsServer } from 'node:https'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { fileURLToPath } from 'node:url'
import test from 'node:test'
import { nativeTransportLimits } from './config.mjs'

const source = fileURLToPath(new URL('.', import.meta.url))
const listen = (server) => new Promise((resolve) => server.listen(0, '127.0.0.1', resolve))
async function temporaryPort() {
  const server = createHttpServer()
  await listen(server)
  const port = server.address().port
  await new Promise((resolve) => server.close(resolve))
  return port
}

async function startProcess(t, root, config, environment = {}) {
  const configPath = join(root, `console-${config.listen_port}.json`)
  writeFileSync(configPath, JSON.stringify(config))
  const child = spawn(process.execPath, [join(root, 'server/main.mjs'), '--config', configPath], {
    env: { ...process.env, ...environment }, stdio: ['ignore', 'pipe', 'pipe'],
  })
  const exited = new Promise((resolve) => child.once('exit', resolve))
  t.after(async () => {
    if (child.exitCode === null && child.signalCode === null) child.kill('SIGTERM')
    await exited
  })
  let output = ''
  let errors = ''
  child.stderr.on('data', (chunk) => { errors = (errors + chunk).slice(0, 4096) })
  return new Promise((resolve, reject) => {
    const timer = setTimeout(() => { child.kill('SIGKILL'); reject(new Error(`Console startup timed out: ${errors}`)) }, 5000)
    child.once('error', (error) => { clearTimeout(timer); reject(error) })
    child.once('exit', (code) => { clearTimeout(timer); reject(new Error(`Console exited before startup: ${code}: ${errors}`)) })
    child.stdout.on('data', (chunk) => {
      output += chunk
      if (output.includes('\n')) {
        clearTimeout(timer)
        try { resolve(JSON.parse(output.split('\n')[0]).origin) } catch (error) { reject(error) }
      }
    })
  })
}

test('the real service entrypoint forwards trusted HTTPS, rejects unknown roots and verifies peer names', async (t) => {
  const root = mkdtempSync(join(tmpdir(), 'insight-console-https-'))
  mkdirSync(join(root, 'server'))
  mkdirSync(join(root, 'dist'))
  for (const file of ['main.mjs', 'gateway-server.mjs', 'config.mjs', 'process.mjs']) copyFileSync(join(source, file), join(root, 'server', file))
  writeFileSync(join(root, 'dist/index.html'), '<!doctype html><title>immutable candidate</title>')
  const key = join(root, 'gateway-key.pem')
  const cert = join(root, 'gateway-cert.pem')
  const sslConfig = join(root, 'openssl.cnf')
  writeFileSync(sslConfig, '[req]\ndistinguished_name=dn\nx509_extensions=extensions\nprompt=no\n[dn]\nCN=localhost\n[extensions]\nsubjectAltName=DNS:localhost\nbasicConstraints=critical,CA:TRUE\nkeyUsage=digitalSignature,keyCertSign\nextendedKeyUsage=serverAuth\n')
  const generated = spawnSync('openssl', ['req', '-x509', '-newkey', 'rsa:2048', '-nodes', '-days', '1', '-keyout', key, '-out', cert, '-config', sslConfig], { encoding: 'utf8' })
  assert.equal(generated.status, 0, generated.stderr)
  let calls = 0
  const gateway = createHttpsServer({ key: readFileSync(key), cert: readFileSync(cert) }, (input, response) => {
    calls++
    assert.equal(input.headers.authorization, 'Bearer exact-browser-session')
    response.writeHead(200, { etag: '"tls-verified"' })
    response.end('actual Gateway reply')
  })
  await listen(gateway)
  // Cleanup is registered before the child cleanup hooks, so explicitly stop the children via
  // their own nested test scopes before removing their physical candidate bundle.
  try {
    for (const mode of ['trusted', 'untrusted', 'wrong-name']) {
      await t.test(mode, async (childTest) => {
        const config = {
          schema_version: 1, topology: 'compose', listen_host: '127.0.0.1', listen_port: await temporaryPort(),
          runtime_origin: `https://${mode === 'wrong-name' ? '127.0.0.1' : 'localhost'}:${gateway.address().port}`,
          management_origin: `https://localhost:${gateway.address().port}`, ...nativeTransportLimits,
        }
        const origin = await startProcess(childTest, root, config, {
          NODE_EXTRA_CA_CERTS: mode === 'untrusted' ? '' : cert,
          // The service must still verify TLS when an ambient setting attempts to weaken it.
          NODE_TLS_REJECT_UNAUTHORIZED: '0',
        })
        const staticReply = await fetch(origin)
        assert.match(await staticReply.text(), /immutable candidate/)
        const reply = await fetch(`${origin}/v1/runs`, { headers: { authorization: 'Bearer exact-browser-session' } })
        assert.equal(reply.status, mode === 'trusted' ? 200 : 503)
        if (mode === 'trusted') {
          assert.equal(reply.headers.get('etag'), '"tls-verified"')
          assert.equal(await reply.text(), 'actual Gateway reply')
        } else assert.equal((await reply.json()).code, 'gateway_unavailable')
      })
    }
    assert.equal(calls, 1)
  } finally {
    gateway.closeAllConnections()
    await new Promise((resolve) => gateway.close(resolve))
    rmSync(root, { recursive: true })
  }
})
