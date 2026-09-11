// Actual installed Console, public API and browser. No intercepted requests or synthetic approval.
import { readFile, lstat, writeFile } from 'node:fs/promises'
import { resolve } from 'node:path'
import assert from 'node:assert/strict'
import { startHeadlessBrowser } from './browser-process.mjs'

const args = new Map()
for (let index = 2; index < process.argv.length; index += 2) {
  const key = process.argv[index]
  if (!['--endpoint', '--session-file', '--model-alias', '--screenshot'].includes(key) || args.has(key) || !process.argv[index + 1]) throw new Error('invalid journey arguments')
  args.set(key, process.argv[index + 1])
}
for (const key of ['--endpoint', '--session-file', '--model-alias']) if (!args.has(key)) throw new Error('missing journey argument')
const tokenPath = resolve(args.get('--session-file'))
const metadata = await lstat(tokenPath)
if (!metadata.isFile() || metadata.isSymbolicLink() || metadata.nlink !== 1 || (metadata.mode & 0o777) !== 0o600 || metadata.size > 65537) throw new Error('session file must be private and bounded')
const token = (await readFile(tokenPath, 'utf8')).replace(/\n$/, '')
if (!/^[\x21-\x7e]{1,65536}$/.test(token)) throw new Error('invalid session file')
const abort = new AbortController()
const timer = setTimeout(() => abort.abort(), 60000)
let browser, socket
try {
  browser = await startHeadlessBrowser({ executable: process.env.INSIGHT_CONSOLE_BROWSER_BIN ?? '/Applications/Google Chrome.app/Contents/MacOS/Google Chrome', origin: args.get('--endpoint'), signal: abort.signal })
  socket = new WebSocket(browser.pageWebSocketUrl)
  await new Promise((resolveOpen, reject) => {
    const interrupted = () => { socket.close(); reject(new Error('browser debug connection expired')) }
    if (abort.signal.aborted) return interrupted()
    abort.signal.addEventListener('abort', interrupted, { once: true })
    socket.addEventListener('open', () => { abort.signal.removeEventListener('abort', interrupted); resolveOpen() }, { once: true })
    socket.addEventListener('error', () => { abort.signal.removeEventListener('abort', interrupted); reject(new Error('browser debug connection failed')) }, { once: true })
  })
  let sequence = 0
  const pending = new Map()
  socket.addEventListener('message', ({ data }) => { const message = JSON.parse(data); const waiter = pending.get(message.id); if (!waiter) return; pending.delete(message.id); if (message.error) waiter.reject(new Error('browser command failed')); else waiter.resolve(message.result) })
  const fail = () => { for (const waiter of pending.values()) waiter.reject(new Error('browser journey interrupted')); pending.clear() }
  socket.addEventListener('close', fail)
  abort.signal.addEventListener('abort', fail)
  const call = (method, params = {}) => new Promise((resolveResult, reject) => { if (abort.signal.aborted) return reject(new Error('browser journey expired')); const id = ++sequence; pending.set(id, { resolve: resolveResult, reject }); socket.send(JSON.stringify({ id, method, params })) })
  const evaluate = async expression => { const result = await call('Runtime.evaluate', { expression, awaitPromise: true, returnByValue: true }); if (result.exceptionDetails) throw new Error('browser expression failed'); return result.result.value }
  const waitFor = async expression => { for (;;) { if (await evaluate(expression)) return; if (abort.signal.aborted) throw new Error('browser journey expired'); await new Promise(next => setTimeout(next, 100)) } }
  const click = text => evaluate(`(() => { const button = [...document.querySelectorAll('button')].find(item => item.textContent.trim() === ${JSON.stringify(text)} || item.textContent.trim().endsWith(${JSON.stringify(text)})); if (!button || button.disabled) throw new Error('control unavailable'); button.click(); })()`)
  const set = (selector, value) => evaluate(`(() => { const input = document.querySelector(${JSON.stringify(selector)}); Object.getOwnPropertyDescriptor(HTMLInputElement.prototype, 'value').set.call(input, ${JSON.stringify(value)}); input.dispatchEvent(new Event('input', { bubbles: true })); })()`)
  await call('Runtime.enable'); await call('Page.enable')
  await waitFor(`!!document.querySelector('.connection-form')`)
  await set('input[type="url"]', args.get('--endpoint'))
  await set('input[type="password"]', token)
  await evaluate(`document.querySelector('.connection-form').requestSubmit()`)
  await waitFor(`document.body.innerText.includes('Gateway is ready.')`)
  await click('Models')
  await waitFor(`document.querySelector('.model-settings')?.innerText.includes(${JSON.stringify(args.get('--model-alias'))}) && document.querySelector('.model-settings')?.innerText.includes('Selected')`)
  assert.equal(await evaluate(`document.querySelector('.model-settings').innerText.includes('Sources and models')`), true)
  assert.equal(await evaluate(`document.querySelector('.model-settings').innerText.includes('Test connection')`), true)
  assert.equal(await evaluate(`document.querySelector('.model-settings').innerText.includes('Credential')`), true)
  assert.equal(await evaluate(`document.body.innerText.includes(${JSON.stringify(token)})`), false)
  if (args.has('--screenshot')) {
    const screenshot = await call('Page.captureScreenshot', { format: 'png', captureBeyondViewport: true })
    await writeFile(resolve(args.get('--screenshot')), Buffer.from(screenshot.data, 'base64'), { mode: 0o600 })
  }
  console.log(JSON.stringify({ schema_version: 1, journey: 'installed-model-console', status: 'passed', checks: ['actual_gateway_session', 'configured_source_and_model', 'selected_default', 'connection_and_credential_controls', 'no_visible_session_token'] }))
} finally { clearTimeout(timer); socket?.close(); await browser?.close() }
