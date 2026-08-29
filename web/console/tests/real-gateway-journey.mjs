import { existsSync, mkdtempSync, rmSync } from 'node:fs'
import { createServer } from 'node:net'
import { tmpdir } from 'node:os'
import { join, resolve } from 'node:path'
import { spawn } from 'node:child_process'
import { startGatewayConsoleServer } from './gateway-server.mjs'

const required = (name) => {
  const value = process.env[name]
  if (!value) throw new Error(`${name} is required`)
  return value
}

const delay = (milliseconds) => new Promise((resolveDelay) => setTimeout(resolveDelay, milliseconds))

function waitForChild(child, timeoutMilliseconds) {
  if (child.exitCode !== null || child.signalCode !== null) return Promise.resolve()
  return new Promise((resolveExit) => {
    const finish = () => {
      clearTimeout(timer)
      child.removeListener('exit', finish)
      resolveExit()
    }
    const timer = setTimeout(() => {
      child.kill('SIGKILL')
      finish()
    }, timeoutMilliseconds)
    child.once('exit', finish)
  })
}

async function unusedPort() {
  const server = createServer()
  await new Promise((resolveListen, rejectListen) => {
    server.once('error', rejectListen)
    server.listen(0, '127.0.0.1', resolveListen)
  })
  const address = server.address()
  if (!address || typeof address === 'string') throw new Error('could not reserve a browser debug port')
  const { port } = address
  await new Promise((resolveClose) => server.close(resolveClose))
  return port
}

async function jsonEventually(url, deadline) {
  let lastError
  while (Date.now() < deadline) {
    try {
      const response = await fetch(url)
      if (response.ok) return await response.json()
      lastError = new Error(`HTTP ${response.status}`)
    } catch (error) {
      lastError = error
    }
    await delay(50)
  }
  throw new Error(`browser debugging endpoint did not become ready: ${lastError instanceof Error ? lastError.message : String(lastError)}`)
}

function cdp(webSocketUrl) {
  const socket = new WebSocket(webSocketUrl)
  let sequence = 0
  const pending = new Map()
  socket.addEventListener('message', (event) => {
    const message = JSON.parse(event.data)
    const waiter = pending.get(message.id)
    if (!waiter) return
    pending.delete(message.id)
    if (message.error) waiter.reject(new Error(message.error.message))
    else waiter.resolve(message.result)
  })
  const opened = new Promise((resolveOpen, rejectOpen) => {
    socket.addEventListener('open', resolveOpen, { once: true })
    socket.addEventListener('error', rejectOpen, { once: true })
  })
  return {
    async call(method, params = {}) {
      await opened
      const id = ++sequence
      const result = new Promise((resolveResult, rejectResult) => pending.set(id, { resolve: resolveResult, reject: rejectResult }))
      socket.send(JSON.stringify({ id, method, params }))
      return result
    },
    close() { socket.close() },
  }
}

async function evaluate(client, expression) {
  const result = await client.call('Runtime.evaluate', { expression, awaitPromise: true, returnByValue: true })
  if (result.exceptionDetails) throw new Error(result.exceptionDetails.exception?.description ?? result.exceptionDetails.text)
  return result.result.value
}

async function waitFor(client, expression, description, timeoutMilliseconds = 30_000) {
  const deadline = Date.now() + timeoutMilliseconds
  while (Date.now() < deadline) {
    if (await evaluate(client, expression)) return
    await delay(100)
  }
  const body = await evaluate(client, `document.body.innerText.slice(0, 4096)`)
  throw new Error(`timed out waiting for ${description}; visible page:\n${body}`)
}

function jsonLiteral(value) {
  return JSON.stringify(value).replaceAll('<', '\\u003c')
}

function setInput(selector, value) {
  return `(() => {
    const element = document.querySelector(${jsonLiteral(selector)});
    if (!element) throw new Error('missing input: ' + ${jsonLiteral(selector)});
    const prototype = element instanceof HTMLTextAreaElement ? HTMLTextAreaElement.prototype : HTMLInputElement.prototype;
    Object.getOwnPropertyDescriptor(prototype, 'value').set.call(element, ${jsonLiteral(value)});
    element.dispatchEvent(new Event('input', { bubbles: true }));
  })()`
}

function clickText(text, selector = 'button') {
  return `(() => {
    const element = [...document.querySelectorAll(${jsonLiteral(selector)})].find((candidate) => {
      const label = candidate.textContent.trim();
      return label === ${jsonLiteral(text)} || label.endsWith(${jsonLiteral(text)});
    });
    if (!element) throw new Error('missing control: ' + ${jsonLiteral(text)});
    element.click();
  })()`
}

async function main() {
  const gatewayOrigin = required('INSIGHT_CONSOLE_GATEWAY_ORIGIN')
  const token = required('INSIGHT_CONSOLE_ACCESS_TOKEN')
  const runId = required('INSIGHT_CONSOLE_RUN_ID')
  const taskId = required('INSIGHT_CONSOLE_TASK_ID')
  const deterministicRunId = process.env.INSIGHT_CONSOLE_DETERMINISTIC_RUN_ID
  const timerSignalRunId = process.env.INSIGHT_CONSOLE_TIMER_SIGNAL_RUN_ID
  const responseBody = JSON.parse(required('INSIGHT_CONSOLE_TASK_RESPONSE'))
  const expectedResultText = process.env.INSIGHT_CONSOLE_EXPECTED_RESULT_TEXT ?? 'after task'
  const browser = [
    process.env.INSIGHT_CONSOLE_BROWSER_BIN,
    '/Applications/Google Chrome.app/Contents/MacOS/Google Chrome',
    '/usr/bin/google-chrome',
    '/usr/bin/google-chrome-stable',
    '/usr/bin/chromium',
    '/usr/bin/chromium-browser',
  ]
    .filter(Boolean)
    .map((candidate) => resolve(candidate))
    .find((candidate) => existsSync(candidate))
  if (!browser) throw new Error('an executable Chromium or Chrome browser is required')
  const consoleServer = await startGatewayConsoleServer({ gatewayOrigin })
  const browserProfile = mkdtempSync(join(tmpdir(), 'insight-console-browser-'))
  const debugPort = await unusedPort()
  const browserProcess = spawn(browser, [
    '--headless=new',
    '--disable-background-networking',
    '--disable-component-update',
    '--disable-default-apps',
    '--disable-extensions',
    '--disable-gpu',
    '--disable-sync',
    '--metrics-recording-only',
    '--no-first-run',
    '--no-sandbox',
    `--remote-debugging-port=${debugPort}`,
    `--user-data-dir=${browserProfile}`,
    consoleServer.origin,
  ], { stdio: ['ignore', 'ignore', 'pipe'] })
  let browserErrors = ''
  browserProcess.stderr.on('data', (chunk) => { browserErrors = `${browserErrors}${chunk}`.slice(-8192) })

  let client
  let observer
  const consoleMessages = []
  try {
    const targets = await jsonEventually(`http://127.0.0.1:${debugPort}/json`, Date.now() + 20_000)
    const page = targets.find((target) => target.type === 'page' && target.url.startsWith(consoleServer.origin))
    if (!page) throw new Error('headless browser did not expose the Console page')
    client = cdp(page.webSocketDebuggerUrl)
    await client.call('Runtime.enable')
    await client.call('Page.enable')
    await client.call('Log.enable')
    // Runtime console events are not request/response messages, so collect them through a second
    // small protocol connection dedicated to passive observation.
    observer = new WebSocket(page.webSocketDebuggerUrl)
    await new Promise((resolveOpen, rejectOpen) => {
      observer.addEventListener('open', resolveOpen, { once: true })
      observer.addEventListener('error', rejectOpen, { once: true })
    })
    observer.addEventListener('message', (event) => {
      const message = JSON.parse(event.data)
      if (message.method === 'Runtime.consoleAPICalled' || message.method === 'Log.entryAdded') consoleMessages.push(JSON.stringify(message.params))
    })
    observer.send(JSON.stringify({ id: 1, method: 'Runtime.enable' }))
    observer.send(JSON.stringify({ id: 2, method: 'Log.enable' }))

    await waitFor(client, `document.readyState === 'complete' && !!document.querySelector('.connection-form')`, 'Console application load')
    await evaluate(client, setInput('input[type="url"]', consoleServer.origin))
    await evaluate(client, setInput('input[type="password"]', token))
    await evaluate(client, `document.querySelector('.connection-form').requestSubmit()`)
    await waitFor(client, `document.body.innerText.includes('Gateway is ready.')`, 'real Gateway readiness')

    if (deterministicRunId) {
      await evaluate(client, clickText('Runs'))
      await evaluate(client, setInput('input[placeholder="run_…"]', deterministicRunId))
      await evaluate(client, `document.querySelector('.search').requestSubmit()`)
      await waitFor(
        client,
        `document.body.innerText.toLowerCase().includes('succeeded') && document.body.innerText.includes('hello')`,
        'exact deterministic Run authority and Inline result',
      )
    }
    if (timerSignalRunId) {
      await evaluate(client, clickText('Runs'))
      await evaluate(client, setInput('input[placeholder="run_…"]', timerSignalRunId))
      await evaluate(client, `document.querySelector('.search').requestSubmit()`)
      await waitFor(
        client,
        `document.body.innerText.toLowerCase().includes('succeeded') && document.body.innerText.includes('resume after signal')`,
        'exact Timer/Signal Run authority and Inline result',
      )
    }

    await evaluate(client, clickText('Runs'))
    await evaluate(client, setInput('input[placeholder="run_…"]', runId))
    await evaluate(client, `document.querySelector('.search').requestSubmit()`)
    await waitFor(client, `document.body.innerText.includes(${jsonLiteral(taskId)}) && document.body.innerText.toLowerCase().includes('waiting')`, 'waiting Run and linked Task')
    await evaluate(client, clickText(taskId))
    await evaluate(client, `document.querySelector('.search').requestSubmit()`)
    await waitFor(client, `document.body.innerText.includes(${jsonLiteral(taskId)}) && document.body.innerText.toLowerCase().includes('pending')`, 'pending Task authority')
    await evaluate(client, setInput('textarea', JSON.stringify(responseBody, null, 2)))
    await evaluate(client, clickText('Submit input'))
    await waitFor(client, `document.body.innerText.toLowerCase().includes('responded') && document.body.innerText.includes('submit-input committed')`, 'Task mutation authority result')

    await evaluate(client, clickText('Runs'))
    await evaluate(client, setInput('input[placeholder="run_…"]', runId))
    const terminalDeadline = Date.now() + 60_000
    while (Date.now() < terminalDeadline) {
      await evaluate(client, `document.querySelector('.search').requestSubmit()`)
      if (await evaluate(client, `document.body.innerText.toLowerCase().includes('succeeded')`)) break
      await delay(250)
    }
    await waitFor(client, `document.body.innerText.toLowerCase().includes('succeeded') && document.body.innerText.includes(${jsonLiteral(expectedResultText)})`, 'terminal Run and safe result')

    await client.call('Page.reload', { ignoreCache: true })
    await waitFor(client, `document.readyState === 'complete' && !!document.querySelector('.connection-form')`, 'Console reload')
    const cleared = await evaluate(client, `({
      passwordEmpty: document.querySelector('input[type="password"]').value === '',
      runAbsent: !document.body.innerText.includes(${jsonLiteral(runId)}),
      localStorageEmpty: localStorage.length === 0,
      sessionStorageEmpty: sessionStorage.length === 0,
      tokenAbsent: !document.documentElement.innerHTML.includes(${jsonLiteral(token)}),
    })`)
    if (!Object.values(cleared).every(Boolean)) throw new Error(`reload did not clear browser-only authority state: ${JSON.stringify(cleared)}`)
    await evaluate(client, setInput('input[type="password"]', token))
    await evaluate(client, `document.querySelector('.connection-form').requestSubmit()`)
    await waitFor(client, `document.body.innerText.includes('Gateway is ready.')`, 'Gateway readiness after reload')
    await evaluate(client, clickText('Runs'))
    await evaluate(client, setInput('input[placeholder="run_…"]', runId))
    await evaluate(client, `document.querySelector('.search').requestSubmit()`)
    await waitFor(client, `document.body.innerText.toLowerCase().includes('succeeded')`, 'authority Run re-read after reload')
    if (consoleMessages.some((message) => message.includes(token))) throw new Error('access token appeared in browser console output')
    observer.close()
    process.stdout.write(`${JSON.stringify({
      kind: 'insight.console.real-gateway-journey/v1',
      status: 'passed',
      gateway_origin: gatewayOrigin,
      run_id: runId,
      task_id: taskId,
      deterministic_run_id: deterministicRunId,
      timer_signal_run_id: timerSignalRunId,
      checks: [
        'gateway_ready',
        ...(deterministicRunId ? ['deterministic_run_read'] : []),
        ...(timerSignalRunId ? ['timer_signal_run_read'] : []),
        'sse_task_discovery',
        'task_mutation',
        'terminal_run',
        'reload_authority_read',
        'memory_only_token',
      ],
    })}\n`)
  } finally {
    observer?.close()
    client?.close()
    browserProcess.kill('SIGTERM')
    await waitForChild(browserProcess, 5_000)
    await consoleServer.close()
    rmSync(browserProfile, { recursive: true, force: true })
  }
  if (browserProcess.exitCode && browserProcess.exitCode !== 0) throw new Error(`browser exited ${browserProcess.exitCode}: ${browserErrors}`)
}

main().catch((error) => {
  process.stderr.write(`${error instanceof Error ? error.stack ?? error.message : String(error)}\n`)
  process.exit(1)
})
