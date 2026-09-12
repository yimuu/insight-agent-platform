import { tcpPort } from './fixtures/types.ts'
import assert from 'node:assert/strict'
import { spawn } from 'node:child_process'
import { existsSync, mkdtempSync, readFileSync, rmSync } from 'node:fs'
import { createServer } from 'node:http'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import test from 'node:test'
import { startGatewayConsoleServer } from '../server/native.ts'

const browserBinary =
  process.env.INSIGHT_CONSOLE_BROWSER_BIN ??
  '/Applications/Google Chrome.app/Contents/MacOS/Google Chrome'
const bundleRoot = process.env.INSIGHT_CONSOLE_BUNDLE_ROOT
const delay = (milliseconds) => new Promise<void>((resolve) => setTimeout(resolve, milliseconds))
const runA = 'run_0198f1c3-8f49-7c3e-b1f3-773c28367b90'
const runB = 'run_0198f1c3-8f49-7c3e-b1f3-773c28367b91'
const runC = 'run_0198f1c3-8f49-7c3e-b1f3-773c28367b92'
const runWithoutOutput = 'run_0198f1c3-8f49-7c3e-b1f3-773c28367b94'

async function eventually(check, label) {
  const deadline = Date.now() + 8_000
  while (Date.now() < deadline) {
    if (await check()) return
    await delay(30)
  }
  throw new Error(`Timed out: ${label}`)
}

async function debuggingClient(url) {
  const socket = new WebSocket(url)
  await new Promise((resolve, reject) => {
    socket.addEventListener('open', resolve, { once: true })
    socket.addEventListener('error', reject, { once: true })
  })
  let nextId = 0
  const pending = new Map()
  const handlers = new Map()
  socket.addEventListener('message', ({ data }) => {
    const message = JSON.parse(data)
    handlers.get(message.method)?.(message.params)
    const request = pending.get(message.id)
    if (!request) return
    pending.delete(message.id)
    if (message.error) request.reject(new Error(message.error.message))
    else request.resolve(message.result)
  })
  return {
    close: () => socket.close(),
    on: (method, handler) => handlers.set(method, handler),
    call(method, params = {}) {
      const id = ++nextId
      const result = new Promise((resolve, reject) => pending.set(id, { resolve, reject }))
      socket.send(JSON.stringify({ id, method, params }))
      return result
    },
  }
}

function runView(id, state = 'running') {
  return {
    schema_version: 1,
    run_id: id,
    agent_deployment_id: 'adep_0198f1c3-8f49-7c3e-b1f3-773c28367b93',
    state,
    version: state === 'succeeded' ? 2 : 1,
    etag: '"run-v1"',
    output_value_id: state === 'succeeded' ? 'val_0198f1c3-8f49-7c3e-b1f3-773c28367b95' : null,
    created_at: '2026-09-01T00:00:00Z',
    started_at: '2026-09-01T00:00:00Z',
    updated_at: '2026-09-01T00:00:00Z',
    deadline: '2026-09-07T00:00:00Z',
  }
}

function frame(sequence, cursor, eventType = 'run.started') {
  const envelope = {
    schema_version: 1,
    event_id: `evt_0198f1c3-8f49-7c3e-b1f3-773c28367ba${sequence}`,
    run_id: runA,
    cursor,
    sequence,
    event_type: eventType,
    durability: 'durable',
    trace_id: '0123456789abcdef0123456789abcdef',
    occurred_at: '2026-09-01T00:00:00Z',
    data: {
      source_kind: 'run',
      source_id: runA,
      source_projection_version: sequence,
      safe_summary: null,
    },
  }
  return `id: ${cursor}\nevent: ${eventType}\ndata: ${JSON.stringify(envelope)}\n\n`
}

// Explicit browser qualification; excluded from the ordinary fast node test glob.
test(
  'Run UI follows, preserves a history error, and fences identity/resource changes',
  {
    skip:
      !bundleRoot || !existsSync(browserBinary)
        ? 'Requires an explicit Console bundle and Chrome executable'
        : false,
    timeout: 30_000,
  },
  async () => {
    let terminal = false
    let resultFailure = false
    let rejectCursor = false
    let releaseB
    let releaseC
    let releaseTask
    let runReadsInFlight = 0
    let maximumConcurrentRunReads = 0
    let pendingStreamCancelled = false
    const requests = []
    const send = (response, status, body) => {
      response.writeHead(status, {
        'content-type': 'application/json',
        'cache-control': 'no-store',
      })
      response.end(JSON.stringify(body))
    }
    const api = createServer((request, response) => {
      if (request.url.startsWith('/v1/agents?')) {
        response.setHeader('content-type', 'application/json')
        response.end(JSON.stringify({ schema_version: 1, items: [], next_cursor: null }))
        return
      }
      const path = request.url
      requests.push({
        path,
        cursor: request.headers['last-event-id'],
        auth: request.headers.authorization,
      })
      if (path === '/readyz') {
        response.end('ready')
        return
      }
      if (path === `/v1/runs/${runA}`) {
        const view = runView(runA, terminal ? 'succeeded' : 'running')
        maximumConcurrentRunReads = Math.max(maximumConcurrentRunReads, ++runReadsInFlight)
        setTimeout(() => {
          runReadsInFlight--
          send(response, 200, view)
        }, 30)
        return
      }
      if (path === `/v1/runs/${runB}`) {
        releaseB = () => send(response, 200, runView(runB))
        return
      }
      if (path === `/v1/runs/${runC}`) {
        releaseC = () => send(response, 200, runView(runC))
        return
      }
      if (path === `/v1/runs/${runWithoutOutput}`) {
        send(response, 200, runView(runWithoutOutput, 'failed'))
        return
      }
      if (path === `/v1/runs/${runWithoutOutput}/events`) {
        response.writeHead(200, {
          'content-type': 'text/event-stream',
          'x-insight-run-replay-floor': '0',
          'x-insight-run-high-water': '0',
          'x-insight-history-truncated': 'false',
        })
        response.end(': no events\n\n')
        return
      }
      if (path === `/v1/runs/${runA}/result`) {
        if (resultFailure)
          send(response, 503, {
            code: 'content_unavailable',
            detail: 'Current result content is unavailable.',
            retryable: true,
          })
        else
          send(response, 200, {
            value: { kind: 'inline', value: { answer: 'authorized-result-canary' } },
          })
        return
      }
      if (path === '/v1/tasks/int_old?purpose=respondable') {
        releaseTask = () =>
          send(response, 200, {
            task_id: 'int_old',
            state: 'pending',
            safe_prompt_key: 'old-task-private-canary',
          })
        return
      }
      if (path === `/v1/runs/${runB}/events`) {
        response.writeHead(200, {
          'content-type': 'text/event-stream',
          'x-insight-run-replay-floor': '0',
          'x-insight-run-high-water': '2',
          'x-insight-history-truncated': 'false',
        })
        response.write(': waiting\n\n')
        return
      }
      if (path === `/v1/runs/${runC}/events`) {
        send(response, 403, {
          code: 'permission_denied',
          detail: 'Runtime read permission was revoked.',
          retryable: false,
        })
        return
      }
      if (path === `/v1/runs/${runA}/events`) {
        const cursor = request.headers['last-event-id']
        if (cursor === 'opaque-2' && rejectCursor) {
          send(response, 400, {
            code: 'cursor_expired',
            detail: 'The Run event cursor has expired.',
            retryable: false,
          })
          return
        }
        response.writeHead(200, {
          'content-type': 'text/event-stream',
          'x-insight-run-replay-floor': '0',
          'x-insight-run-high-water': '2',
          'x-insight-history-truncated': 'false',
        })
        // A terminal event must never replace a fresh Run authority read.
        response.end(
          !cursor
            ? frame(1, 'opaque-1', 'run.completed')
            : cursor === 'opaque-1'
              ? frame(1, 'reissued-1', 'run.completed') + frame(2, 'opaque-2')
              : '',
        )
        return
      }
      send(response, 404, {
        code: 'not_found',
        detail: 'Fixture resource unavailable',
        retryable: false,
      })
    })
    await new Promise<void>((resolve) => api.listen(0, '127.0.0.1', resolve))
    const consoleServer = await startGatewayConsoleServer({
      gatewayOrigin: `http://127.0.0.1:${tcpPort(api)}`,
      managementGatewayOrigin: `http://127.0.0.1:${tcpPort(api)}`,
      bundleRoot,
    })
    const profile = mkdtempSync(join(tmpdir(), 'insight-console-sse-browser-'))
    const chrome = spawn(
      browserBinary,
      [
        '--headless=new',
        '--no-sandbox',
        '--no-first-run',
        '--disable-background-networking',
        '--remote-debugging-port=0',
        `--user-data-dir=${profile}`,
        consoleServer.origin,
      ],
      { stdio: 'ignore' },
    )
    let client
    try {
      const activePort = join(profile, 'DevToolsActivePort')
      await eventually(() => existsSync(activePort), 'Chrome debugging port')
      const port = readFileSync(activePort, 'utf8').split('\n')[0]
      let page
      await eventually(async () => {
        const targets = await (await fetch(`http://127.0.0.1:${port}/json`)).json()
        page = targets.find((target) => target.type === 'page')
        return page
      }, 'Chrome page')
      client = await debuggingClient(page.webSocketDebuggerUrl)
      await client.call('Runtime.enable')
      const networkRequests = new Map()
      client.on('Network.requestWillBeSent', ({ requestId, request }) =>
        networkRequests.set(requestId, request.url),
      )
      client.on('Network.loadingFailed', ({ requestId, canceled }) => {
        if (canceled && networkRequests.get(requestId)?.endsWith(`/v1/runs/${runB}/events`))
          pendingStreamCancelled = true
      })
      await client.call('Network.enable')
      const evaluate = async (expression) => {
        const output = await client.call('Runtime.evaluate', {
          expression,
          returnByValue: true,
          awaitPromise: true,
        })
        if (output.exceptionDetails)
          throw new Error(
            output.exceptionDetails.exception?.description ?? output.exceptionDetails.text,
          )
        return output.result.value
      }
      const waitDom = (expression, label) => eventually(() => evaluate(expression), label)
      const input = (selector, value) =>
        evaluate(`(() => {
      const element = document.querySelector(${JSON.stringify(selector)});
      Object.getOwnPropertyDescriptor(HTMLInputElement.prototype, 'value').set.call(element, ${JSON.stringify(value)});
      element.dispatchEvent(new Event('input', { bubbles: true }));
    })()`)
      const connect = async (token: string) => {
        if (!(await evaluate(`!!document.querySelector('input[type="password"]')`))) {
          await evaluate(`document.querySelector('header details > summary').click()`)
          await evaluate(
            `[...document.querySelectorAll('button')].find(node => node.textContent === '更换连接').click()`,
          )
        }
        await waitDom(`!!document.querySelector('input[type="password"]')`, 'connection page')
        await input('input[type="password"]', token)
        await evaluate(`document.querySelector('[data-ui~=connection-form]').requestSubmit()`)
        await waitDom(`!!document.querySelector('nav')`, 'authenticated workspace')
        await evaluate(
          `[...document.querySelectorAll('nav a')].find(node => node.textContent.includes('运行记录')).click()`,
        )
        await waitDom(`!!document.querySelector('input[placeholder="run_…"]')`, 'Runs page')
      }
      const selectRunInput = async (id) => {
        if (!(await evaluate(`!!document.querySelector('input[placeholder="run_…"]')`))) {
          await evaluate(
            `[...document.querySelectorAll('button')].find(button => button.textContent.includes('返回运行记录')).click()`,
          )
        }
        await waitDom(`!!document.querySelector('input[placeholder="run_…"]')`, 'Run lookup')
        await evaluate(
          `document.querySelector('input[placeholder="run_…"]').closest('details:not([open])')?.querySelector(':scope > summary')?.click()`,
        )
        await input('input[placeholder="run_…"]', id)
      }
      const openRun = async (id) => {
        await selectRunInput(id)
        await evaluate(
          `document.querySelector('input[placeholder="run_…"]').closest('form').requestSubmit()`,
        )
      }
      await waitDom(`!!document.querySelector('input[type="password"]')`, 'Console mount')
      await connect('principal-one-fixture-token')
      await evaluate(
        `[...document.querySelectorAll('nav a')].find(button => button.textContent.includes('运行记录')).click()`,
      )
      await openRun(runA)
      await waitDom(
        `document.querySelectorAll('[data-ui~=timeline] li').length === 2`,
        'continuous finite-page follow and event_id deduplication',
      )
      await waitDom(
        `document.querySelector('[data-ui~=panel__heading] > [data-ui~=status]')?.getAttribute('data-status') === 'running'`,
        'Run authority read',
      )
      assert.equal(
        await evaluate(
          `document.querySelector('[data-ui~=panel__heading] > [data-ui~=status]')?.getAttribute('data-status')`,
        ),
        'running',
      )
      assert.equal(
        await evaluate(`document.body.innerText.includes('authorized-result-canary')`),
        false,
      )
      assert.equal(
        await evaluate(`Object.keys(sessionStorage).some(key => key.includes('run-cursor'))`),
        false,
      )

      rejectCursor = true
      await waitDom(
        `document.body.innerText.includes('已停止跟随时间线。') && document.body.innerText.includes('cursor has expired')`,
        'explicit history error',
      )
      const eventRequestCount = requests.filter(({ path }) => path.endsWith('/events')).length
      terminal = true
      await evaluate(
        `[...document.querySelectorAll('button')].filter(button => button.textContent === '刷新').at(-1).click()`,
      )
      await waitDom(
        `document.body.innerText.includes('authorized-result-canary')`,
        'authoritative terminal result after explicit refresh',
      )
      await delay(300)
      assert.equal(
        requests.filter(({ path }) => path.endsWith('/events')).length,
        eventRequestCount,
      )
      assert.equal(await evaluate(`document.body.innerText.includes('已停止跟随时间线。')`), true)
      assert.equal(maximumConcurrentRunReads, 1)
      resultFailure = true
      await evaluate(
        `[...document.querySelectorAll('button')].filter(button => button.textContent === '刷新').at(-1).click()`,
      )
      await waitDom(
        `[...document.querySelectorAll('[role=alert]')].some(node => node.textContent.includes('content_unavailable'))`,
        'current result failure',
      )
      await evaluate(`(() => {
        const notice = [...document.querySelectorAll('[role=alert]')].find(node => node.textContent.includes('content_unavailable'));
        notice.querySelector('details:not([open]) > summary')?.click();
      })()`)
      assert.equal(await evaluate(`document.body.innerText.includes('content_unavailable')`), true)
      assert.equal(
        await evaluate(`document.body.innerText.includes('authorized-result-canary')`),
        false,
        'every failed content read clears previously authorized body',
      )
      resultFailure = false
      await evaluate(
        `[...document.querySelectorAll('button')].filter(button => button.textContent === '刷新').at(-1).click()`,
      )
      await waitDom(
        `document.body.innerText.includes('authorized-result-canary')`,
        'fresh result authorization restores body',
      )

      await openRun(runWithoutOutput)
      await waitDom(
        `document.querySelector('[data-ui~=panel__heading] > [data-ui~=status]')?.getAttribute('data-status') === 'failed'`,
        'failed Run without an output',
      )
      await delay(100)
      assert.equal(
        requests.some(({ path }) => path === `/v1/runs/${runWithoutOutput}/result`),
        false,
        'an absent output is not a result read capability',
      )
      assert.equal(
        await evaluate(`document.body.innerText.includes('authorized-result-canary')`),
        false,
      )

      await selectRunInput(runB)
      assert.equal(
        await evaluate(
          `document.body.innerText.includes('authorized-result-canary') || document.querySelectorAll('[data-ui~=timeline] li').length > 0`,
        ),
        false,
      )
      await evaluate(
        `document.querySelector('input[placeholder="run_…"]').closest('form').requestSubmit()`,
      )
      await eventually(() => releaseB, 'pending second Run read')
      await connect('principal-two-fixture-token')
      await waitDom(
        `document.querySelector('input[placeholder="run_…"]').value === ''`,
        'identity change clears selected Run',
      )
      await eventually(
        () => pendingStreamCancelled,
        'identity change aborts old browser SSE request',
      )
      releaseB()
      await delay(100)
      assert.equal(
        await evaluate(
          `document.body.innerText.includes(${JSON.stringify(runB)}) || document.body.innerText.includes('authorized-result-canary')`,
        ),
        false,
      )

      await openRun(runC)
      await eventually(() => releaseC, 'Run read concurrent with revocation')
      await waitDom(
        `document.body.innerText.includes('permission was revoked')`,
        'revocation error',
      )
      releaseC()
      await delay(100)
      assert.equal(
        await evaluate(
          `!!document.querySelector('[data-ui~=timeline]') || [...document.querySelectorAll('[data-ui~=kicker]')].some(node => node.textContent === '运行详情')`,
        ),
        false,
      )
      assert.equal(requests.filter(({ path }) => path === `/v1/runs/${runC}/events`).length, 1)
      assert.equal(
        requests.find(({ path }) => path === `/v1/runs/${runC}/events`).auth,
        'Bearer principal-two-fixture-token',
      )

      await evaluate(
        `[...document.querySelectorAll('nav a')].find(button => button.textContent.includes('待办任务')).click()`,
      )
      await waitDom(`!!document.querySelector('input[placeholder="int_… 或 apv_…"]')`, 'Tasks page')
      await input('input[placeholder="int_… 或 apv_…"]', 'int_old')
      await evaluate(
        `document.querySelector('input[placeholder="int_… 或 apv_…"]').closest('form').requestSubmit()`,
      )
      await eventually(() => releaseTask, 'pending Task read')
      await input('input[placeholder="int_… 或 apv_…"]', 'int_new')
      releaseTask()
      await delay(100)
      assert.equal(
        await evaluate(
          `document.body.innerText.includes('old-task-private-canary') || !!document.querySelector('textarea')`,
        ),
        false,
      )
    } finally {
      client?.close()
      chrome.kill('SIGTERM')
      await new Promise<void>((resolve) => {
        if (chrome.exitCode !== null) resolve()
        else chrome.once('exit', resolve)
      })
      api.closeAllConnections()
      await consoleServer.close()
      await new Promise((resolve) => api.close(resolve))
      rmSync(profile, { recursive: true, force: true })
    }
  },
)
