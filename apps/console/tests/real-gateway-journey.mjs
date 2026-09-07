import { existsSync, mkdtempSync, rmSync } from 'node:fs'
import { createServer } from 'node:net'
import { tmpdir } from 'node:os'
import { join, resolve } from 'node:path'
import { spawn } from 'node:child_process'
import { randomUUID } from 'node:crypto'
import { pathToFileURL } from 'node:url'
import assert from 'node:assert/strict'
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
  const handlers = new Map()
  socket.addEventListener('message', (event) => {
    const message = JSON.parse(event.data)
    if (message.method && handlers.has(message.method)) {
      Promise.resolve(handlers.get(message.method)(message.params)).catch(() => {})
    }
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
    on(method, handler) { handlers.set(method, handler) },
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

function exactPanelStatus(kicker, state) {
  return `(() => {
    const panel = [...document.querySelectorAll('article.panel')].find((candidate) =>
      candidate.querySelector(':scope > .panel__heading .kicker')?.textContent.trim() === ${jsonLiteral(kicker)}
    );
    return panel?.querySelector(':scope > .panel__heading > .status')?.textContent.trim() === ${jsonLiteral(state)};
  })()`
}

function exactPanelText(kicker, text) {
  return `(() => {
    const panel = [...document.querySelectorAll('article.panel')].find((candidate) =>
      candidate.querySelector(':scope > .kicker, :scope > .panel__heading .kicker')?.textContent.trim() === ${jsonLiteral(kicker)}
    );
    return panel?.innerText.includes(${jsonLiteral(text)}) === true;
  })()`
}

function submitSearchAndWaitForIdle(inputSelector) {
  return `new Promise((resolveDone, rejectDone) => {
    const input = document.querySelector(${jsonLiteral(inputSelector)});
    const form = input?.closest('form');
    const button = form?.querySelector('button');
    if (!(form instanceof HTMLFormElement) || !(button instanceof HTMLButtonElement)) {
      rejectDone(new Error('missing search form: ' + ${jsonLiteral(inputSelector)}));
      return;
    }
    let sawBusy = button.disabled;
    let timer;
    const observer = new MutationObserver((records) => {
      if (button.disabled || records.some((record) =>
        record.attributeName === 'disabled' && record.oldValue === null
      )) sawBusy = true;
      if (sawBusy && !button.disabled) {
        clearTimeout(timer);
        observer.disconnect();
        resolveDone(true);
      }
    });
    observer.observe(button, { attributes: true, attributeFilter: ['disabled'], attributeOldValue: true });
    timer = setTimeout(() => {
      observer.disconnect();
      rejectDone(new Error('search form did not complete: ' + ${jsonLiteral(inputSelector)}));
    }, 10_000);
    form.requestSubmit();
  })`
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

function fieldByLabel(label) {
  return `[...document.querySelectorAll('label')].find(candidate => candidate.querySelector(':scope > span')?.textContent.trim() === ${jsonLiteral(label)})?.querySelector('input, textarea, select')`
}

function setField(label, value) {
  return `(() => {
    const element = ${fieldByLabel(label)};
    if (!element) throw new Error('missing field: ' + ${jsonLiteral(label)});
    const prototype = element instanceof HTMLTextAreaElement ? HTMLTextAreaElement.prototype
      : element instanceof HTMLSelectElement ? HTMLSelectElement.prototype : HTMLInputElement.prototype;
    Object.getOwnPropertyDescriptor(prototype, 'value').set.call(element, ${jsonLiteral(value)});
    element.dispatchEvent(new Event(element instanceof HTMLSelectElement ? 'change' : 'input', { bubbles: true }));
  })()`
}

function metricValue(label) {
  return `[...document.querySelectorAll('.metric')].find(metric => metric.querySelector('dt')?.textContent.trim() === ${jsonLiteral(label)})?.querySelector('dd')?.textContent.trim()`
}

// Only the explicit synthetic fixture entry supplies this hook. The executable real journey
// uses the transparent Gateway proxy and never intercepts or replaces browser responses.
export async function runGatewayJourney({ configureSyntheticBrowser } = {}) {
  const gatewayOrigin = required('INSIGHT_CONSOLE_GATEWAY_ORIGIN')
  const managementGatewayOrigin = required('INSIGHT_CONSOLE_MANAGEMENT_GATEWAY_ORIGIN')
  const token = required('INSIGHT_CONSOLE_ACCESS_TOKEN')
  const runId = required('INSIGHT_CONSOLE_RUN_ID')
  const emptyRunId = process.env.INSIGHT_CONSOLE_EMPTY_RUN_ID
  const expectSlowLoading = process.env.INSIGHT_CONSOLE_EXPECT_SLOW_LOADING === '1'
  const taskId = required('INSIGHT_CONSOLE_TASK_ID')
  const taskSafePromptKey = required('INSIGHT_CONSOLE_TASK_SAFE_PROMPT_KEY')
  const deterministicRunId = process.env.INSIGHT_CONSOLE_DETERMINISTIC_RUN_ID
  const timerSignalRunId = process.env.INSIGHT_CONSOLE_TIMER_SIGNAL_RUN_ID
  const subagentRunId = process.env.INSIGHT_CONSOLE_SUBAGENT_RUN_ID
  const artifactId = process.env.INSIGHT_CONSOLE_ARTIFACT_ID
  const contextRunId = process.env.INSIGHT_CONSOLE_CONTEXT_RUN_ID
  const modelRunId = process.env.INSIGHT_CONSOLE_MODEL_RUN_ID
  const capabilityRunId = process.env.INSIGHT_CONSOLE_CAPABILITY_RUN_ID
  const mcpRunId = process.env.INSIGHT_CONSOLE_MCP_RUN_ID
  const sandboxRunId = process.env.INSIGHT_CONSOLE_SANDBOX_RUN_ID
  const responseBody = JSON.parse(required('INSIGHT_CONSOLE_TASK_RESPONSE'))
  const expectedResultText = process.env.INSIGHT_CONSOLE_EXPECTED_RESULT_TEXT ?? 'after task'
  const authoringJourney = process.env.INSIGHT_CONSOLE_AUTHORING_JOURNEY === '1'
  let authoringEvidence
  const bundleRoot = process.env.INSIGHT_CONSOLE_BUNDLE_ROOT
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
  const consoleServer = await startGatewayConsoleServer({ gatewayOrigin, managementGatewayOrigin, bundleRoot })
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
  const authoringNetwork = []
  const requestKinds = new Map()
  const transportFailures = []
  const uploadRequests = new Map()
  const extraHeaders = new Map()
  try {
    const targets = await jsonEventually(`http://127.0.0.1:${debugPort}/json`, Date.now() + 20_000)
    const page = targets.find((target) => target.type === 'page' && target.url.startsWith(consoleServer.origin))
    if (!page) throw new Error('headless browser did not expose the Console page')
    client = cdp(page.webSocketDebuggerUrl)
    await client.call('Runtime.enable')
    await client.call('Page.enable')
    await client.call('Log.enable')
    await client.call('Network.enable')
    client.on('Network.requestWillBeSent', ({ requestId, request }) => {
      const url = new URL(request.url)
      const path = url.pathname
      if (requestKinds.size < 256) requestKinds.set(requestId, { origin: url.origin, method: request.method })
      if (request.method === 'PUT' && url.origin !== consoleServer.origin && uploadRequests.size < 16) uploadRequests.set(requestId, {
        ambient: Object.keys(request.headers).some(name => /^(authorization|cookie)$/i.test(name)),
      })
      if (path.startsWith('/v1/') && (path.includes('/authoring') || !['GET', 'HEAD', 'OPTIONS'].includes(request.method))) authoringNetwork.push({ path, method: request.method })
    })
    client.on('Network.requestWillBeSentExtraInfo', ({ requestId, headers }) => {
      if (extraHeaders.size < 256) extraHeaders.set(requestId, Object.keys(headers).some(name => /^(authorization|cookie)$/i.test(name)))
    })
    client.on('Network.loadingFinished', ({ requestId }) => requestKinds.delete(requestId))
    client.on('Network.loadingFailed', ({ requestId, canceled, errorText, corsErrorStatus }) => {
      if (!canceled && transportFailures.length < 8) transportFailures.push({
        ...requestKinds.get(requestId),
        error: /^net::ERR_[A-Z_]+$/.test(errorText) ? errorText : 'browser_network_failure',
        cors_error: /^[A-Za-z]+$/.test(corsErrorStatus?.corsError ?? '') ? corsErrorStatus.corsError : null,
      })
      requestKinds.delete(requestId)
    })
    if (configureSyntheticBrowser) await configureSyntheticBrowser({ client, consoleOrigin: consoleServer.origin, gatewayOrigin })
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

    if (authoringJourney) {
      const suffix = randomUUID().replaceAll('-', '').slice(0, 16)
      const name = `browser-full-plan-${suffix}`
      const displayName = `Browser Full Plan ${suffix}`
      const explicitInput = { message: `full-plan-echo-${suffix}` }
      await evaluate(client, clickText('New Agent'))
      await waitFor(client, `document.body.innerText.includes('Define an Agent')`, 'new Agent editor')
      await evaluate(client, setField('Name', name))
      await evaluate(client, setField('Display name', displayName))
      const originalSchema = await evaluate(client, `(${fieldByLabel('Input schema JSON')}).value`)
      const beforeInvalidSource = authoringNetwork.length
      await evaluate(client, setField('Input schema JSON', '{ invalid local schema'))
      await evaluate(client, clickText('Validate'))
      await waitFor(client, `!!document.querySelector('.notice--error') && [...document.querySelectorAll('button')].some(button => button.textContent.trim() === 'Validate' && !button.disabled)`, 'source-only rejection before dependency queries')
      assert.deepEqual(authoringNetwork.slice(beforeInvalidSource), [], 'invalid local schema must not query dependencies or create publication state')
      await evaluate(client, setField('Input schema JSON', originalSchema))
      await evaluate(client, clickText('Validate'))
      await waitFor(client, `[...document.querySelectorAll('button')].some(button => button.textContent.trim() === 'Edit validated Plan' && !button.disabled)`, 'shared Rust deterministic compilation')
      const planDigest = await evaluate(client, metricValue('Plan digest'))
      assert.match(planDigest, /^sha256:[0-9a-f]{64}$/)
      // This UI action copies the actual shared compiler output into the editable source.
      await evaluate(client, clickText('Edit validated Plan'))
      await waitFor(client, `(${fieldByLabel('Execution')})?.value === 'full_plan' && !!(${fieldByLabel('Plan JSON')})`, 'Full Plan source editor')
      const planSource = await evaluate(client, `(${fieldByLabel('Plan JSON')}).value`)
      assert.equal(JSON.parse(planSource).plan_version, 6)
      const invalidPlan = JSON.parse(planSource)
      invalidPlan.entry_node_id = 'missing-local-node'
      const beforeInvalidPlan = authoringNetwork.length
      await evaluate(client, setField('Plan JSON', JSON.stringify(invalidPlan)))
      await evaluate(client, clickText('Validate'))
      await waitFor(client, `!!document.querySelector('.notice--error') && [...document.querySelectorAll('button')].some(button => button.textContent.trim() === 'Validate' && !button.disabled)`, 'local Plan structure rejection before dependency queries')
      assert.deepEqual(authoringNetwork.slice(beforeInvalidPlan), [], 'invalid local Plan must not query dependencies or create publication state')
      await evaluate(client, setField('Plan JSON', planSource))
      await evaluate(client, clickText('Validate'))
      await waitFor(client, `${metricValue('Plan digest')} === ${jsonLiteral(planDigest)} && [...document.querySelectorAll('button')].some(button => button.textContent.trim() === 'Publish' && !button.disabled)`, 'Full Plan validation with the same exact Plan digest')
      assert.equal(await evaluate(client, `(${fieldByLabel('Plan JSON')}).value`), planSource)
      await evaluate(client, clickText('Publish'))
      await waitFor(
        client,
        `[...document.querySelectorAll('.agent-row')].some(row => row.querySelector('strong')?.textContent === ${jsonLiteral(displayName)} && [...row.querySelectorAll('span')].some(span => span.textContent === ${jsonLiteral(name)}) && [...row.querySelectorAll('button')].some(button => button.textContent.trim() === 'Run' && !button.disabled))`,
        'exact new Full Plan Agent publication in a populated tenant',
        60_000,
      )
      assert.ok(uploadRequests.size >= 2, 'publication must use actual source and Plan object uploads')
      for (const [requestId, request] of uploadRequests) {
        assert.equal(request.ambient, false, 'signed object upload must not inherit Gateway Authorization or Cookie')
        if (!configureSyntheticBrowser) assert.equal(extraHeaders.get(requestId), false, 'actual browser transport must observe no ambient upload credentials')
      }
      await evaluate(client, `(() => {
        const row = [...document.querySelectorAll('.agent-row')].find(row => row.querySelector('strong')?.textContent === ${jsonLiteral(displayName)});
        const button = [...row.querySelectorAll('button')].find(button => button.textContent.trim() === 'Run');
        if (!button || button.disabled) throw new Error('new Agent is not runnable');
        button.click();
      })()`)
      await waitFor(
        client,
        `${exactPanelText('NEW RUN', `Run ${displayName}`)} && [...document.querySelectorAll('button')].some((button) => button.textContent.trim() === 'Start Run' && !button.disabled)`,
        'exact newly published Agent Run input',
      )
      await evaluate(client, setField('Input JSON', JSON.stringify(explicitInput)))
      await evaluate(client, clickText('Start Run'))
      await waitFor(client, `/^run_/.test(${metricValue('Run ID')} ?? '')`, 'created Run exact identity')
      const createdRunId = await evaluate(client, metricValue('Run ID'))
      const resultRead = `(() => {
        const panel = [...document.querySelectorAll('article.panel')].find(panel => panel.querySelector(':scope > .kicker')?.textContent.trim() === 'TYPED RESULT');
        const text = panel?.querySelector('pre')?.textContent;
        return text ? JSON.parse(text) : null;
      })()`
      const deadline = Date.now() + 60_000
      let output
      while (Date.now() < deadline) {
        await evaluate(client, submitSearchAndWaitForIdle('input[placeholder="run_…"]'))
        output = await evaluate(client, resultRead)
        if (await evaluate(client, exactPanelStatus('RUN', 'succeeded')) && output) break
        await delay(250)
      }
      assert.equal(await evaluate(client, exactPanelStatus('RUN', 'succeeded')), true, 'new Full Plan Run must succeed')
      assert.equal(await evaluate(client, metricValue('Run ID')), createdRunId)
      assert.equal(output?.run_id, createdRunId)
      assert.deepEqual(output?.value, { kind: 'inline', value: explicitInput }, 'exact Full Plan result must echo the explicit submitted input')
      await evaluate(client, clickText('Inspect frozen source'))
      const sourcePanel = `([...document.querySelectorAll('article.panel')].find(panel => panel.querySelector(':scope > .kicker')?.textContent.trim() === 'FROZEN RUN SOURCE'))`
      await waitFor(client, `${sourcePanel}?.innerText.includes('Verified source identity') && ${sourcePanel}?.querySelectorAll('li').length > 0`, 'exact Run source map rebuilt from authorized published bytes')
      await evaluate(client, `(() => { ${sourcePanel}.querySelector('details summary').click() })()`)
      assert.equal(await evaluate(client, `${sourcePanel}.innerText.includes(${jsonLiteral(planDigest)})`), true, 'frozen Run source must identify the compiled Plan')
      const sourceLocations = await evaluate(client, `[...${sourcePanel}.querySelectorAll('li code:first-child')].map(node => node.textContent)`)
      assert.ok(sourceLocations.every(location => /^[^/][^:]*:[1-9][0-9]*:[1-9][0-9]*$/.test(location)), 'source map locations must retain relative files and actual positive line/column')
      authoringEvidence = { agent_name: name, execution_kind: 'full_plan', plan_version: 6, plan_digest: planDigest, run_id: createdRunId, explicit_inline_echo: true, frozen_source_verified: true, invalid_source_preflight_no_http: true, signed_upload_without_ambient_credentials: true }
    }

    if (emptyRunId) {
      await evaluate(client, clickText('Runs'))
      await evaluate(client, setInput('input[placeholder="run_…"]', emptyRunId))
      await evaluate(client, `document.querySelector('.search').requestSubmit()`)
      if (expectSlowLoading) {
        await waitFor(
          client,
          `document.querySelector('.search button').disabled && document.querySelector('.search button').textContent.includes('Loading')`,
          'bounded loading state while the authority is slow',
          500,
        )
      }
      await waitFor(
        client,
        `!!document.querySelector('.empty-state') && document.body.innerText.includes('No public events in this bounded page')`,
        'explicit empty durable timeline',
      )
    }

    if (deterministicRunId) {
      await evaluate(client, clickText('Runs'))
      await evaluate(client, setInput('input[placeholder="run_…"]', deterministicRunId))
      await evaluate(client, submitSearchAndWaitForIdle('input[placeholder="run_…"]'))
      await waitFor(
        client,
        `${exactPanelStatus('RUN', 'succeeded')} && ${exactPanelText('TYPED RESULT', 'hello')}`,
        'exact deterministic Run authority and Inline result',
      )
    }
    if (timerSignalRunId) {
      await evaluate(client, clickText('Runs'))
      await evaluate(client, setInput('input[placeholder="run_…"]', timerSignalRunId))
      await evaluate(client, submitSearchAndWaitForIdle('input[placeholder="run_…"]'))
      await waitFor(
        client,
        `${exactPanelStatus('RUN', 'succeeded')} && ${exactPanelText('TYPED RESULT', 'resume after signal')}`,
        'exact Timer/Signal Run authority and Inline result',
      )
    }
    if (subagentRunId) {
      await evaluate(client, clickText('Runs'))
      await evaluate(client, setInput('input[placeholder="run_…"]', subagentRunId))
      await evaluate(client, submitSearchAndWaitForIdle('input[placeholder="run_…"]'))
      await waitFor(
        client,
        `${exactPanelStatus('RUN', 'succeeded')} && document.body.innerText.includes('child.started') && document.body.innerText.includes('child.completed')`,
        'exact Subagent parent Run and durable child timeline',
      )
    }
    if (artifactId) {
      await evaluate(client, clickText('Settings'))
      await evaluate(client, clickText('Artifacts'))
      await evaluate(client, setInput('input[placeholder="art_…"]', artifactId))
      await evaluate(client, submitSearchAndWaitForIdle('input[placeholder="art_…"]'))
      await waitFor(
        client,
        `(() => {
          const input = document.querySelector('input[placeholder="art_…"]')
          const panel = input?.closest('.nested-panel')
          const metrics = panel && [...panel.querySelectorAll('.metric')]
          const action = panel && [...panel.querySelectorAll('button')]
            .find((button) => button.textContent.trim() === 'Controlled download')
          const exactMetric = (label, value) => metrics && metrics.some((metric) =>
            metric.querySelector('dt')?.textContent.trim() === label
              && metric.querySelector('dd')?.textContent.trim() === value)
          return input instanceof HTMLInputElement
            && input.value === ${jsonLiteral(artifactId)}
            && panel instanceof HTMLElement
            && exactMetric('Artifact ID', ${jsonLiteral(artifactId)})
            && exactMetric('State', 'ready')
            && action instanceof HTMLButtonElement
            && !action.disabled
        })()`,
        'exact Ready Artifact authority and controlled-download action',
      )
    }
    if (contextRunId) {
      await evaluate(client, clickText('Runs'))
      await evaluate(client, setInput('input[placeholder="run_…"]', contextRunId))
      await evaluate(client, submitSearchAndWaitForIdle('input[placeholder="run_…"]'))
      await waitFor(
        client,
        `${exactPanelStatus('RUN', 'succeeded')} && ${exactPanelText('TYPED RESULT', 'local deterministic context item')} && ${exactPanelText('TYPED RESULT', 'observation_only')}`,
        'exact Context Run and citation projection',
      )
    }
    if (modelRunId) {
      await evaluate(client, clickText('Runs'))
      await evaluate(client, setInput('input[placeholder="run_…"]', modelRunId))
      await evaluate(client, submitSearchAndWaitForIdle('input[placeholder="run_…"]'))
      await waitFor(
        client,
        `${exactPanelStatus('RUN', 'succeeded')} && ${exactPanelText('TYPED RESULT', 'deterministic streamed model response')}`,
        'exact Model Run and structured Inline result',
      )
    }
    if (capabilityRunId) {
      await evaluate(client, clickText('Runs'))
      await evaluate(client, setInput('input[placeholder="run_…"]', capabilityRunId))
      await evaluate(client, submitSearchAndWaitForIdle('input[placeholder="run_…"]'))
      await waitFor(
        client,
        `${exactPanelStatus('RUN', 'succeeded')} && ${exactPanelText('TYPED RESULT', 'capability round trip')}`,
        'exact native-to-remote Capability Run and typed Inline result',
      )
    }
    if (mcpRunId) {
      await evaluate(client, clickText('Runs'))
      await evaluate(client, setInput('input[placeholder="run_…"]', mcpRunId))
      await evaluate(client, submitSearchAndWaitForIdle('input[placeholder="run_…"]'))
      await waitFor(
        client,
        `${exactPanelStatus('RUN', 'succeeded')} && ${exactPanelText('TYPED RESULT', 'mcp round trip')}`,
        'exact MCP Capability Run and typed Inline result',
      )
    }
    if (sandboxRunId) {
      await evaluate(client, clickText('Runs'))
      await evaluate(client, setInput('input[placeholder="run_…"]', sandboxRunId))
      await evaluate(client, submitSearchAndWaitForIdle('input[placeholder="run_…"]'))
      await waitFor(
        client,
        `${exactPanelStatus('RUN', 'succeeded')} && ${exactPanelText('TYPED RESULT', 'langgraph: bounded request')}`,
        'exact sandbox/framework Capability Run and bounded typed Inline result',
      )
    }

    await evaluate(client, clickText('Runs'))
    await evaluate(client, setInput('input[placeholder="run_…"]', runId))
    await evaluate(client, submitSearchAndWaitForIdle('input[placeholder="run_…"]'))
    await waitFor(client, `${exactPanelStatus('RUN', 'waiting')} && document.body.innerText.includes(${jsonLiteral(taskId)})`, 'waiting Run and linked Task')
    const eventCanaryAbsent = await evaluate(client, `![
      'browser-token-must-not-render',
      'browser-prompt-must-not-render',
      'browser-secret-must-not-render'
    ].some((canary) => document.documentElement.innerHTML.includes(canary))`)
    if (!eventCanaryAbsent) throw new Error('sensitive event canary reached the DOM')
    await evaluate(client, clickText('Open task'))
    await evaluate(client, submitSearchAndWaitForIdle('input[placeholder="int_… or apv_…"]'))
    await waitFor(client, `${exactPanelStatus('TASK', 'pending')} && ${exactPanelText('TASK', taskSafePromptKey)}`, 'pending Task authority')
    await waitFor(client, `!!document.querySelector('.task-schema-form textarea')`, 'authorized frozen Task form')
    if (responseBody.value?.kind !== 'inline') throw new Error('Task form journey requires an inline response value')
    await evaluate(client, `(() => {
      const label = [...document.querySelectorAll('label')].find(node => node.querySelector('span')?.textContent === 'Response classification');
      const select = label.querySelector('select');
      Object.getOwnPropertyDescriptor(HTMLSelectElement.prototype, 'value').set.call(select, ${jsonLiteral(responseBody.classification)});
      select.dispatchEvent(new Event('change', { bubbles: true }));
    })()`)
    for (const [key, value] of Object.entries(responseBody.value.value)) {
      const path = '/' + key.replaceAll('~', '~0').replaceAll('/', '~1')
      await evaluate(client, `(() => {
        const field = [...document.querySelectorAll('[data-field-path]')].find(node => node.dataset.fieldPath === ${jsonLiteral(path)});
        const input = field?.querySelector('textarea, input:not([type="checkbox"])');
        if (!input || !['string', 'number'].includes(typeof ${jsonLiteral(value)})) throw new Error('Unsupported journey Task field');
        const prototype = input instanceof HTMLTextAreaElement ? HTMLTextAreaElement.prototype : HTMLInputElement.prototype;
        Object.getOwnPropertyDescriptor(prototype, 'value').set.call(input, ${jsonLiteral(String(value))});
        input.dispatchEvent(new Event('input', { bubbles: true }));
      })()`)
    }
    await evaluate(client, clickText('Submit response'))
    await waitFor(client, `${exactPanelStatus('TASK', 'responded')} && document.body.innerText.includes('submit-input committed')`, 'Task mutation authority result')

    await evaluate(client, clickText('Runs'))
    await evaluate(client, setInput('input[placeholder="run_…"]', runId))
    const terminalDeadline = Date.now() + 60_000
    const terminalEvidence = `${exactPanelStatus('RUN', 'succeeded')} && ${exactPanelText('TYPED RESULT', expectedResultText)}`
    let terminal = false
    while (Date.now() < terminalDeadline) {
      await evaluate(client, submitSearchAndWaitForIdle('input[placeholder="run_…"]'))
      terminal = await evaluate(client, terminalEvidence)
      if (terminal) break
      await delay(250)
    }
    if (!terminal) {
      const body = await evaluate(client, `document.body.innerText.slice(0, 4096)`)
      throw new Error(`timed out waiting for terminal Run and safe result; visible page:\n${body}`)
    }
    if (await evaluate(client, `document.documentElement.innerHTML.includes('browser-tool-output-must-not-render')`)) {
      throw new Error('sensitive result canary reached the DOM')
    }

    await client.call('Emulation.setDeviceMetricsOverride', { width: 390, height: 844, deviceScaleFactor: 1, mobile: true })
    const mobileAccessible = await evaluate(client, `({
      noHorizontalOverflow: document.documentElement.scrollWidth <= document.documentElement.clientWidth,
      navigationComplete: ['Agents', 'Runs', 'Tasks', 'Settings'].every((label) =>
        [...document.querySelectorAll('nav button')].some((button) => button.textContent.includes(label))
      ),
      liveRegionPresent: !!document.querySelector('[aria-live]'),
      mainFocusable: document.querySelector('main').tabIndex === -1,
    })`)
    if (!Object.values(mobileAccessible).every(Boolean)) throw new Error(`mobile or ARIA qualification failed: ${JSON.stringify(mobileAccessible)}`)
    await client.call('Emulation.setDeviceMetricsOverride', { width: 1280, height: 900, deviceScaleFactor: 1, mobile: false })

    await client.call('Page.reload', { ignoreCache: true })
    await waitFor(client, `document.readyState === 'complete' && !!document.querySelector('.connection-form')`, 'Console reload')
    const cleared = await evaluate(client, `({
      passwordEmpty: document.querySelector('input[type="password"]').value === '',
      runAbsent: !document.body.innerText.includes(${jsonLiteral(runId)}),
      localStorageEmpty: localStorage.length === 0,
      persistedStateSafe: !Object.values(sessionStorage).some((value) =>
        value.includes(${jsonLiteral(token)}) ||
        value.includes(${jsonLiteral(JSON.stringify(responseBody))}) ||
        value.includes(${jsonLiteral(expectedResultText)}) ||
        value.includes('apiVersion: insight.platform/v1')
      ),
      tokenAbsent: !document.documentElement.innerHTML.includes(${jsonLiteral(token)}),
    })`)
    if (!Object.values(cleared).every(Boolean)) throw new Error(`reload did not clear browser-only authority state: ${JSON.stringify(cleared)}`)
    await evaluate(client, setInput('input[type="password"]', token))
    await evaluate(client, `document.querySelector('.connection-form').requestSubmit()`)
    await waitFor(client, `document.body.innerText.includes('Gateway is ready.')`, 'Gateway readiness after reload')
    await evaluate(client, clickText('Runs'))
    await evaluate(client, setInput('input[placeholder="run_…"]', runId))
    await evaluate(client, submitSearchAndWaitForIdle('input[placeholder="run_…"]'))
    await waitFor(client, terminalEvidence, 'authority Run and safe result re-read after reload')
    if (consoleMessages.some((message) => message.includes(token))) throw new Error('access token appeared in browser console output')
    observer.close()
    process.stdout.write(`${JSON.stringify({
      kind: configureSyntheticBrowser ? 'insight.console.synthetic-gateway-journey/v1' : 'insight.console.real-gateway-journey/v1',
      status: 'passed',
      gateway_origin: gatewayOrigin,
      management_gateway_origin: managementGatewayOrigin,
      run_id: runId,
      task_id: taskId,
      deterministic_run_id: deterministicRunId,
      timer_signal_run_id: timerSignalRunId,
      subagent_run_id: subagentRunId,
      artifact_id: artifactId,
      context_run_id: contextRunId,
      model_run_id: modelRunId,
      capability_run_id: capabilityRunId,
      mcp_run_id: mcpRunId,
      sandbox_run_id: sandboxRunId,
      authoring: authoringEvidence,
      checks: [
        'gateway_ready',
        ...(authoringJourney ? ['agent_authoring_north_star', 'full_plan_shared_compiler_publication', 'exact_authoring_inline_echo'] : []),
        ...(emptyRunId ? ['empty_run_timeline'] : []),
        ...(expectSlowLoading ? ['slow_dependency_busy_state'] : []),
        ...(deterministicRunId ? ['deterministic_run_read'] : []),
        ...(timerSignalRunId ? ['timer_signal_run_read'] : []),
        ...(subagentRunId ? ['subagent_run_read'] : []),
        ...(artifactId ? ['artifact_ready_read'] : []),
        ...(contextRunId ? ['context_run_read'] : []),
        ...(modelRunId ? ['model_run_read'] : []),
        ...(capabilityRunId ? ['capability_run_read'] : []),
        ...(mcpRunId ? ['mcp_run_read'] : []),
        ...(sandboxRunId ? ['sandbox_run_read'] : []),
        'sse_task_discovery',
        'task_mutation',
        'terminal_run',
        'reload_authority_read',
        'memory_only_token',
        'dom_canary_redaction',
        'mobile_aria_layout',
      ],
    })}\n`)
  } catch (error) {
    throw new Error(`${error instanceof Error ? error.stack ?? error.message : String(error)}\nSafe transport failures: ${JSON.stringify(transportFailures)}`)
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

if (process.argv[1] && import.meta.url === pathToFileURL(resolve(process.argv[1])).href) runGatewayJourney().catch((error) => {
  process.stderr.write(`${error instanceof Error ? error.stack ?? error.message : String(error)}\n`)
  process.exit(1)
})
