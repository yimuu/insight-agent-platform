import { fieldElement as fieldByLabel, fieldValue, setField } from './browser-fields.ts'
import { existsSync } from 'node:fs'
import { resolve } from 'node:path'
import { randomUUID } from 'node:crypto'
import { pathToFileURL } from 'node:url'
import assert from 'node:assert/strict'
import { startGatewayConsoleServer } from '../server/native.ts'
import {
  BrowserProcessError,
  qualificationSignals,
  startHeadlessBrowser,
  withinSignal,
} from './browser-process.ts'

const required = (name) => {
  const value = process.env[name]
  if (!value) throw new Error(`${name} is required`)
  return value
}

const delay = (milliseconds) =>
  new Promise((resolveDelay) => setTimeout(resolveDelay, milliseconds))

function cdp(webSocketUrl, signal) {
  const socket = new WebSocket(webSocketUrl)
  let sequence = 0
  let rejectOpening
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
  const opened = withinSignal(
    new Promise((resolveOpen, rejectOpen) => {
      rejectOpening = rejectOpen
      socket.addEventListener('open', resolveOpen, { once: true })
      socket.addEventListener(
        'error',
        () => rejectOpen(new BrowserProcessError('debug_connection_failed')),
        { once: true },
      )
    }),
    signal,
  )
  const close = () => {
    rejectOpening(new BrowserProcessError('debug_connection_closed'))
    for (const waiter of pending.values())
      waiter.reject(new BrowserProcessError('debug_connection_closed'))
    pending.clear()
    signal.removeEventListener('abort', close)
    socket.close()
  }
  socket.addEventListener('close', close, { once: true })
  signal.addEventListener('abort', close, { once: true })
  return {
    async call(method, params = {}) {
      await opened
      if (signal.aborted) throw new BrowserProcessError('interrupted')
      if (socket.readyState !== WebSocket.OPEN)
        throw new BrowserProcessError('debug_connection_closed')
      const id = ++sequence
      const result = new Promise((resolveResult, rejectResult) =>
        pending.set(id, { resolve: resolveResult, reject: rejectResult }),
      )
      try {
        socket.send(JSON.stringify({ id, method, params }))
      } catch {
        pending.get(id).reject(new BrowserProcessError('debug_connection_closed'))
        pending.delete(id)
      }
      return result
    },
    on(method, handler) {
      handlers.set(method, handler)
    },
    close,
  }
}

async function evaluate(client, expression) {
  const result = await client.call('Runtime.evaluate', {
    expression,
    awaitPromise: true,
    returnByValue: true,
  })
  if (result.exceptionDetails)
    throw new Error(result.exceptionDetails.exception?.description ?? result.exceptionDetails.text)
  return result.result.value
}

async function waitFor(client, expression, description, timeoutMilliseconds = 30_000) {
  const deadline = Date.now() + timeoutMilliseconds
  while (Date.now() < deadline) {
    if (await evaluate(client, expression)) return
    await delay(100)
  }
  const body = await evaluate(
    client,
    `(document.querySelector('[data-ui~=notice--error]')?.textContent ?? '') + '\\n' + document.body.innerText.slice(0, 4096)`,
  )
  throw new Error(`timed out waiting for ${description}; visible page:\n${body}`)
}

function jsonLiteral(value) {
  return JSON.stringify(value).replaceAll('<', '\\u003c')
}

function exactPanelStatus(kicker, state) {
  return `(() => {
    const panel = [...document.querySelectorAll('article[data-ui~=panel]')].find((candidate) =>
      candidate.querySelector(':scope > [data-ui~=panel__heading] [data-ui~=kicker]')?.textContent.trim() === ${jsonLiteral(kicker)}
    );
    return panel?.querySelector(':scope > [data-ui~=panel__heading] > [data-ui~=status]')?.getAttribute('data-status') === ${jsonLiteral(state)};
  })()`
}

function exactPanelText(kicker, text) {
  return `(() => {
    const panel = [...document.querySelectorAll('article[data-ui~=panel]')].find((candidate) =>
      candidate.querySelector(':scope > [data-ui~=kicker], :scope > [data-ui~=panel__heading] [data-ui~=kicker]')?.textContent.trim() === ${jsonLiteral(kicker)}
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

function metricValue(label) {
  return `[...document.querySelectorAll('[data-ui~=metric]')].find(metric => metric.querySelector('dt')?.textContent.trim() === ${jsonLiteral(label)})?.querySelector('dd')?.textContent.trim()`
}

// Only the explicit synthetic fixture entry supplies this hook. The executable real journey
// uses the transparent Gateway proxy and never intercepts or replaces browser responses.
export async function runGatewayJourney({
  configureSyntheticBrowser,
}: { configureSyntheticBrowser?: (context: unknown) => Promise<void> } = {}) {
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
  const signals = qualificationSignals()
  let consoleServer
  let browserOwner
  let client
  let observer
  let evidence
  let failure
  const consoleMessages = []
  const authoringNetwork = []
  const requestKinds = new Map()
  const transportFailures = []
  const uploadRequests = new Map()
  const extraHeaders = new Map()
  try {
    consoleServer = await startGatewayConsoleServer({
      gatewayOrigin,
      managementGatewayOrigin,
      bundleRoot,
    })
    browserOwner = await startHeadlessBrowser({
      executable: browser,
      origin: consoleServer.origin,
      signal: signals.signal,
    })
    client = cdp(browserOwner.pageWebSocketUrl, signals.signal)
    await client.call('Runtime.enable')
    await client.call('Page.enable')
    await client.call('Log.enable')
    await client.call('Network.enable')
    client.on('Network.requestWillBeSent', ({ requestId, request }) => {
      const url = new URL(request.url)
      const path = url.pathname
      if (requestKinds.size < 256)
        requestKinds.set(requestId, { origin: url.origin, method: request.method })
      if (
        request.method === 'PUT' &&
        url.origin !== consoleServer.origin &&
        uploadRequests.size < 16
      )
        uploadRequests.set(requestId, {
          ambient: Object.keys(request.headers).some((name) =>
            /^(authorization|cookie)$/i.test(name),
          ),
        })
      if (
        path.startsWith('/v1/') &&
        (path.includes('/authoring') || !['GET', 'HEAD', 'OPTIONS'].includes(request.method))
      )
        authoringNetwork.push({ path, method: request.method })
    })
    client.on('Network.requestWillBeSentExtraInfo', ({ requestId, headers }) => {
      if (extraHeaders.size < 256)
        extraHeaders.set(
          requestId,
          Object.keys(headers).some((name) => /^(authorization|cookie)$/i.test(name)),
        )
    })
    client.on('Network.loadingFinished', ({ requestId }) => requestKinds.delete(requestId))
    client.on('Network.loadingFailed', ({ requestId, canceled, errorText, corsErrorStatus }) => {
      if (!canceled && transportFailures.length < 8)
        transportFailures.push({
          ...requestKinds.get(requestId),
          error: /^net::ERR_[A-Z_]+$/.test(errorText) ? errorText : 'browser_network_failure',
          cors_error: /^[A-Za-z]+$/.test(corsErrorStatus?.corsError ?? '')
            ? corsErrorStatus.corsError
            : null,
        })
      requestKinds.delete(requestId)
    })
    if (configureSyntheticBrowser)
      await configureSyntheticBrowser({
        client,
        consoleOrigin: consoleServer.origin,
        gatewayOrigin,
      })
    // Runtime console events are not request/response messages, so collect them through a second
    // small protocol connection dedicated to passive observation.
    observer = new WebSocket(browserOwner.pageWebSocketUrl)
    await withinSignal(
      new Promise((resolveOpen, rejectOpen) => {
        observer.addEventListener('open', resolveOpen, { once: true })
        observer.addEventListener(
          'error',
          () => rejectOpen(new BrowserProcessError('debug_connection_failed')),
          { once: true },
        )
      }),
      signals.signal,
    )
    observer.addEventListener('message', (event) => {
      const message = JSON.parse(event.data)
      if (message.method === 'Runtime.consoleAPICalled' || message.method === 'Log.entryAdded')
        consoleMessages.push(JSON.stringify(message.params))
    })
    observer.send(JSON.stringify({ id: 1, method: 'Runtime.enable' }))
    observer.send(JSON.stringify({ id: 2, method: 'Log.enable' }))

    await waitFor(
      client,
      `document.readyState === 'complete' && !!document.querySelector('[data-ui~=connection-form]')`,
      'Console application load',
    )
    await evaluate(client, setInput('input[type="url"]', consoleServer.origin))
    await evaluate(client, setInput('input[type="password"]', token))
    await evaluate(client, `document.querySelector('[data-ui~=connection-form]').requestSubmit()`)
    await waitFor(
      client,
      `!!document.querySelector('nav [aria-current=page]')`,
      'real Gateway readiness',
    )

    await waitFor(
      client,
      `[...document.querySelectorAll('button')].some(node => node.textContent.trim() === '创建智能体')`,
      'Agents page loaded',
    )
    if (authoringJourney) {
      const suffix = randomUUID().replaceAll('-', '').slice(0, 16)
      const name = `browser-full-plan-${suffix}`
      const displayName = `Browser Full Plan ${suffix}`
      const explicitInput = { message: `full-plan-echo-${suffix}` }
      await evaluate(client, clickText('创建智能体'))
      await waitFor(
        client,
        `document.body.innerText.includes('创建你的智能体')`,
        'new Agent editor',
      )
      await evaluate(client, setField('名称', name))
      await evaluate(client, setField('显示名称', displayName))
      await evaluate(client, setField('任务类型', 'deterministic'))
      await evaluate(client, clickText('高级 YAML'))
      await waitFor(client, `!!${fieldByLabel('输入 Schema JSON')}`, 'advanced schema editor')
      const originalSchema = await evaluate(client, `${fieldValue('输入 Schema JSON')}`)
      const beforeInvalidSource = authoringNetwork.length
      await evaluate(client, setField('输入 Schema JSON', '{ invalid local schema'))
      await evaluate(client, clickText('校验配置'))
      await waitFor(
        client,
        `!!document.querySelector('[data-ui~=notice--error]') && [...document.querySelectorAll('button')].some(button => button.textContent.trim() === '校验配置' && !button.disabled)`,
        'source-only rejection before dependency queries',
      )
      assert.deepEqual(
        authoringNetwork.slice(beforeInvalidSource),
        [],
        'invalid local schema must not query dependencies or create publication state',
      )
      await evaluate(client, setField('输入 Schema JSON', originalSchema))
      await evaluate(client, clickText('校验配置'))
      await waitFor(
        client,
        `[...document.querySelectorAll('button')].some(button => button.textContent.trim() === '编辑已校验 Plan' && !button.disabled)`,
        'shared Rust deterministic compilation',
      )
      const planDigest = await evaluate(client, metricValue('Plan 摘要'))
      assert.match(planDigest, /^sha256:[0-9a-f]{64}$/)
      // This UI action copies the actual shared compiler output into the editable source.
      await evaluate(client, clickText('编辑已校验 Plan'))
      await waitFor(
        client,
        `(${fieldByLabel('任务类型')})?.value === 'full_plan' && !!(${fieldByLabel('Plan JSON')})`,
        'Full Plan source editor',
      )
      await evaluate(client, clickText('高级 YAML'))
      const planSource = await evaluate(client, `${fieldValue('Plan JSON')}`)
      assert.equal(JSON.parse(planSource).plan_version, 6)
      const invalidPlan = JSON.parse(planSource)
      invalidPlan.entry_node_id = 'missing-local-node'
      const beforeInvalidPlan = authoringNetwork.length
      await evaluate(client, setField('Plan JSON', JSON.stringify(invalidPlan)))
      await evaluate(client, clickText('校验配置'))
      await waitFor(
        client,
        `!!document.querySelector('[data-ui~=notice--error]') && [...document.querySelectorAll('button')].some(button => button.textContent.trim() === '校验配置' && !button.disabled)`,
        'local Plan structure rejection before dependency queries',
      )
      assert.deepEqual(
        authoringNetwork.slice(beforeInvalidPlan),
        [],
        'invalid local Plan must not query dependencies or create publication state',
      )
      await evaluate(client, setField('Plan JSON', planSource))
      await evaluate(client, clickText('校验配置'))
      await waitFor(
        client,
        `${metricValue('Plan 摘要')} === ${jsonLiteral(planDigest)} && [...document.querySelectorAll('button')].some(button => button.textContent.trim() === '发布智能体' && !button.disabled)`,
        'Full Plan validation with the same exact Plan digest',
      )
      assert.equal(await evaluate(client, `${fieldValue('Plan JSON')}`), planSource)
      await evaluate(client, clickText('发布智能体'))
      await waitFor(
        client,
        `[...document.querySelectorAll('[data-ui~=agent-row]')].some(row => row.querySelector('strong')?.textContent === ${jsonLiteral(displayName)} && [...row.querySelectorAll('span')].some(span => span.textContent === ${jsonLiteral(name)}) && [...row.querySelectorAll('button')].some(button => button.textContent.trim() === '运行' && !button.disabled))`,
        'exact new Full Plan Agent publication in a populated tenant',
        60_000,
      )
      assert.ok(
        uploadRequests.size >= 2,
        'publication must use actual source and Plan object uploads',
      )
      for (const [requestId, request] of uploadRequests) {
        assert.equal(
          request.ambient,
          false,
          'signed object upload must not inherit Gateway Authorization or Cookie',
        )
        if (!configureSyntheticBrowser)
          assert.equal(
            extraHeaders.get(requestId),
            false,
            'actual browser transport must observe no ambient upload credentials',
          )
      }
      await evaluate(
        client,
        `(() => {
        const row = [...document.querySelectorAll('[data-ui~=agent-row]')].find(row => row.querySelector('strong')?.textContent === ${jsonLiteral(displayName)});
        const button = [...row.querySelectorAll('button')].find(button => button.textContent.trim() === '运行');
        if (!button || button.disabled) throw new Error('new Agent is not runnable');
        button.click();
      })()`,
      )
      await waitFor(
        client,
        `${exactPanelText('发起运行', `运行 ${displayName}`)} && [...document.querySelectorAll('button')].some((button) => button.textContent.trim() === '开始运行' && !button.disabled)`,
        'exact newly published Agent Run input',
      )
      await evaluate(client, setField('message（必填）', explicitInput.message))
      await evaluate(client, clickText('开始运行'))
      await waitFor(
        client,
        `/^run_/.test(${metricValue('运行 ID')} ?? '')`,
        'created Run exact identity',
      )
      const createdRunId = await evaluate(client, metricValue('运行 ID'))
      const resultRead = `(() => {
        const panel = [...document.querySelectorAll('article[data-ui~=panel]')].find(panel => panel.querySelector(':scope > [data-ui~=kicker]')?.textContent.trim() === '运行结果');
        const text = panel?.querySelector('details > pre')?.textContent;
        return text ? JSON.parse(text) : null;
      })()`
      const deadline = Date.now() + 60_000
      let output
      while (Date.now() < deadline) {
        await evaluate(client, submitSearchAndWaitForIdle('input[placeholder="run_…"]'))
        output = await evaluate(client, resultRead)
        if ((await evaluate(client, exactPanelStatus('运行详情', 'succeeded'))) && output) break
        await delay(250)
      }
      assert.equal(
        await evaluate(client, exactPanelStatus('运行详情', 'succeeded')),
        true,
        'new Full Plan Run must succeed',
      )
      assert.equal(await evaluate(client, metricValue('运行 ID')), createdRunId)
      assert.equal(output?.run_id, createdRunId)
      assert.deepEqual(
        output?.value,
        { kind: 'inline', value: explicitInput },
        'exact Full Plan result must echo the explicit submitted input',
      )
      await evaluate(
        client,
        `[...document.querySelectorAll('summary')].find(node => node.textContent === '高级运行诊断').click()`,
      )
      await evaluate(client, clickText('查看冻结源码'))
      const sourcePanel = `([...document.querySelectorAll('article[data-ui~=panel]')].find(panel => panel.querySelector(':scope > [data-ui~=kicker]')?.textContent.trim() === '运行源码'))`
      await waitFor(
        client,
        `${sourcePanel}?.innerText.includes('已验证的源码身份') && ${sourcePanel}?.querySelectorAll('li').length > 0`,
        'exact Run source map rebuilt from authorized published bytes',
      )
      await evaluate(
        client,
        `(() => { ${sourcePanel}.querySelector('details summary').click() })()`,
      )
      assert.equal(
        await evaluate(client, `${sourcePanel}.innerText.includes(${jsonLiteral(planDigest)})`),
        true,
        'frozen Run source must identify the compiled Plan',
      )
      const sourceLocations = await evaluate(
        client,
        `[...${sourcePanel}.querySelectorAll('li code:first-child')].map(node => node.textContent)`,
      )
      assert.ok(
        sourceLocations.every((location) => /^[^/][^:]*:[1-9][0-9]*:[1-9][0-9]*$/.test(location)),
        'source map locations must retain relative files and actual positive line/column',
      )
      authoringEvidence = {
        agent_name: name,
        execution_kind: 'full_plan',
        plan_version: 6,
        plan_digest: planDigest,
        run_id: createdRunId,
        explicit_inline_echo: true,
        frozen_source_verified: true,
        invalid_source_preflight_no_http: true,
        signed_upload_without_ambient_credentials: true,
      }
    }

    if (emptyRunId) {
      await evaluate(client, clickText('运行记录'))
      await waitFor(
        client,
        `!!document.querySelector('input[placeholder="run_…"]')`,
        'Runs page loaded',
      )
      await evaluate(client, setInput('input[placeholder="run_…"]', emptyRunId))
      await evaluate(client, `document.querySelector('[data-ui~=search]').requestSubmit()`)
      if (expectSlowLoading) {
        await waitFor(
          client,
          `document.querySelector('[data-ui~=search] button').disabled && document.querySelector('[data-ui~=search] button').textContent.includes('加载中')`,
          'bounded loading state while the authority is slow',
          500,
        )
      }
      await waitFor(
        client,
        `!!document.querySelector('[data-ui~=empty-state]') && document.body.innerText.includes('暂未收到运行事件')`,
        'explicit empty durable timeline',
      )
    }

    if (deterministicRunId) {
      await evaluate(client, clickText('运行记录'))
      await waitFor(
        client,
        `!!document.querySelector('input[placeholder="run_…"]')`,
        'Runs page loaded',
      )
      await evaluate(client, setInput('input[placeholder="run_…"]', deterministicRunId))
      await evaluate(client, submitSearchAndWaitForIdle('input[placeholder="run_…"]'))
      await waitFor(
        client,
        `${exactPanelStatus('运行详情', 'succeeded')} && ${exactPanelText('运行结果', 'hello')}`,
        'exact deterministic Run authority and Inline result',
      )
    }
    if (timerSignalRunId) {
      await evaluate(client, clickText('运行记录'))
      await waitFor(
        client,
        `!!document.querySelector('input[placeholder="run_…"]')`,
        'Runs page loaded',
      )
      await evaluate(client, setInput('input[placeholder="run_…"]', timerSignalRunId))
      await evaluate(client, submitSearchAndWaitForIdle('input[placeholder="run_…"]'))
      await waitFor(
        client,
        `${exactPanelStatus('运行详情', 'succeeded')} && ${exactPanelText('运行结果', 'resume after signal')}`,
        'exact Timer/Signal Run authority and Inline result',
      )
    }
    if (subagentRunId) {
      await evaluate(client, clickText('运行记录'))
      await waitFor(
        client,
        `!!document.querySelector('input[placeholder="run_…"]')`,
        'Runs page loaded',
      )
      await evaluate(client, setInput('input[placeholder="run_…"]', subagentRunId))
      await evaluate(client, submitSearchAndWaitForIdle('input[placeholder="run_…"]'))
      await waitFor(
        client,
        `${exactPanelStatus('运行详情', 'succeeded')} && document.body.innerText.includes('child.started') && document.body.innerText.includes('child.completed')`,
        'exact Subagent parent Run and durable child timeline',
      )
    }
    if (artifactId) {
      await evaluate(client, clickText('设置'))
      await waitFor(
        client,
        `[...document.querySelectorAll('button')].some(node => node.textContent === '文件')`,
        'Settings page loaded',
      )
      await evaluate(client, clickText('文件'))
      await evaluate(client, setInput('input[placeholder="art_…"]', artifactId))
      await evaluate(client, submitSearchAndWaitForIdle('input[placeholder="art_…"]'))
      await waitFor(
        client,
        `(() => {
          const input = document.querySelector('input[placeholder="art_…"]')
          const panel = input?.closest('[data-ui~=nested-panel]')
          const metrics = panel && [..[data-ui~=panel].querySelectorAll('[data-ui~=metric]')]
          const action = panel && [..[data-ui~=panel].querySelectorAll('button')]
            .find((button) => button.textContent.trim() === '授权下载')
          const exactMetric = (label, value) => metrics && metrics.some((metric) =>
            metric.querySelector('dt')?.textContent.trim() === label
              && metric.querySelector('dd')?.textContent.trim() === value)
          return input instanceof HTMLInputElement
            && input.value === ${jsonLiteral(artifactId)}
            && panel instanceof HTMLElement
            && exactMetric('文件 ID', ${jsonLiteral(artifactId)})
            && exactMetric('状态', 'ready')
            && action instanceof HTMLButtonElement
            && !action.disabled
        })()`,
        'exact Ready Artifact authority and controlled-download action',
      )
    }
    if (contextRunId) {
      await evaluate(client, clickText('运行记录'))
      await waitFor(
        client,
        `!!document.querySelector('input[placeholder="run_…"]')`,
        'Runs page loaded',
      )
      await evaluate(client, setInput('input[placeholder="run_…"]', contextRunId))
      await evaluate(client, submitSearchAndWaitForIdle('input[placeholder="run_…"]'))
      await waitFor(
        client,
        `${exactPanelStatus('运行详情', 'succeeded')} && ${exactPanelText('运行结果', 'local deterministic context item')} && ${exactPanelText('运行结果', 'observation_only')}`,
        'exact Context Run and citation projection',
      )
    }
    if (modelRunId) {
      await evaluate(client, clickText('运行记录'))
      await waitFor(
        client,
        `!!document.querySelector('input[placeholder="run_…"]')`,
        'Runs page loaded',
      )
      await evaluate(client, setInput('input[placeholder="run_…"]', modelRunId))
      await evaluate(client, submitSearchAndWaitForIdle('input[placeholder="run_…"]'))
      await waitFor(
        client,
        `${exactPanelStatus('运行详情', 'succeeded')} && ${exactPanelText('运行结果', 'deterministic streamed model response')}`,
        'exact Model Run and structured Inline result',
      )
    }
    if (capabilityRunId) {
      await evaluate(client, clickText('运行记录'))
      await waitFor(
        client,
        `!!document.querySelector('input[placeholder="run_…"]')`,
        'Runs page loaded',
      )
      await evaluate(client, setInput('input[placeholder="run_…"]', capabilityRunId))
      await evaluate(client, submitSearchAndWaitForIdle('input[placeholder="run_…"]'))
      await waitFor(
        client,
        `${exactPanelStatus('运行详情', 'succeeded')} && ${exactPanelText('运行结果', 'capability round trip')}`,
        'exact native-to-remote Capability Run and typed Inline result',
      )
    }
    if (mcpRunId) {
      await evaluate(client, clickText('运行记录'))
      await waitFor(
        client,
        `!!document.querySelector('input[placeholder="run_…"]')`,
        'Runs page loaded',
      )
      await evaluate(client, setInput('input[placeholder="run_…"]', mcpRunId))
      await evaluate(client, submitSearchAndWaitForIdle('input[placeholder="run_…"]'))
      await waitFor(
        client,
        `${exactPanelStatus('运行详情', 'succeeded')} && ${exactPanelText('运行结果', 'mcp round trip')}`,
        'exact MCP Capability Run and typed Inline result',
      )
    }
    if (sandboxRunId) {
      await evaluate(client, clickText('运行记录'))
      await waitFor(
        client,
        `!!document.querySelector('input[placeholder="run_…"]')`,
        'Runs page loaded',
      )
      await evaluate(client, setInput('input[placeholder="run_…"]', sandboxRunId))
      await evaluate(client, submitSearchAndWaitForIdle('input[placeholder="run_…"]'))
      await waitFor(
        client,
        `${exactPanelStatus('运行详情', 'succeeded')} && ${exactPanelText('运行结果', 'langgraph: bounded request')}`,
        'exact sandbox/framework Capability Run and bounded typed Inline result',
      )
    }

    await evaluate(client, clickText('运行记录'))
    await waitFor(
      client,
      `!!document.querySelector('input[placeholder="run_…"]')`,
      'Runs page loaded',
    )
    await evaluate(client, setInput('input[placeholder="run_…"]', runId))
    await evaluate(client, submitSearchAndWaitForIdle('input[placeholder="run_…"]'))
    await waitFor(
      client,
      `${exactPanelStatus('运行详情', 'waiting')} && document.body.textContent.includes(${jsonLiteral(taskId)})`,
      'waiting Run and linked Task',
    )
    const eventCanaryAbsent = await evaluate(
      client,
      `![
      'browser-token-must-not-render',
      'browser-prompt-must-not-render',
      'browser-secret-must-not-render'
    ].some((canary) => document.documentElement.innerHTML.includes(canary))`,
    )
    if (!eventCanaryAbsent) throw new Error('sensitive event canary reached the DOM')
    await evaluate(client, clickText('打开任务'))
    await waitFor(
      client,
      `!!document.querySelector('input[placeholder="int_… 或 apv_…"]')`,
      'Tasks page loaded',
    )
    await evaluate(client, submitSearchAndWaitForIdle('input[placeholder="int_… 或 apv_…"]'))
    await waitFor(
      client,
      `${exactPanelStatus('任务详情', 'pending')} && ${exactPanelText('任务详情', taskSafePromptKey)}`,
      'pending Task authority',
    )
    await waitFor(
      client,
      `!!document.querySelector('[data-ui~=task-schema-form] textarea')`,
      'authorized frozen Task form',
    )
    if (responseBody.value?.kind !== 'inline')
      throw new Error('Task form journey requires an inline response value')
    await evaluate(
      client,
      `(() => {
      const label = [...document.querySelectorAll('label')].find(node => node.querySelector('span')?.textContent === '回复数据分类');
      const select = label.querySelector('select');
      Object.getOwnPropertyDescriptor(HTMLSelectElement.prototype, 'value').set.call(select, ${jsonLiteral(responseBody.classification)});
      select.dispatchEvent(new Event('change', { bubbles: true }));
    })()`,
    )
    for (const [key, value] of Object.entries(responseBody.value.value)) {
      const path = '/' + key.replaceAll('~', '~0').replaceAll('/', '~1')
      await evaluate(
        client,
        `(() => {
        const field = [...document.querySelectorAll('[data-field-path]')].find(node => node.dataset.fieldPath === ${jsonLiteral(path)});
        const input = field?.querySelector('textarea, input:not([type="checkbox"])');
        if (!input || !['string', 'number'].includes(typeof ${jsonLiteral(value)})) throw new Error('Unsupported journey Task field');
        const prototype = input instanceof HTMLTextAreaElement ? HTMLTextAreaElement.prototype : HTMLInputElement.prototype;
        Object.getOwnPropertyDescriptor(prototype, 'value').set.call(input, ${jsonLiteral(String(value))});
        input.dispatchEvent(new Event('input', { bubbles: true }));
      })()`,
      )
    }
    await evaluate(client, clickText('提交回复'))
    await waitFor(
      client,
      `${exactPanelStatus('任务详情', 'responded')} && document.body.innerText.includes('操作已提交')`,
      'Task mutation authority result',
    )

    await evaluate(client, clickText('运行记录'))
    await waitFor(
      client,
      `!!document.querySelector('input[placeholder="run_…"]')`,
      'Runs page loaded',
    )
    await evaluate(client, setInput('input[placeholder="run_…"]', runId))
    const terminalDeadline = Date.now() + 60_000
    const terminalEvidence = `${exactPanelStatus('运行详情', 'succeeded')} && ${exactPanelText('运行结果', expectedResultText)}`
    let terminal = false
    while (Date.now() < terminalDeadline) {
      await evaluate(client, submitSearchAndWaitForIdle('input[placeholder="run_…"]'))
      terminal = await evaluate(client, terminalEvidence)
      if (terminal) break
      await delay(250)
    }
    if (!terminal) {
      const body = await evaluate(
        client,
        `(document.querySelector('[data-ui~=notice--error]')?.textContent ?? '') + '\\n' + document.body.innerText.slice(0, 4096)`,
      )
      throw new Error(`timed out waiting for terminal Run and safe result; visible page:\n${body}`)
    }
    if (
      await evaluate(
        client,
        `document.documentElement.innerHTML.includes('browser-tool-output-must-not-render')`,
      )
    ) {
      throw new Error('sensitive result canary reached the DOM')
    }

    await client.call('Emulation.setDeviceMetricsOverride', {
      width: 390,
      height: 844,
      deviceScaleFactor: 1,
      mobile: true,
    })
    const mobileAccessible = await evaluate(
      client,
      `({
      noHorizontalOverflow: document.documentElement.scrollWidth <= document.documentElement.clientWidth,
      navigationComplete: ['智能体', '运行记录', '待办任务', '设置'].every((label) =>
        [...document.querySelectorAll('nav button')].some((button) => button.textContent.includes(label))
      ),
      liveRegionPresent: !!document.querySelector('[aria-live]'),
      mainFocusable: document.querySelector('main').tabIndex === -1,
    })`,
    )
    if (!Object.values(mobileAccessible).every(Boolean))
      throw new Error(`mobile or ARIA qualification failed: ${JSON.stringify(mobileAccessible)}`)
    await client.call('Emulation.setDeviceMetricsOverride', {
      width: 1280,
      height: 900,
      deviceScaleFactor: 1,
      mobile: false,
    })

    await client.call('Page.reload', { ignoreCache: true })
    await waitFor(
      client,
      `document.readyState === 'complete' && !!document.querySelector('[data-ui~=connection-form]')`,
      'Console reload',
    )
    const cleared = await evaluate(
      client,
      `({
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
    })`,
    )
    if (!Object.values(cleared).every(Boolean))
      throw new Error(
        `reload did not clear browser-only authority state: ${JSON.stringify(cleared)}`,
      )
    await evaluate(client, setInput('input[type="password"]', token))
    await evaluate(client, `document.querySelector('[data-ui~=connection-form]').requestSubmit()`)
    await waitFor(
      client,
      `!!document.querySelector('nav [aria-current=page]')`,
      'Gateway readiness after reload',
    )
    await evaluate(client, clickText('运行记录'))
    await waitFor(
      client,
      `!!document.querySelector('input[placeholder="run_…"]')`,
      'Runs page loaded',
    )
    await evaluate(client, setInput('input[placeholder="run_…"]', runId))
    await evaluate(client, submitSearchAndWaitForIdle('input[placeholder="run_…"]'))
    await waitFor(client, terminalEvidence, 'authority Run and safe result re-read after reload')
    if (consoleMessages.some((message) => message.includes(token)))
      throw new Error('access token appeared in browser console output')
    observer.close()
    browserOwner.assertRunning()
    if (signals.signal.aborted) throw new BrowserProcessError('interrupted')
    evidence = {
      kind: configureSyntheticBrowser
        ? 'insight.console.synthetic-gateway-journey/v1'
        : 'insight.console.real-gateway-journey/v1',
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
        ...(authoringJourney
          ? [
              'agent_authoring_north_star',
              'full_plan_shared_compiler_publication',
              'exact_authoring_inline_echo',
            ]
          : []),
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
    }
  } catch (error) {
    failure = new Error(
      `${error instanceof Error ? (error.stack ?? error.message) : String(error)}\nSafe transport failures: ${JSON.stringify(transportFailures)}\nSafe browser process: ${JSON.stringify(browserOwner?.diagnostics() ?? null)}`,
    )
  } finally {
    const cleanupFailures = []
    // Ask Chromium to close its own helpers before the process owner verifies group teardown.
    if (client) {
      try {
        await withinSignal(client.call('Browser.close'), AbortSignal.timeout(2000))
      } catch {
        /* The debug socket may close before its reply. */
      }
    }
    try {
      observer?.close()
      client?.close()
    } catch {
      cleanupFailures.push('debug_connection_cleanup_failed')
    }
    try {
      await browserOwner?.close()
    } catch (error) {
      cleanupFailures.push(
        error instanceof BrowserProcessError ? error.reason : 'browser_cleanup_failed',
      )
    }
    try {
      if (consoleServer) await withinSignal(consoleServer.close(), AbortSignal.timeout(2000))
    } catch {
      cleanupFailures.push('console_server_cleanup_failed')
    }
    signals.dispose()
    if (cleanupFailures.length)
      failure = new Error(
        `${failure?.message ?? 'browser qualification cleanup failed'}\nSafe cleanup failures: ${JSON.stringify(cleanupFailures)}`,
      )
  }
  if (signals.signal.aborted && !failure) failure = new BrowserProcessError('interrupted')
  if (failure) throw failure
  process.stdout.write(`${JSON.stringify(evidence)}\n`)
}

if (process.argv[1] && import.meta.url === pathToFileURL(resolve(process.argv[1])).href)
  runGatewayJourney().catch((error) => {
    process.stderr.write(
      `${error instanceof Error ? (error.stack ?? error.message) : String(error)}\n`,
    )
    process.exit(process.exitCode ?? 1)
  })
