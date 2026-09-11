import { fieldElement as field, setField } from './browser-fields.ts'
// Explicit installed-platform exercise. Uses the real Console, its WASM compiler and public API.
// No response interception, provider fixture, SQL, automatic retry or Task mutation.
// Usage: node tests/model-chat-installation-journey.ts --endpoint <origin>
//   --session-file <0600 file> --agent-name <fresh name> --evidence-directory <new directory>
// Exit 0: typed result matched; 2: a real pending Task needs the user; otherwise failed.
// On failure, reconcile the recorded Agent/Run and publication journal before another invocation.
import { constants } from 'node:fs'
import { lstat, mkdir, open, realpath, rename } from 'node:fs/promises'
import { basename, dirname, join, resolve } from 'node:path'
import { pathToFileURL } from 'node:url'
import { randomUUID } from 'node:crypto'
import { setTimeout as delay } from 'node:timers/promises'
import {
  BrowserProcessError,
  qualificationSignals,
  startHeadlessBrowser,
  withinSignal,
} from './browser-process.ts'

const TOTAL_MS = 300_000
const PUBLICATION_KEY = 'insight.console.agent-publication.v3'
const uuid = '[0-9a-f]{8}-[0-9a-f]{4}-7[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}'
const resourceId = (prefix) => new RegExp(`^${prefix}_${uuid}$`)
const sha256 = /^sha256:[0-9a-f]{64}$/
const literal = (value) => JSON.stringify(value).replaceAll('<', '\\u003c')

class JourneyError extends Error {
  reason: string
  constructor(reason) {
    super(reason)
    this.reason = reason
  }
}
function requireCondition(condition, reason) {
  if (!condition) throw new JourneyError(reason)
}

export function parseArguments(argv) {
  const values = new Map()
  const allowed = ['--endpoint', '--session-file', '--agent-name', '--evidence-directory']
  for (let i = 0; i < argv.length; i += 2) {
    requireCondition(
      allowed.includes(argv[i]) &&
        !values.has(argv[i]) &&
        typeof argv[i + 1] === 'string' &&
        argv[i + 1].length > 0,
      'invalid_arguments',
    )
    values.set(argv[i], argv[i + 1])
  }
  requireCondition(values.size === allowed.length, 'invalid_arguments')
  let endpoint
  try {
    endpoint = new URL(values.get('--endpoint'))
  } catch {
    throw new JourneyError('invalid_endpoint')
  }
  requireCondition(
    ['https:', 'http:'].includes(endpoint.protocol) &&
      !endpoint.username &&
      !endpoint.password &&
      endpoint.pathname === '/' &&
      !endpoint.search &&
      !endpoint.hash,
    'invalid_endpoint',
  )
  // The bearer session is only sent over TLS, or to the explicit local installation.
  requireCondition(
    endpoint.protocol === 'https:' ||
      ['localhost', '127.0.0.1', '[::1]'].includes(endpoint.hostname),
    'insecure_endpoint',
  )
  const name = values.get('--agent-name')
  requireCondition(/^[a-z][a-z0-9-]{0,62}$/.test(name), 'invalid_agent_name')
  return {
    endpoint: endpoint.origin,
    sessionFile: resolve(values.get('--session-file')),
    agentName: name,
    evidenceDirectory: resolve(values.get('--evidence-directory')),
  }
}

export async function readPrivateSession(path) {
  const before = await lstat(path)
  const valid = (stat) =>
    stat.isFile() &&
    !stat.isSymbolicLink() &&
    stat.nlink === 1 &&
    (stat.mode & 0o777) === 0o600 &&
    stat.size >= 1 &&
    stat.size <= 65537
  requireCondition(valid(before), 'invalid_session_file')
  const file = await open(path, constants.O_RDONLY | constants.O_NOFOLLOW)
  try {
    const current = await file.stat()
    requireCondition(
      valid(current) && current.dev === before.dev && current.ino === before.ino,
      'invalid_session_file',
    )
    const bytes = Buffer.alloc(65538)
    try {
      let size = 0
      for (;;) {
        const read = await file.read(bytes, size, bytes.length - size, size)
        if (read.bytesRead === 0) break
        size += read.bytesRead
        requireCondition(size <= 65537, 'invalid_session_file')
      }
      requireCondition(size === current.size, 'invalid_session_file')
      const token = new TextDecoder('utf-8', { fatal: true })
        .decode(bytes.subarray(0, size))
        .replace(/\n$/, '')
      requireCondition(/^[\x21-\x7e]{1,65536}$/.test(token), 'invalid_session_file')
      return token
    } finally {
      bytes.fill(0)
    }
  } finally {
    await file.close()
  }
}

function cdp(url, signal) {
  const socket = new WebSocket(url)
  let sequence = 0
  const pending = new Map()
  const fail = () => {
    for (const waiter of pending.values())
      waiter.reject(new JourneyError('browser_connection_closed'))
    pending.clear()
  }
  socket.addEventListener('message', ({ data }) => {
    // Runtime.evaluate returns only bounded projections selected below, never credentials or raw errors.
    let message
    try {
      requireCondition(
        typeof data === 'string' && Buffer.byteLength(data) <= 131072,
        'browser_response_too_large',
      )
      message = JSON.parse(data)
    } catch {
      fail()
      socket.close()
      return
    }
    const waiter = pending.get(message.id)
    if (!waiter) return
    pending.delete(message.id)
    if (message.error) waiter.reject(new JourneyError('browser_command_failed'))
    else waiter.resolve(message.result)
  })
  socket.addEventListener('close', fail)
  socket.addEventListener('error', fail)
  const opened = withinSignal(
    new Promise((resolveOpen, rejectOpen) => {
      socket.addEventListener('open', resolveOpen, { once: true })
      socket.addEventListener(
        'error',
        () => rejectOpen(new JourneyError('browser_connection_failed')),
        { once: true },
      )
      socket.addEventListener(
        'close',
        () => rejectOpen(new JourneyError('browser_connection_closed')),
        { once: true },
      )
    }),
    signal,
  )
  const close = () => {
    fail()
    socket.close()
  }
  signal.addEventListener('abort', close, { once: true })
  return {
    async call(method, params = {}) {
      await opened
      signal.throwIfAborted()
      requireCondition(socket.readyState === WebSocket.OPEN, 'browser_connection_closed')
      const id = ++sequence
      const result = new Promise((resolveResult, reject) =>
        pending.set(id, { resolve: resolveResult, reject }),
      )
      try {
        socket.send(JSON.stringify({ id, method, params }))
      } catch {
        fail()
        throw new JourneyError('browser_connection_closed')
      }
      return withinSignal(result, signal)
    },
    close() {
      signal.removeEventListener('abort', close)
      close()
    },
  }
}

const panel = (kicker) =>
  `[...document.querySelectorAll('article[data-ui~=panel]')].find(item => item.querySelector(':scope > [data-ui~=kicker], :scope > [data-ui~=panel__heading] [data-ui~=kicker]')?.textContent.trim() === ${literal(kicker)})`
const metric = (label) =>
  `[...document.querySelectorAll('[data-ui~=metric]')].find(item => item.querySelector('dt')?.textContent.trim() === ${literal(label)})?.querySelector('dd')?.textContent.trim()`
function click(label, scope = 'document') {
  return `(() => { const item = [...(${scope}).querySelectorAll('button')].find(item => item.textContent.trim() === ${literal(label)});
    if (!item || item.disabled) throw 0; item.click(); })()`
}

export async function runInstalledModelChat(options) {
  const began = performance.now()
  const interrupted = qualificationSignals()
  const deadline = new AbortController()
  const timer = setTimeout(
    () => deadline.abort(new BrowserProcessError('journey_deadline_exceeded')),
    TOTAL_MS,
  )
  const signal = AbortSignal.any([interrupted.signal, deadline.signal])
  let token, directory, directoryIdentity, browser, client, failure
  const expectedAnswer = `model-chat-${randomUUID()}`
  const report = {
    trace_id: null as string | null,
    run_state: null as string | null,
    result_schema_digest: null as string | null,
    failure: null as string | null,
    elapsed_ms: 0,
    schema_version: 1,
    journey: 'installed-model-chat',
    status: 'running',
    stage: 'preflight',
    agent_name: options.agentName,
    agent_id: null,
    plan_digest: null,
    run_id: null,
    agent_deployment_id: null,
    task_id: null,
    expected_answer: expectedAnswer,
    result_matches: false,
    cleanup: 'not_started',
    // This observation is separate from retrieval, approval and provider capability qualification.
    retrieval_verified: false,
    human_approved: false,
    provider_conformance: false,
  }
  const persist = async (name, value) => {
    if (!directory) return
    const current = await lstat(directory)
    requireCondition(
      current.isDirectory() &&
        !current.isSymbolicLink() &&
        current.dev === directoryIdentity.dev &&
        current.ino === directoryIdentity.ino &&
        (current.mode & 0o777) === 0o700,
      'evidence_directory_changed',
    )
    const text = JSON.stringify(value, null, 2) + '\n'
    requireCondition(
      Buffer.byteLength(text) <= 65536 && (!token || !text.includes(token)),
      'unsafe_evidence',
    )
    const temporary = join(directory, `${name}.pending`)
    const file = await open(
      temporary,
      constants.O_WRONLY | constants.O_CREAT | constants.O_EXCL | constants.O_NOFOLLOW,
      0o600,
    )
    try {
      await file.writeFile(text)
      await file.sync()
    } finally {
      await file.close()
    }
    await rename(temporary, join(directory, name))
    const parent = await open(directory, constants.O_RDONLY)
    try {
      await parent.sync()
    } finally {
      await parent.close()
    }
  }
  const evaluate = async (expression) => {
    browser.assertRunning()
    const result = await client.call('Runtime.evaluate', {
      expression,
      awaitPromise: true,
      returnByValue: true,
    })
    requireCondition(!result.exceptionDetails, 'browser_expression_failed')
    return result.result.value
  }
  let previousRecovery
  const observe = async () => {
    const state = await evaluate(`(() => {
      const error = document.querySelector('[data-ui~=notice--error]');
      const raw = sessionStorage.getItem(${literal(PUBLICATION_KEY)});
      if (raw && new TextEncoder().encode(raw).length > 16384) throw 0;
      return { error:!!error, trace:error?.querySelector('code')?.textContent.trim().slice(0,128) ?? null, recovery:raw };
    })()`)
    if (state.recovery !== previousRecovery) {
      const recovery = state.recovery === null ? null : JSON.parse(state.recovery)
      requireCondition(
        recovery === null || recovery.schema_version === 4,
        'invalid_publication_recovery',
      )
      await persist('publication-recovery.json', recovery)
      previousRecovery = state.recovery
    }
    if (state.error) {
      const trace = /^trace ([0-9a-f]{32})$/.exec(state.trace ?? '')
      report.trace_id = trace?.[1] ?? null
      throw new JourneyError('console_reported_error')
    }
  }
  const waitFor = async (expression) => {
    for (;;) {
      signal.throwIfAborted()
      await observe()
      const value = await evaluate(expression)
      if (value) return value
      await delay(100, undefined, { signal })
    }
  }
  const stage = async (value) => {
    report.stage = value
    await persist('report.json', report)
  }
  try {
    token = await readPrivateSession(options.sessionFile)
    const parent = await realpath(dirname(options.evidenceDirectory))
    directory = join(parent, basename(options.evidenceDirectory))
    // A completed or unknown previous attempt must be reconciled, never silently retried.
    await mkdir(directory, { mode: 0o700 })
    directoryIdentity = await lstat(directory)
    await persist('report.json', report)
    browser = await startHeadlessBrowser({
      executable:
        process.env.INSIGHT_CONSOLE_BROWSER_BIN ??
        '/Applications/Google Chrome.app/Contents/MacOS/Google Chrome',
      origin: options.endpoint,
      signal,
    })
    client = cdp(browser.pageWebSocketUrl, signal)
    await client.call('Page.enable')
    await waitFor(`!!document.querySelector('[data-ui~=connection-form]')`)
    await stage('login')
    for (const [selector, value] of [
      ['input[type="url"]', options.endpoint],
      ['input[type="password"]', token],
    ]) {
      await evaluate(
        `(() => { const input=document.querySelector(${literal(selector)}); if(!input) throw 0; Object.getOwnPropertyDescriptor(HTMLInputElement.prototype,'value').set.call(input,${literal(value)}); input.dispatchEvent(new Event('input',{bubbles:true})); })()`,
      )
    }
    await evaluate(`document.querySelector('[data-ui~=connection-form]').requestSubmit()`)
    await waitFor(`!!document.querySelector('nav [aria-current=page]')`)
    await evaluate(
      `(() => { const item=[...document.querySelectorAll('nav button')].find(item=>item.textContent.trim().endsWith('智能体')); if(!item || item.disabled) throw 0; item.click(); })()`,
    )
    await waitFor(
      `[...document.querySelectorAll('button')].some(item => item.textContent.trim()==='创建智能体' && !item.disabled)`,
    )
    // Read every bounded UI page before writing. Never treat an incomplete first page as absence.
    for (let page = 0; ; page++) {
      requireCondition(
        !(await evaluate(
          `[...document.querySelectorAll('[data-ui~=agent-row]')].some(item => item.querySelector('div > span')?.textContent.trim()===${literal(options.agentName)})`,
        )),
        'agent_name_already_present',
      )
      if (
        !(await evaluate(
          `[...document.querySelectorAll('button')].some(item => item.textContent.trim()==='下一页')`,
        ))
      )
        break
      requireCondition(page < 15, 'agent_list_exceeded_bound')
      // Wait for this specific React async handler to cross busy -> idle, even for a fast page.
      await evaluate(`new Promise((done, failed) => {
        const button=[...document.querySelectorAll('button')].find(item=>item.textContent.trim()==='创建智能体');
        const next=[...document.querySelectorAll('button')].find(item=>item.textContent.trim()==='下一页');
        if(!button || !next || next.disabled) throw 0;
        let busy=false;
        const observer=new MutationObserver(records=>{
          if(button.disabled || records.some(row=>row.attributeName==='disabled' && row.oldValue===null)) busy=true;
          if(busy && !button.disabled){clearTimeout(timer);observer.disconnect();done(true);}
        });
        observer.observe(button,{attributes:true,attributeFilter:['disabled'],attributeOldValue:true});
        const timer=setTimeout(()=>{observer.disconnect();failed(0);},10000);
        next.click();
      })`)
      await observe()
    }
    await evaluate(click('创建智能体'))
    await stage('compile')
    await evaluate(setField('名称', options.agentName))
    await evaluate(setField('显示名称', options.agentName))
    await evaluate(setField('任务类型', 'model_chat'))
    await evaluate(click('下一步'))
    await waitFor(
      `Array.from((${field('模型')})?.options ?? []).some(item => item.value==='project/default')`,
    )
    await evaluate(setField('模型', 'project/default'))
    await evaluate(setField('数据分类', 'internal'))
    await evaluate(setField('超时秒数', '120'))
    await evaluate(setField('发布环境', ''))
    await evaluate(
      setField(
        '任务指令',
        'Read the message from the current user input. Return exactly one JSON object with the single key answer. Set answer to the message unchanged. Do not add Markdown or any other text.',
      ),
    )
    await evaluate(click('下一步'))
    await evaluate(click('下一步'))
    await evaluate(click('校验配置', 'document.querySelector("[data-ui~=editor]")'))
    await waitFor(
      `document.body.innerText.includes(${literal(`${options.agentName} 校验通过，依赖已解析。`)})`,
    )
    report.plan_digest = await evaluate(metric('Plan 摘要'))
    requireCondition(sha256.test(report.plan_digest), 'missing_compiler_identity')
    await stage('publish')
    await evaluate(click('发布智能体', 'document.querySelector("[data-ui~=editor]")'))
    await waitFor(
      `(() => { const row=[...document.querySelectorAll('[data-ui~=agent-row]')].find(item => item.querySelector('div > span')?.textContent.trim()===${literal(options.agentName)}); return row?.querySelector('[data-ui~=status]')?.getAttribute('data-status')==='ready'; })()`,
    )
    report.agent_id = await evaluate(
      `[...document.querySelectorAll('[data-agent-id]')].find(node => node.querySelector('div > span')?.textContent.trim() === ${literal(options.agentName)})?.dataset.agentId`,
    )
    requireCondition(resourceId('agt').test(report.agent_id), 'missing_agent_identity')
    await stage('create_run')
    await evaluate(
      click(
        '运行',
        `[...document.querySelectorAll('[data-ui~=agent-row]')].find(item => item.querySelector('div > span')?.textContent.trim()===${literal(options.agentName)})`,
      ),
    )
    await waitFor(
      `[...document.querySelectorAll('button')].some(item => item.textContent.trim()==='开始运行' && !item.disabled)`,
    )
    await evaluate(setField('message（必填）', expectedAnswer))
    await evaluate(click('开始运行'))
    await waitFor(`!!${panel('运行详情')}?.querySelector('[data-ui~=status]')`)
    report.run_id = await evaluate(metric('运行 ID'))
    report.agent_deployment_id = await evaluate(metric('智能体部署'))
    requireCondition(
      resourceId('run').test(report.run_id) && resourceId('adep').test(report.agent_deployment_id),
      'missing_run_identity',
    )
    await stage('observe_run')
    let nextRefresh = performance.now() + 2000
    let previousState
    for (;;) {
      await observe()
      const current = await evaluate(
        `${panel('运行详情')}?.querySelector('[data-ui~=status]')?.getAttribute('data-status')`,
      )
      requireCondition(
        [
          'queued',
          'running',
          'waiting',
          'cancelling',
          'succeeded',
          'failed',
          'cancelled',
          'timed_out',
        ].includes(current),
        'unknown_run_state',
      )
      report.run_state = current
      if (current === 'succeeded') {
        const resultText = await waitFor(
          `(() => { const text=${panel('运行结果')}?.querySelector('details > pre')?.textContent; if(text && new TextEncoder().encode(text).length>16384) throw 0; return text; })()`,
        )
        const result = JSON.parse(resultText)
        requireCondition(
          result.value?.kind === 'inline' &&
            result.value.value?.answer === expectedAnswer &&
            Object.keys(result.value.value).length === 1 &&
            sha256.test(result.schema_digest),
          'typed_result_mismatch',
        )
        report.result_schema_digest = result.schema_digest
        report.result_matches = true
        report.status = 'passed'
        break
      }
      if (['failed', 'cancelled', 'timed_out'].includes(current))
        throw new JourneyError('run_terminal_failure')
      if (
        current === 'waiting' &&
        (await evaluate(`document.querySelectorAll('[data-ui~=linked-tasks] button').length>0`))
      ) {
        // Read the real Task only. Never select approve/reject/submit/cancel.
        await evaluate(click('打开任务', 'document.querySelector("[data-ui~=linked-tasks]")'))
        await waitFor(`!!${panel('任务详情')}?.querySelector('[data-ui~=status]')`)
        report.task_id = await evaluate(metric('任务 ID'))
        requireCondition(
          new RegExp(`^(int|apv)_${uuid}$`).test(report.task_id),
          'invalid_task_identity',
        )
        requireCondition(
          await evaluate(
            `${panel('任务详情')}?.querySelector('[data-ui~=status]')?.getAttribute('data-status')==='pending'`,
          ),
          'task_not_pending',
        )
        report.status = 'awaiting_human'
        break
      }
      // Current-state refresh is read-only and does not restart or re-submit a Run.
      if (performance.now() >= nextRefresh) {
        const canRefresh = await evaluate(
          `[...(${panel('运行详情')}).querySelectorAll('button')].some(item=>item.textContent.trim()==='刷新' && !item.disabled)`,
        )
        if (canRefresh) await evaluate(click('刷新', panel('运行详情')))
        nextRefresh = performance.now() + 2000
      }
      if (current !== previousState) {
        await persist('report.json', report)
        previousState = current
      }
      await delay(200, undefined, { signal })
    }
  } catch (error) {
    failure = error
    report.status = 'failed'
    report.failure =
      error instanceof JourneyError || error instanceof BrowserProcessError
        ? error.reason
        : 'journey_failed'
  } finally {
    clearTimeout(timer)
    client?.close()
    try {
      await browser?.close()
      report.cleanup = 'completed'
    } catch {
      report.cleanup = 'failed'
      report.status = 'failed'
      report.failure ??= 'browser_cleanup_failed'
      failure ??= new JourneyError('browser_cleanup_failed')
    }
    interrupted.dispose()
    report.elapsed_ms = Math.round(performance.now() - began)
    if (directoryIdentity) await persist('report.json', report)
    token = undefined
  }
  if (failure) return { ...report, status: 'failed' }
  return report
}

if (process.argv[1] && import.meta.url === pathToFileURL(resolve(process.argv[1])).href) {
  try {
    const report = await runInstalledModelChat(parseArguments(process.argv.slice(2)))
    console.log(JSON.stringify(report))
    if (report.status !== 'passed') process.exitCode ||= report.status === 'awaiting_human' ? 2 : 1
  } catch {
    console.error(
      JSON.stringify({
        schema_version: 1,
        journey: 'installed-model-chat',
        status: 'failed',
        reason: 'invalid_or_unavailable_local_input',
      }),
    )
    process.exitCode ||= 1
  }
}
