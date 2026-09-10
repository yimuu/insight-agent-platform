import { setField } from './browser-fields.ts'
import type { CdpResult } from './fixtures/types.ts'
import { tcpPort } from './fixtures/types.ts'
import { spawn } from 'node:child_process'
import { existsSync, mkdtempSync, readFileSync, rmSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { startGatewayConsoleServer } from '../server/native.ts'

export const browserBinary =
  process.env.INSIGHT_CONSOLE_BROWSER_BIN ??
  '/Applications/Google Chrome.app/Contents/MacOS/Google Chrome'
export const explicitBrowserBundle = process.env.INSIGHT_CONSOLE_BUNDLE_ROOT
export const browserAvailable = Boolean(explicitBrowserBundle && existsSync(browserBinary))
const delay = (milliseconds) => new Promise<void>((resolve) => setTimeout(resolve, milliseconds))

export async function eventually(check, label) {
  const deadline = Date.now() + 8_000
  while (Date.now() < deadline) {
    if (await check()) return
    await delay(30)
  }
  throw new Error(`Timed out: ${label}`)
}

export async function withConsoleBrowser(
  api,
  run,
  options: { bundleRoot?: string; readyExpression?: string } = {},
) {
  await new Promise<void>((resolve) => api.listen(0, '127.0.0.1', resolve))
  const consoleServer = await startGatewayConsoleServer({
    gatewayOrigin: `http://127.0.0.1:${tcpPort(api)}`,
    managementGatewayOrigin: `http://127.0.0.1:${tcpPort(api)}`,
    bundleRoot: options.bundleRoot ?? explicitBrowserBundle,
  })
  const profile = mkdtempSync(join(tmpdir(), 'insight-console-editor-browser-'))
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
  let socket
  try {
    const activePort = join(profile, 'DevToolsActivePort')
    let port
    await eventually(() => {
      if (!existsSync(activePort)) return false
      const lines = readFileSync(activePort, 'utf8').split('\n')
      if (lines.length < 2 || !/^[1-9][0-9]{0,4}$/.test(lines[0]) || Number(lines[0]) > 65535)
        return false
      port = lines[0]
      return true
    }, 'Chrome debugging port')
    let page
    await eventually(async () => {
      try {
        page = (await (await fetch(`http://127.0.0.1:${port}/json`)).json()).find(
          (target) => target.type === 'page',
        )
        return page
      } catch {
        return false
      }
    }, 'Chrome page')
    socket = new WebSocket(page.webSocketDebuggerUrl)
    await new Promise((resolve, reject) => {
      socket.addEventListener('open', resolve, { once: true })
      socket.addEventListener('error', reject, { once: true })
    })
    let nextId = 0
    const pending = new Map()
    socket.addEventListener('message', ({ data }) => {
      const message = JSON.parse(data)
      const request = pending.get(message.id)
      if (!request) return
      pending.delete(message.id)
      if (message.error) request.reject(new Error(message.error.message))
      else request.resolve(message.result)
    })
    const call = (method, params = {}) => {
      const id = ++nextId
      const result = new Promise<CdpResult>((resolve, reject) =>
        pending.set(id, { resolve, reject }),
      )
      socket.send(JSON.stringify({ id, method, params }))
      return result
    }
    await call('Runtime.enable')
    const evaluate = async (expression) => {
      const output = await call('Runtime.evaluate', {
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
    const driver = {
      call,
      evaluate,
      wait: async (expression, label) => {
        try {
          await eventually(() => evaluate(expression), label)
        } catch (error) {
          throw new Error(
            `${error.message}\n${String(await evaluate(`(document.querySelector('[data-ui~=notice--error]')?.textContent ?? '') + '\\n' + document.body.innerText`)).slice(0, 2600)}`,
          )
        }
      },
      click: (text) =>
        evaluate(
          `[...document.querySelectorAll('button')].find(button => button.textContent.trim() === ${JSON.stringify(text)}).click()`,
        ),
      async connect(token: string) {
        const previous = await evaluate(
          `document.querySelector('nav [aria-current="page"]')?.textContent`,
        )
        if (!(await evaluate(`!!document.querySelector('input[type="password"]')`))) {
          await evaluate(
            `[...document.querySelectorAll('summary')].find(node => node.textContent === '工作空间会话').click()`,
          )
          await driver.click('更换连接')
        }
        await driver.wait(`!!document.querySelector('input[type="password"]')`, 'connection page')
        await driver.field('访问令牌', token)
        await driver.click('进入工作空间')
        await driver.wait(
          `!!document.querySelector('nav [aria-current="page"]')`,
          'authenticated workspace',
        )
        if (previous)
          await evaluate(
            `[...document.querySelectorAll('nav button')].find(node => node.textContent === ${JSON.stringify(previous)})?.click()`,
          )
        await driver.wait(`!document.body.innerText.includes('正在加载页面…')`, 'feature page')
      },
      async field(label: string, value: string) {
        await driver.wait(
          `!!([...document.querySelectorAll('label')].find(node => node.querySelector('span')?.textContent === ${JSON.stringify(label)})?.querySelector('input,textarea,select') ?? [...document.querySelectorAll('input[aria-label],select[aria-label],textarea[aria-label]')].find(node => node.getAttribute('aria-label') === ${JSON.stringify(label)}) ?? [...document.querySelectorAll('[data-code-editor]')].find(node => node.dataset.codeEditor === ${JSON.stringify(label)})?.querySelector('.cm-content'))`,
          'field ' + label,
        )
        const editorSelector = `[data-code-editor=${JSON.stringify(label)}] .cm-content`
        if (await evaluate(`!!document.querySelector(${JSON.stringify(editorSelector)})`)) {
          await evaluate(setField(label, value))
          return
        }
        await evaluate(`(() => {
        const container = [...document.querySelectorAll('label')].find(node => node.querySelector('span')?.textContent === ${JSON.stringify(label)});
        const input = container?.querySelector('input, textarea, select') ?? [...document.querySelectorAll('[aria-label]')].find(node => node.getAttribute('aria-label') === ${JSON.stringify(label)});
        const prototype = input instanceof HTMLTextAreaElement ? HTMLTextAreaElement.prototype : input instanceof HTMLSelectElement ? HTMLSelectElement.prototype : HTMLInputElement.prototype;
        Object.getOwnPropertyDescriptor(prototype, 'value').set.call(input, ${JSON.stringify(value)});
        input.dispatchEvent(new Event(input instanceof HTMLSelectElement ? 'change' : 'input', { bubbles: true }));
      })()`)
      },
      async upload(selector, path) {
        const document = await call('DOM.getDocument')
        const { nodeId } = await call('DOM.querySelector', {
          nodeId: document.root.nodeId,
          selector,
        })
        await call('DOM.setFileInputFiles', { nodeId, files: [path] })
      },
    }
    await driver.wait(
      options.readyExpression ?? `!!document.querySelector('input[type="password"]')`,
      'Console mount',
    )
    await run(driver)
  } finally {
    socket?.close()
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
}
