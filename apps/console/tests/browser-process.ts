import type { SpawnOptions } from 'node:child_process'
// Local qualification process ownership. This is not part of the Console bundle.
import { spawn } from 'node:child_process'
import { constants } from 'node:fs'
import { lstat, mkdtemp, open, rm } from 'node:fs/promises'
import { createHash } from 'node:crypto'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { performance } from 'node:perf_hooks'

const maximumStderrBytes = 8192
const maximumEndpointBytes = 256
const maximumResponseBytes = 64 * 1024
const safeErrnos = new Set(['ENOENT', 'EACCES', 'EPERM', 'ENOEXEC', 'EMFILE', 'ENFILE', 'EIO'])
const safeSignals = new Set([
  'SIGTERM',
  'SIGKILL',
  'SIGINT',
  'SIGABRT',
  'SIGSEGV',
  'SIGBUS',
  'SIGILL',
  'SIGTRAP',
])

export class BrowserProcessError extends Error {
  reason: string
  constructor(reason: string, diagnostics: Record<string, unknown> = {}) {
    super(`browser qualification: ${JSON.stringify({ reason, ...diagnostics })}`)
    this.name = 'BrowserProcessError'
    this.reason = reason
  }
}

function interrupted() {
  return new BrowserProcessError('interrupted')
}

export function withinSignal<T>(promise: PromiseLike<T>, signal: AbortSignal): Promise<T> {
  return new Promise((resolve, reject) => {
    const abort = () =>
      reject(signal.reason instanceof BrowserProcessError ? signal.reason : interrupted())
    const finish = (callback) => (value) => {
      signal.removeEventListener('abort', abort)
      callback(value)
    }
    Promise.resolve(promise).then(finish(resolve), finish(reject))
    if (signal.aborted) abort()
    else signal.addEventListener('abort', abort, { once: true })
  })
}

function pause(milliseconds, signal) {
  return new Promise<void>((resolve, reject) => {
    const finish = () => {
      signal.removeEventListener('abort', abort)
      resolve()
    }
    const timer = setTimeout(finish, milliseconds)
    const abort = () => {
      clearTimeout(timer)
      reject(signal.reason instanceof BrowserProcessError ? signal.reason : interrupted())
    }
    if (signal.aborted) abort()
    else signal.addEventListener('abort', abort, { once: true })
  })
}

async function completedWithin(completed, milliseconds) {
  let timer
  try {
    return await Promise.race([
      completed,
      new Promise((resolve) => {
        timer = setTimeout(() => resolve(null), milliseconds)
      }),
    ])
  } finally {
    clearTimeout(timer)
  }
}

export function spawnFixtureChild(executable: string, args: string[], options: SpawnOptions = {}) {
  const child = spawn(executable, args, {
    ...options,
    stdio: options.stdio ?? ['ignore', 'pipe', 'pipe'],
  })
  const groupId = options.detached ? child.pid : undefined
  let groupGone = groupId === undefined
  const stopped = new AbortController()
  let closed = false
  let spawnError
  let stderrTail = Buffer.alloc(0)
  let stderrBytes = 0
  let closing
  const diagnostics = () => ({
    exit_code: Number.isInteger(child.exitCode) ? child.exitCode : null,
    signal:
      child.signalCode === null
        ? null
        : safeSignals.has(child.signalCode)
          ? child.signalCode
          : 'Other',
    spawn_error: spawnError ? (safeErrnos.has(spawnError.code) ? spawnError.code : 'Other') : null,
    stderr_bytes: stderrBytes,
    stderr_tail_sha256: createHash('sha256').update(stderrTail).digest('hex'),
    stderr_signals: [
      ['debug_bind_failed', /bind\(\) failed|Cannot start http server for devtools/],
      ['profile_locked', /SingletonLock|ProcessSingleton|profile.*already in use/i],
      ['sandbox_failed', /No usable sandbox|Running as root without --no-sandbox/],
      ['debug_listener_started', /DevTools listening on /],
    ]
      .filter(([, pattern]) => (pattern as RegExp).test(stderrTail.toString('utf8')))
      .map(([name]) => name),
  })
  const fail = () =>
    stopped.abort(
      new BrowserProcessError(spawnError ? 'spawn_failed' : 'child_exited', diagnostics()),
    )
  child.stderr?.on('data', (chunk) => {
    stderrBytes = Math.min(Number.MAX_SAFE_INTEGER, stderrBytes + chunk.length)
    stderrTail = Buffer.concat([stderrTail, chunk]).subarray(-maximumStderrBytes)
  })
  const completed = new Promise<{ code: number | null; signal: NodeJS.Signals | null }>(
    (resolve) => {
      child.once('error', (error) => {
        spawnError = error
        fail()
      })
      child.once('exit', fail)
      child.once('close', (code, signal) => {
        closed = true
        resolve({ code, signal })
      })
    },
  )
  const signalChild = (signal) => {
    if (
      groupId !== undefined
        ? groupGone
        : closed || child.exitCode !== null || child.signalCode !== null || !child.pid
    )
      return
    try {
      if (groupId !== undefined) process.kill(-groupId, signal)
      else child.kill(signal)
    } catch (error) {
      if (error.code !== 'ESRCH') throw new BrowserProcessError('termination_failed', diagnostics())
      if (groupId !== undefined) groupGone = true
    }
  }
  const completedGroupWithin = async (milliseconds) => {
    if (groupId === undefined) return await completedWithin(completed, milliseconds)
    const deadline = performance.now() + milliseconds
    for (;;) {
      if (!groupGone) {
        try {
          process.kill(-groupId, 0)
        } catch (error) {
          if (error.code !== 'ESRCH')
            throw new BrowserProcessError('termination_failed', diagnostics())
          // Never operate on this numeric PGID again, even if the OS later reuses it.
          groupGone = true
        }
      }
      if (groupGone && closed) return await completed
      const remaining = deadline - performance.now()
      if (remaining <= 0) return null
      await new Promise((resolve) => setTimeout(resolve, Math.min(20, remaining)))
    }
  }
  return {
    child,
    completed,
    stopped: stopped.signal,
    diagnostics,
    assertRunning() {
      if (stopped.signal.aborted)
        throw new BrowserProcessError(spawnError ? 'spawn_failed' : 'child_exited', diagnostics())
    },
    terminate({ graceMs = 5000, killMs = 2000 } = {}) {
      closing ??= (async () => {
        signalChild('SIGTERM')
        let status = await completedGroupWithin(graceMs)
        if (status === null) {
          signalChild('SIGKILL')
          status = await completedGroupWithin(killMs)
        }
        if (status === null) throw new BrowserProcessError('cleanup_timeout', diagnostics())
        return status
      })()
      return closing
    },
  }
}

export async function waitForFixtureChild(
  owner: ReturnType<typeof spawnFixtureChild>,
  timeoutMs: number,
  signal?: AbortSignal,
) {
  const controller = new AbortController()
  const timer = setTimeout(
    () => controller.abort(new BrowserProcessError('journey_timeout')),
    timeoutMs,
  )
  try {
    return await withinSignal(
      owner.completed,
      signal ? AbortSignal.any([signal, controller.signal]) : controller.signal,
    )
  } finally {
    clearTimeout(timer)
  }
}

export function qualificationSignals() {
  const controller = new AbortController()
  const onTerm = () => {
    if (!controller.signal.aborted) {
      process.exitCode = 143
      controller.abort(interrupted())
    }
  }
  const onInt = () => {
    if (!controller.signal.aborted) {
      process.exitCode = 130
      controller.abort(interrupted())
    }
  }
  process.on('SIGTERM', onTerm)
  process.on('SIGINT', onInt)
  return {
    signal: controller.signal,
    dispose() {
      process.removeListener('SIGTERM', onTerm)
      process.removeListener('SIGINT', onInt)
    },
  }
}

async function readEndpoint(profile) {
  const path = join(profile, 'DevToolsActivePort')
  let before
  try {
    before = await lstat(path)
  } catch (error) {
    if (error.code === 'ENOENT') return null
    throw error
  }
  if (
    !before.isFile() ||
    before.isSymbolicLink() ||
    before.nlink !== 1 ||
    before.size > maximumEndpointBytes
  )
    throw new BrowserProcessError('invalid_endpoint_file')
  const file = await open(path, constants.O_RDONLY | constants.O_NOFOLLOW | constants.O_NONBLOCK)
  try {
    const opened = await file.stat()
    if (
      !opened.isFile() ||
      opened.nlink !== 1 ||
      opened.dev !== before.dev ||
      opened.ino !== before.ino ||
      opened.size > maximumEndpointBytes
    )
      throw new BrowserProcessError('invalid_endpoint_file')
    const buffer = Buffer.alloc(maximumEndpointBytes + 1)
    const { bytesRead } = await file.read(buffer, 0, buffer.length, 0)
    const after = await file.stat()
    if (after.nlink !== 1 || bytesRead > maximumEndpointBytes || after.size > maximumEndpointBytes)
      throw new BrowserProcessError('invalid_endpoint_file')
    if (opened.size !== after.size || bytesRead !== after.size || bytesRead === 0) return null
    let text
    try {
      text = new TextDecoder('utf-8', { fatal: true }).decode(buffer.subarray(0, bytesRead))
    } catch {
      throw new BrowserProcessError('invalid_endpoint_file')
    }
    const match = /^([1-9][0-9]{0,4})\n(\/devtools\/browser\/[A-Za-z0-9-]{1,64})\n?$/.exec(text)
    if (!match || Number(match[1]) > 65535) throw new BrowserProcessError('invalid_endpoint_file')
    return {
      origin: `http://127.0.0.1:${match[1]}`,
      browserSocket: `ws://127.0.0.1:${match[1]}${match[2]}`,
    }
  } finally {
    await file.close()
  }
}

async function readJson(url, signal) {
  let response
  try {
    response = await fetch(url, { signal, redirect: 'error', credentials: 'omit' })
  } catch {
    if (signal.aborted) throw signal.reason
    return undefined
  }
  let reader: ReadableStreamDefaultReader<Uint8Array>
  try {
    if (!response.ok) return undefined
    if (
      response.headers.get('content-encoding') &&
      response.headers.get('content-encoding') !== 'identity'
    )
      throw new BrowserProcessError('invalid_debug_response')
    const declared = response.headers.get('content-length')
    if (
      declared !== null &&
      (!/^(0|[1-9][0-9]*)$/.test(declared) || Number(declared) > maximumResponseBytes)
    )
      throw new BrowserProcessError('invalid_debug_response')
    reader = response.body.getReader()
    let length = 0
    const chunks = []
    for (;;) {
      const { value, done } = await withinSignal(reader.read(), signal)
      if (done) break
      length += value.byteLength
      if (length > maximumResponseBytes) throw new BrowserProcessError('invalid_debug_response')
      chunks.push(value)
    }
    try {
      return JSON.parse(new TextDecoder('utf-8', { fatal: true }).decode(Buffer.concat(chunks)))
    } catch {
      throw new BrowserProcessError('invalid_debug_response')
    }
  } finally {
    if (reader) {
      await withinSignal(reader.cancel(), signal).catch(() => {})
      reader.releaseLock()
    } else if (response.body) await withinSignal(response.body.cancel(), signal).catch(() => {})
  }
}

function pageSocket(targets, endpoint, origin) {
  if (!Array.isArray(targets) || targets.length > 16)
    throw new BrowserProcessError('invalid_debug_response')
  const matches = targets.filter(
    (target) => target && target.type === 'page' && target.url === `${origin}/`,
  )
  if (matches.length === 0) return null
  if (matches.length !== 1) throw new BrowserProcessError('invalid_debug_response')
  const page = matches[0]
  if (
    typeof page.id !== 'string' ||
    !/^[A-Za-z0-9-]{1,64}$/.test(page.id) ||
    page.webSocketDebuggerUrl !==
      `${endpoint.origin.replace('http:', 'ws:')}/devtools/page/${page.id}`
  )
    throw new BrowserProcessError('invalid_debug_response')
  return page.webSocketDebuggerUrl
}

export async function startHeadlessBrowser({
  executable,
  origin,
  timeoutMs = 60_000,
  signal: externalSignal,
  graceMs = 5000,
  killMs = 2000,
}: {
  executable: string
  origin: string
  timeoutMs?: number
  signal?: AbortSignal
  graceMs?: number
  killMs?: number
}) {
  const expectedOrigin = new URL(origin)
  if (
    expectedOrigin.origin !== origin ||
    expectedOrigin.protocol !== 'http:' ||
    expectedOrigin.hostname !== '127.0.0.1' ||
    !expectedOrigin.port
  )
    throw new BrowserProcessError('invalid_console_origin')
  const began = performance.now()
  const controller = new AbortController()
  const timer = setTimeout(
    () => controller.abort(new BrowserProcessError('startup_timeout')),
    timeoutMs,
  )
  const checkDeadline = () => {
    if (performance.now() - began >= timeoutMs)
      controller.abort(new BrowserProcessError('startup_timeout'))
    if (controller.signal.aborted) throw controller.signal.reason
    if (externalSignal?.aborted) throw interrupted()
  }
  let profile
  let profileIdentity
  let owner
  let closing
  let phase = 'profile'
  const close = () => {
    closing ??= (async () => {
      if (owner) await owner.terminate({ graceMs, killMs })
      if (profile) {
        const current = await lstat(profile)
        if (
          !current.isDirectory() ||
          current.isSymbolicLink() ||
          current.dev !== profileIdentity.dev ||
          current.ino !== profileIdentity.ino
        )
          throw new BrowserProcessError('cleanup_identity_mismatch')
        await rm(profile, { recursive: true, force: false })
      }
    })()
    return closing
  }
  try {
    checkDeadline()
    profile = await mkdtemp(join(tmpdir(), 'insight-console-browser-'))
    profileIdentity = await lstat(profile)
    checkDeadline()
    phase = 'spawn'
    owner = spawnFixtureChild(
      executable,
      [
        '--headless=new',
        '--disable-background-networking',
        '--disable-component-update',
        '--disable-default-apps',
        '--disable-extensions',
        '--disable-gpu',
        '--disable-dev-shm-usage',
        '--disable-sync',
        '--metrics-recording-only',
        '--no-first-run',
        '--no-sandbox',
        '--remote-debugging-address=127.0.0.1',
        '--remote-debugging-port=0',
        `--user-data-dir=${profile}`,
        origin,
      ],
      { detached: true, stdio: ['ignore', 'ignore', 'pipe'] },
    )
    const signal = AbortSignal.any([
      controller.signal,
      owner.stopped,
      ...(externalSignal ? [externalSignal] : []),
    ])
    for (;;) {
      checkDeadline()
      owner.assertRunning()
      phase = 'endpoint_file'
      const endpoint = await withinSignal(readEndpoint(profile), signal)
      if (endpoint) {
        phase = 'browser_identity'
        const version = await readJson(`${endpoint.origin}/json/version`, signal)
        if (version !== undefined) {
          if (
            !version ||
            typeof version !== 'object' ||
            Array.isArray(version) ||
            version.webSocketDebuggerUrl !== endpoint.browserSocket
          )
            throw new BrowserProcessError('browser_identity_mismatch')
          phase = 'page_identity'
          const targets = await readJson(`${endpoint.origin}/json`, signal)
          if (targets !== undefined) {
            const pageWebSocketUrl = pageSocket(targets, endpoint, origin)
            if (pageWebSocketUrl) {
              owner.assertRunning()
              checkDeadline()
              if (signal.aborted) throw signal.reason
              return {
                pageWebSocketUrl,
                close,
                assertRunning: owner.assertRunning,
                diagnostics: owner.diagnostics,
              }
            }
          }
        }
      }
      await pause(Math.min(50, Math.max(1, timeoutMs - (performance.now() - began))), signal)
    }
  } catch (error) {
    const elapsedMs = Math.round(performance.now() - began)
    const cleanupBegan = performance.now()
    let cleanup
    try {
      await close()
    } catch (failure) {
      cleanup = failure instanceof BrowserProcessError ? failure.reason : 'cleanup_failed'
    }
    throw new BrowserProcessError(
      error instanceof BrowserProcessError ? error.reason : 'startup_failed',
      {
        phase,
        elapsed_ms: elapsedMs,
        cleanup_elapsed_ms: Math.round(performance.now() - cleanupBegan),
        ...(owner?.diagnostics() ?? {}),
        ...(cleanup ? { cleanup } : {}),
      },
    )
  } finally {
    clearTimeout(timer)
  }
}
