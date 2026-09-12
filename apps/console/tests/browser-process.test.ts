import { errorValue } from './fixtures/types.ts'
import assert from 'node:assert/strict'
import { once } from 'node:events'
import { chmod, lstat, mkdir, mkdtemp, readFile, rm, writeFile } from 'node:fs/promises'
import { tmpdir } from 'node:os'
import { dirname, join } from 'node:path'
import test from 'node:test'
import {
  BrowserProcessError,
  spawnFixtureChild,
  startHeadlessBrowser,
  waitForFixtureChild,
} from './browser-process.ts'
import { fixtureReadyOrigin, waitForFixture } from './qualify-browser-fixture.ts'

const origin = 'http://127.0.0.1:12345'
const delay = (ms) => new Promise((resolve) => setTimeout(resolve, ms))
const exists = async (path) =>
  lstat(path).then(
    () => true,
    (error) => {
      if (error.code === 'ENOENT') return false
      throw error
    },
  )
const pidExists = (pid) => {
  try {
    process.kill(pid, 0)
    return true
  } catch (error) {
    if (error.code === 'ESRCH') return false
    throw error
  }
}

async function fakeBrowser(
  t,
  mode = 'ready',
  { ignoreTerm = false, pauseAfterRecord = false } = {},
) {
  const directory = await mkdtemp(join(tmpdir(), 'insight-browser-test-'))
  const executable = join(directory, 'browser.ts')
  const recordPath = join(directory, 'record.json')
  const releasePath = join(directory, 'release')
  await writeFile(
    join(directory, 'fixture.json'),
    JSON.stringify({ mode, recordPath, releasePath, pauseAfterRecord, ignoreTerm }),
  )
  await writeFile(
    executable,
    `#!${process.execPath}\n${await readFile(new URL('./fixtures/fake-browser.ts', import.meta.url), 'utf8')}`,
  )
  await chmod(executable, 0o700)
  const record = () =>
    readFile(recordPath, 'utf8').then(JSON.parse, (error) => {
      if (error.code === 'ENOENT') return null
      throw error
    })
  t.after(async () => {
    const observed = await record()
    if (observed && pidExists(observed.pid)) {
      process.kill(observed.pid, 'SIGKILL')
      for (let attempt = 0; attempt < 100 && pidExists(observed.pid); attempt++) await delay(10)
      assert.equal(
        pidExists(observed.pid),
        false,
        'owned fixture child must exit before directory cleanup',
      )
    }
    if (observed) {
      await rm(observed.profile, { recursive: true, force: true })
      if (observed.moved) await rm(observed.moved, { recursive: true, force: true })
    }
    await rm(directory, { recursive: true, force: true })
  })
  return { executable, record, release: () => writeFile(releasePath, '') }
}

test('termination does not finish before the owned process exits', async () => {
  const owner = spawnFixtureChild(process.execPath, [
    '-e',
    "process.on('SIGTERM',()=>{});console.log('ready');setInterval(()=>{},1000)",
  ])
  try {
    await once(owner.child.stdout, 'data')
    const status = await owner.terminate({ graceMs: 10, killMs: 1000 })
    assert.equal(status.signal, 'SIGKILL')
    assert.equal(owner.child.signalCode, 'SIGKILL')
    assert.equal(pidExists(owner.child.pid), false)
    assert.deepEqual(await owner.terminate(), status)
  } finally {
    await owner.terminate()
  }
})

test('detached cleanup waits for the group after its leader exits', async (t) => {
  for (const holdPipe of [true, false])
    await t.test(
      holdPipe ? 'descendant holds stderr' : 'leader close already arrived',
      async () => {
        const directory = await mkdtemp(join(tmpdir(), 'insight-browser-group-'))
        const recordPath = join(directory, 'descendant.json')
        const descendantPath = join(directory, 'descendant.cjs')
        await writeFile(
          descendantPath,
          "process.on('SIGTERM',()=>{});process.send({pid:process.pid});setInterval(()=>{},1000)",
        )
        const script = `const {fork}=require('node:child_process');const fs=require('node:fs');const child=fork(${JSON.stringify(descendantPath)},[],{stdio:['ignore','ignore',${holdPipe ? "'inherit'" : "'ignore'"},'ipc']});child.once('message',message=>{fs.writeFileSync(${JSON.stringify(recordPath)},JSON.stringify(message));process.exit(0)});`
        const owner = spawnFixtureChild(process.execPath, ['-e', script], {
          detached: true,
          stdio: ['ignore', 'ignore', 'pipe'],
        })
        const unrelated = spawnFixtureChild(
          process.execPath,
          ['-e', "console.log('ready');setInterval(()=>{},1000)"],
          { detached: true },
        )
        let descendant
        try {
          await once(unrelated.child.stdout, 'data')
          await withinTestDeadline(
            new Promise<void>((resolve) =>
              owner.child.exitCode !== null ? resolve() : owner.child.once('exit', resolve),
            ),
          )
          descendant = JSON.parse(await readFile(recordPath, 'utf8')).pid
          if (!holdPipe) await withinTestDeadline(owner.completed)
          assert.equal(pidExists(descendant), true)
          const status = await owner.terminate({ graceMs: 20, killMs: 1000 })
          assert.equal(status.code, 0)
          assert.equal(
            pidExists(descendant),
            false,
            'group descendants must exit even after leader close',
          )
          assert.equal(pidExists(-owner.child.pid), false, 'original process group must be gone')
          assert.equal(
            pidExists(unrelated.child.pid),
            true,
            'separate owned fixture group must remain alive',
          )
          const kill = process.kill
          const signals = []
          // Once gone, even a reused numeric group ID must never be probed or signalled again.
          process.kill = (pid, signal) => {
            if (pid === -owner.child.pid) {
              signals.push(signal)
              return true
            }
            return kill(pid, signal)
          }
          try {
            assert.deepEqual(await owner.terminate(), status)
          } finally {
            process.kill = kill
          }
          assert.deepEqual(signals, [])
        } finally {
          // A failed assertion or leader-close deadline must not skip the other owned group.
          try {
            if (descendant && pidExists(descendant)) {
              try {
                // The leader may already be reaped; only this recorded fixture PID is needed.
                process.kill(descendant, 'SIGKILL')
              } catch (error) {
                if (error.code !== 'ESRCH') throw error
              }
            }
            await withinTestDeadline(owner.completed)
            if (descendant) {
              for (let attempt = 0; attempt < 200 && pidExists(descendant); attempt++)
                await delay(10)
              assert.equal(pidExists(descendant), false)
            }
          } finally {
            try {
              await unrelated.terminate({ graceMs: 20, killMs: 1000 })
            } finally {
              await rm(directory, { recursive: true })
            }
          }
        }
      },
    )
})

async function withinTestDeadline(promise) {
  let timer
  try {
    return await Promise.race([
      promise,
      new Promise((_, reject) => {
        timer = setTimeout(() => reject(new Error('test process deadline exceeded')), 2000)
      }),
    ])
  } finally {
    clearTimeout(timer)
  }
}

test('group cleanup errors preserve the startup failure and private profile', async (t) => {
  for (const fault of ['timeout', 'permission'])
    await t.test(fault, async (t) => {
      const fixture = await fakeBrowser(t, 'file-null')
      const kill = process.kill
      const signals = []
      process.kill = (pid, signal) => {
        if (pid >= 0) return kill(pid, signal)
        signals.push({ pid, signal })
        if (fault === 'permission')
          throw Object.assign(new Error('test-only denied signal'), { code: 'EPERM' })
        return true // Controlled fault injection: no signal is delivered to this test's group.
      }
      try {
        await assert.rejects(
          startHeadlessBrowser({ executable: fixture.executable, origin, graceMs: 20, killMs: 20 }),
          (error) => {
            assert.equal(errorValue(error).reason, 'invalid_endpoint_file')
            assert.match(
              errorValue(error).message,
              new RegExp(
                `"cleanup":"${fault === 'timeout' ? 'cleanup_timeout' : 'termination_failed'}"`,
              ),
            )
            assert.ok(!errorValue(error).message.includes('test-secret-canary'))
            return true
          },
        )
      } finally {
        process.kill = kill
      }
      const observed = await fixture.record()
      assert.equal(pidExists(observed.pid), true)
      assert.equal(
        await exists(observed.profile),
        true,
        'failed group cleanup must not remove the private profile',
      )
      assert.ok(signals.every((entry) => entry.pid === -observed.pid))
      assert.deepEqual(
        signals.filter((entry) => entry.signal !== 0).map((entry) => entry.signal),
        fault === 'timeout' ? ['SIGTERM', 'SIGKILL'] : ['SIGTERM'],
      )
    })
})

test('startup deadline includes a stalled JSON body and headers', async (t) => {
  for (const mode of ['body', 'headers'])
    await t.test(mode, async (t) => {
      const fixture = await fakeBrowser(t, mode)
      const began = performance.now()
      await assert.rejects(
        startHeadlessBrowser({ executable: fixture.executable, origin, timeoutMs: 1000 }),
        (error) => errorValue(error).reason === 'startup_timeout',
      )
      assert.ok(performance.now() - began < 2500)
      const observed = await fixture.record()
      assert.ok(observed.requests.includes('/json/version'))
      assert.equal(pidExists(observed.pid), false)
      assert.equal(await exists(observed.profile), false)
    })
})

test('port zero binds the private file to actual browser and page endpoints', async (t) => {
  const fixture = await fakeBrowser(t)
  const browser = await startHeadlessBrowser({ executable: fixture.executable, origin })
  const observed = await fixture.record()
  try {
    assert.equal(
      browser.pageWebSocketUrl,
      `ws://127.0.0.1:${observed.port}/devtools/page/test-page`,
    )
    assert.equal((await lstat(observed.profile)).mode & 0o777, 0o700)
    assert.deepEqual(observed.requests, ['/json/version', '/json'])
  } finally {
    await browser.close()
  }
  assert.equal(pidExists(observed.pid), false)
  assert.equal(await exists(observed.profile), false)
  await browser.close()
})

test('missing, malformed and linked endpoint files fail closed', async (t) => {
  for (const mode of [
    'missing',
    'file-empty',
    'file-port',
    'file-leading-zero',
    'file-path',
    'file-null',
    'file-huge',
    'file-link',
    'file-hardlink',
  ])
    await t.test(mode, async (t) => {
      const fixture = await fakeBrowser(t, mode)
      await assert.rejects(
        startHeadlessBrowser({ executable: fixture.executable, origin, timeoutMs: 1000 }),
        (error) =>
          errorValue(error).reason ===
          (['missing', 'file-empty'].includes(mode) ? 'startup_timeout' : 'invalid_endpoint_file'),
      )
      const observed = await fixture.record()
      assert.deepEqual(observed.requests, [])
      assert.equal(pidExists(observed.pid), false)
      assert.equal(await exists(observed.profile), false)
    })
})

test('debug responses reject foreign identities, ambiguity and oversized or invalid bodies', async (t) => {
  for (const mode of [
    'wrong-browser',
    'foreign-page',
    'duplicate-page',
    'huge',
    'declared-huge',
    'encoded',
    'malformed',
    'null',
    'invalid-utf8',
    'redirect',
    'wrong-origin',
  ])
    await t.test(mode, async (t) => {
      const fixture = await fakeBrowser(t, mode)
      const expectedReason = ['redirect', 'wrong-origin'].includes(mode)
        ? 'startup_timeout'
        : ['wrong-browser', 'null'].includes(mode)
          ? 'browser_identity_mismatch'
          : 'invalid_debug_response'
      await assert.rejects(
        startHeadlessBrowser({ executable: fixture.executable, origin, timeoutMs: 3000 }),
        (error) => errorValue(error).reason === expectedReason,
      )
      const observed = await fixture.record()
      assert.ok(observed.requests.includes('/json/version'))
      assert.equal(observed.requests.includes('/redirected'), false)
      assert.equal(pidExists(observed.pid), false)
      assert.equal(await exists(observed.profile), false)
    })
})

test('early exit, signal and spawn errors are prompt and never expose stderr or paths', async (t) => {
  for (const mode of ['exit', 'signal', 'missing-executable', 'not-executable'])
    await t.test(mode, async (t) => {
      const fixture = await fakeBrowser(t, mode)
      let executable = fixture.executable
      if (mode === 'missing-executable') executable += '-missing-private-path'
      if (mode === 'not-executable') await chmod(executable, 0o600)
      const began = performance.now()
      await assert.rejects(startHeadlessBrowser({ executable, origin }), (error) => {
        assert.equal(
          errorValue(error).reason,
          mode.endsWith('executable') ? 'spawn_failed' : 'child_exited',
        )
        for (const sensitive of ['test-secret-canary', 'private.invalid', executable])
          assert.equal(errorValue(error).message.includes(sensitive), false)
        return true
      })
      assert.ok(performance.now() - began < 1500)
    })
})

test('browser cleanup escalates before removing its private profile', async (t) => {
  const fixture = await fakeBrowser(t, 'ready', { ignoreTerm: true })
  const browser = await startHeadlessBrowser({
    executable: fixture.executable,
    origin,
    graceMs: 10,
    killMs: 1000,
  })
  const observed = await fixture.record()
  await browser.close()
  assert.equal(pidExists(observed.pid), false)
  assert.equal(await exists(observed.profile), false)
})

test('profile replacement is preserved and cleanup failure retains the startup failure', async (t) => {
  const fixture = await fakeBrowser(t, 'replace-profile')
  await assert.rejects(
    startHeadlessBrowser({ executable: fixture.executable, origin, timeoutMs: 1000 }),
    (error) => {
      assert.equal(errorValue(error).reason, 'startup_timeout')
      assert.ok(errorValue(error).message.includes('cleanup_identity_mismatch'))
      return true
    },
  )
  const observed = await fixture.record()
  assert.equal(pidExists(observed.pid), false)
  assert.equal(await exists(observed.profile), true)
  assert.equal(await exists(observed.moved), true)
})

test('outer journey timeout preserves its deadline then terminates and reaps', async () => {
  const owner = spawnFixtureChild(process.execPath, [
    '-e',
    "process.on('SIGTERM',()=>process.exit(143));console.log('ready');setInterval(()=>{},1000)",
  ])
  try {
    await once(owner.child.stdout, 'data')
    await assert.rejects(
      waitForFixtureChild(owner, 20),
      (error) => errorValue(error).reason === 'journey_timeout',
    )
    assert.equal((await owner.terminate()).code, 143)
    assert.equal(pidExists(owner.child.pid), false)
  } finally {
    await owner.terminate()
  }
})

test('SIGTERM during browser startup cleans up and cannot emit Passed', async (t) => {
  const fixture = await fakeBrowser(t, 'missing')
  const moduleUrl = new URL('./browser-process.ts', import.meta.url).href
  const script = `import {qualificationSignals,startHeadlessBrowser} from ${JSON.stringify(moduleUrl)};const signals=qualificationSignals();try{const browser=await startHeadlessBrowser({executable:${JSON.stringify(fixture.executable)},origin:${JSON.stringify(origin)},signal:signals.signal});await browser.close();console.log('Passed')}catch(e){console.error(e.message)}finally{signals.dispose()}process.exit(process.exitCode??1)`
  const owner = spawnFixtureChild(process.execPath, ['--input-type=module', '-e', script])
  let stdout = ''
  owner.child.stdout.on('data', (chunk) => {
    stdout += chunk
  })
  try {
    for (let attempt = 0; attempt < 100 && !(await fixture.record()); attempt++) await delay(10)
    const observed = await fixture.record()
    assert.ok(observed)
    assert.equal((await owner.terminate()).code, 143)
    assert.equal(stdout.includes('Passed'), false)
    assert.equal(pidExists(observed.pid), false)
    assert.equal(await exists(observed.profile), false)
  } finally {
    await owner.terminate()
  }
})

test('the real journey entry retains safe startup errors and does not pass on cleanup failure', async (t) => {
  for (const mode of ['exit', 'missing', 'replace-profile-invalid'])
    await t.test(mode, async (t) => {
      const fixture = await fakeBrowser(t, mode, {
        pauseAfterRecord: mode === 'replace-profile-invalid',
      })
      const bundle = join(dirname(fixture.executable), 'bundle')
      await mkdir(bundle)
      await writeFile(
        join(bundle, 'index.html'),
        '<!doctype html><title>startup failure fixture</title>',
      )
      const entry = new URL('./real-gateway-journey.ts', import.meta.url)
      const owner = spawnFixtureChild(process.execPath, [entry.pathname], {
        env: {
          INSIGHT_CONSOLE_BROWSER_BIN: fixture.executable,
          INSIGHT_CONSOLE_BUNDLE_ROOT: bundle,
          INSIGHT_CONSOLE_GATEWAY_ORIGIN: origin,
          INSIGHT_CONSOLE_MANAGEMENT_GATEWAY_ORIGIN: origin,
          INSIGHT_CONSOLE_ACCESS_TOKEN: 'entry-secret-canary',
          INSIGHT_CONSOLE_RUN_ID: 'run_0198f1c3-8f49-7c3e-b1f3-773c28367b90',
          INSIGHT_CONSOLE_TASK_ID: 'int_0198f1c3-8f49-7c3e-b1f3-773c28367b91',
          INSIGHT_CONSOLE_TASK_SAFE_PROMPT_KEY: 'interaction.confirm_release',
          INSIGHT_CONSOLE_TASK_RESPONSE: '{}',
        },
      })
      let stdout = ''
      let stderr = ''
      owner.child.stdout.on('data', (chunk) => {
        stdout += chunk
      })
      owner.child.stderr.on('data', (chunk) => {
        stderr += chunk
      })
      try {
        for (let attempt = 0; attempt < 200 && !(await fixture.record()); attempt++) await delay(10)
        const initial = await fixture.record()
        assert.ok(initial)
        if (mode === 'replace-profile-invalid') {
          assert.equal(
            initial.moved,
            undefined,
            'capture the initial record before profile replacement',
          )
          await fixture.release()
        }
        const status =
          mode === 'missing' ? await owner.terminate() : await waitForFixtureChild(owner, 2000)
        const observed = await fixture.record()
        assert.ok(observed)
        assert.equal(observed.pid, initial.pid)
        assert.equal(observed.profile, initial.profile)
        assert.equal(status.code, mode === 'missing' ? 143 : 1)
        assert.equal(stdout, '')
        assert.ok(
          stderr.includes(
            mode === 'missing'
              ? 'interrupted'
              : mode === 'exit'
                ? 'child_exited'
                : 'invalid_endpoint_file',
          ),
        )
        for (const sensitive of [
          'entry-secret-canary',
          'test-secret-canary',
          'private.invalid',
          observed.profile,
        ])
          assert.equal(stderr.includes(sensitive), false)
        assert.equal(pidExists(observed.pid), false)
        assert.equal(await exists(observed.profile), mode === 'replace-profile-invalid')
        if (mode === 'replace-profile-invalid') {
          assert.ok(stderr.includes('cleanup_identity_mismatch'))
          assert.equal(await exists(observed.moved), true)
        }
      } finally {
        try {
          if (mode === 'replace-profile-invalid') await fixture.release()
        } finally {
          await owner.terminate()
        }
      }
    })
})

test('fixture readiness uses the actual loopback port and rejects malformed identities', async () => {
  const suffix =
    'run=run_0198f1c3-8f49-7c3e-b1f3-773c28367b90 task=int_0198f1c3-8f49-7c3e-b1f3-773c28367b91'
  const good = `console fixture ready http://127.0.0.1:45678 ${suffix}\n`
  assert.equal(fixtureReadyOrigin(good), 'http://127.0.0.1:45678')
  assert.equal(fixtureReadyOrigin(good.slice(0, -1)), null)
  for (const value of [
    good + good,
    good.replace('127.0.0.1', 'example.invalid'),
    good.replace('45678', '0'),
    good.replace('45678', '65536'),
    good.replace('45678', '045678'),
    good.replace(suffix, 'run=wrong task=wrong'),
  ])
    assert.throws(() => fixtureReadyOrigin(value), BrowserProcessError)
  const owner = spawnFixtureChild(process.execPath, ['-e', 'process.exit(7)'])
  try {
    await assert.rejects(
      waitForFixture(() => '', owner, 1000, new AbortController().signal),
      (error) => errorValue(error).reason === 'child_exited',
    )
  } finally {
    await owner.terminate()
  }
})
