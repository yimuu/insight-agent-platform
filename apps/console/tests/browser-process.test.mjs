import assert from 'node:assert/strict'
import { once } from 'node:events'
import { chmod, lstat, mkdir, mkdtemp, readFile, rm, writeFile } from 'node:fs/promises'
import { tmpdir } from 'node:os'
import { dirname, join } from 'node:path'
import test from 'node:test'
import { BrowserProcessError, spawnFixtureChild, startHeadlessBrowser, waitForFixtureChild } from './browser-process.mjs'
import { fixtureReadyOrigin, waitForFixture } from './qualify-browser-fixture.mjs'

const origin = 'http://127.0.0.1:12345'
const delay = ms => new Promise(resolve => setTimeout(resolve, ms))
const exists = async path => lstat(path).then(() => true, error => { if (error.code === 'ENOENT') return false; throw error })
const pidExists = pid => { try { process.kill(pid, 0); return true } catch (error) { if (error.code === 'ESRCH') return false; throw error } }

async function fakeBrowser(t, mode = 'ready', { ignoreTerm = false, pauseAfterRecord = false } = {}) {
  const directory = await mkdtemp(join(tmpdir(), 'insight-browser-test-'))
  const executable = join(directory, 'browser.mjs')
  const recordPath = join(directory, 'record.json')
  const releasePath = join(directory, 'release')
  await writeFile(executable, `#!${process.execPath}
import {createServer} from 'node:http';
import {writeFileSync,symlinkSync,linkSync,renameSync,mkdirSync,existsSync} from 'node:fs';
import {join} from 'node:path';
const mode=${JSON.stringify(mode)}, recordPath=${JSON.stringify(recordPath)};
const profile=process.argv.find(arg=>arg.startsWith('--user-data-dir=')).slice('--user-data-dir='.length);
const pageOrigin=process.argv.at(-1), record={profile,pid:process.pid,requests:[]};
const save=()=>{writeFileSync(recordPath+'.pending',JSON.stringify(record));renameSync(recordPath+'.pending',recordPath)};save();
if(${pauseAfterRecord}){
 const deadline=performance.now()+2000;
 while(!existsSync(${JSON.stringify(releasePath)})){
  if(performance.now()>=deadline)process.exit(9);
  await new Promise(resolve=>setTimeout(resolve,10));
 }
}
process.stderr.write('test-secret-canary https://private.invalid/secret\\n');
if(mode==='exit')process.exit(7);
if(mode==='signal')process.kill(process.pid,'SIGTERM');
const sockets=new Set();
let port;
const server=createServer((request,response)=>{
 record.requests.push(request.url);save();
 if(mode==='headers')return;
 if(mode==='body'){response.writeHead(200,{'content-type':'application/json'});response.write('[');return;}
 if(mode==='redirect'&&request.url==='/json/version'){response.writeHead(302,{location:'/redirected'});response.end();return;}
 if(mode==='huge'){response.writeHead(200,{'content-type':'application/json'});response.end('['+' '.repeat(65536)+']');return;}
 if(mode==='declared-huge'){response.writeHead(200,{'content-length':'65537'});response.end();return;}
 if(mode==='encoded'){response.writeHead(200,{'content-encoding':'gzip'});response.end('untrusted');return;}
 if(mode==='malformed'){response.end('{');return;}
 if(mode==='null'){response.end('null');return;}
 if(mode==='invalid-utf8'){response.end(Buffer.from([0xff]));return;}
 const browserSocket='ws://127.0.0.1:'+port+'/devtools/browser/test-browser';
 const page={type:'page',id:'test-page',url:pageOrigin+'/',webSocketDebuggerUrl:'ws://127.0.0.1:'+port+'/devtools/page/test-page'};
 if(mode==='foreign-page')page.webSocketDebuggerUrl='ws://example.invalid:12345/devtools/page/test-page';
 if(mode==='wrong-origin')page.url='http://127.0.0.1:12346/';
 response.setHeader('content-type','application/json');
 response.end(JSON.stringify(request.url==='/json/version'?{webSocketDebuggerUrl:mode==='wrong-browser'?'ws://example.invalid:12345/devtools/browser/test-browser':browserSocket}:mode==='duplicate-page'?[page,page]:[page]));
});
server.on('connection',socket=>{sockets.add(socket);socket.on('close',()=>sockets.delete(socket))});
server.listen(0,'127.0.0.1',()=>{
 port=server.address().port;record.port=port;save();
 let text=port+'\\n/devtools/browser/test-browser\\n';
 if(mode==='missing')return;
 if(mode==='file-empty')text='';
 if(mode==='file-port')text='65536\\n/devtools/browser/test-browser\\n';
 if(mode==='file-leading-zero')text='0'+port+'\\n/devtools/browser/test-browser\\n';
 if(mode==='file-path')text=port+'\\nhttps://private.invalid/\\n';
 if(mode==='file-null')text='null';
 if(mode==='file-huge')text='x'.repeat(257);
 if(mode==='replace-profile'||mode==='replace-profile-invalid'){renameSync(profile,profile+'-moved');mkdirSync(profile);record.moved=profile+'-moved';save();if(mode==='replace-profile')return;text='null';}
 const path=join(profile,'DevToolsActivePort');
 if(mode==='file-link'||mode==='file-hardlink'){
  const target=join(profile,'target');writeFileSync(target,text);
  if(mode==='file-link')symlinkSync(target,path);else linkSync(target,path);
 }else writeFileSync(path,text);
});
process.on('SIGTERM',()=>{${ignoreTerm ? 'return;' : 'for(const socket of sockets)socket.destroy();server.close(()=>process.exit(0));'}});
`)
  await chmod(executable, 0o700)
  const record = () => readFile(recordPath, 'utf8').then(JSON.parse, error => { if (error.code === 'ENOENT') return null; throw error })
  t.after(async () => {
    const observed = await record()
    if (observed && pidExists(observed.pid)) {
      process.kill(observed.pid, 'SIGKILL')
      for (let attempt = 0; attempt < 100 && pidExists(observed.pid); attempt++) await delay(10)
      assert.equal(pidExists(observed.pid), false, 'owned fixture child must exit before directory cleanup')
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
  const owner = spawnFixtureChild(process.execPath, ['-e', "process.on('SIGTERM',()=>{});console.log('ready');setInterval(()=>{},1000)"])
  try {
    await once(owner.child.stdout, 'data')
    const status = await owner.terminate({ graceMs: 10, killMs: 1000 })
    assert.equal(status.signal, 'SIGKILL')
    assert.equal(owner.child.signalCode, 'SIGKILL')
    assert.equal(pidExists(owner.child.pid), false)
    assert.deepEqual(await owner.terminate(), status)
  } finally { await owner.terminate() }
})

test('detached cleanup waits for the group after its leader exits', async t => {
  for (const holdPipe of [true, false]) await t.test(holdPipe ? 'descendant holds stderr' : 'leader close already arrived', async () => {
    const directory = await mkdtemp(join(tmpdir(), 'insight-browser-group-'))
    const recordPath = join(directory, 'descendant.json')
    const descendantPath = join(directory, 'descendant.cjs')
    await writeFile(descendantPath, "process.on('SIGTERM',()=>{});process.send({pid:process.pid});setInterval(()=>{},1000)")
    const script = `const {fork}=require('node:child_process');const fs=require('node:fs');const child=fork(${JSON.stringify(descendantPath)},[],{stdio:['ignore','ignore',${holdPipe ? "'inherit'" : "'ignore'"},'ipc']});child.once('message',message=>{fs.writeFileSync(${JSON.stringify(recordPath)},JSON.stringify(message));process.exit(0)});`
    const owner = spawnFixtureChild(process.execPath, ['-e', script], { detached: true, stdio: ['ignore', 'ignore', 'pipe'] })
    const unrelated = spawnFixtureChild(process.execPath, ['-e', "console.log('ready');setInterval(()=>{},1000)"], { detached: true })
    let descendant
    try {
      await once(unrelated.child.stdout, 'data')
      await withinTestDeadline(new Promise(resolve => owner.child.exitCode !== null ? resolve() : owner.child.once('exit', resolve)))
      descendant = JSON.parse(await readFile(recordPath, 'utf8')).pid
      if (!holdPipe) await withinTestDeadline(owner.completed)
      assert.equal(pidExists(descendant), true)
      const status = await owner.terminate({ graceMs: 20, killMs: 1000 })
      assert.equal(status.code, 0)
      assert.equal(pidExists(descendant), false, 'group descendants must exit even after leader close')
      assert.equal(pidExists(-owner.child.pid), false, 'original process group must be gone')
      assert.equal(pidExists(unrelated.child.pid), true, 'separate owned fixture group must remain alive')
      const kill = process.kill
      const signals = []
      // Once gone, even a reused numeric group ID must never be probed or signalled again.
      process.kill = (pid, signal) => { if (pid === -owner.child.pid) { signals.push(signal); return true } return kill(pid, signal) }
      try { assert.deepEqual(await owner.terminate(), status) } finally { process.kill = kill }
      assert.deepEqual(signals, [])
    } finally {
      // This fallback also makes the expected RED run leave no surviving test group.
      if (descendant && pidExists(descendant)) process.kill(-owner.child.pid, 'SIGKILL')
      await withinTestDeadline(owner.completed)
      if (descendant) {
        for (let attempt = 0; attempt < 200 && pidExists(descendant); attempt++) await delay(10)
        assert.equal(pidExists(descendant), false)
      }
      await unrelated.terminate({ graceMs: 20, killMs: 1000 })
      await rm(directory, { recursive: true })
    }
  })
})

async function withinTestDeadline(promise) {
  let timer
  try { return await Promise.race([promise, new Promise((_, reject) => { timer = setTimeout(() => reject(new Error('test process deadline exceeded')), 2000) })]) }
  finally { clearTimeout(timer) }
}

test('group cleanup errors preserve the startup failure and private profile', async t => {
  for (const fault of ['timeout', 'permission']) await t.test(fault, async t => {
    const fixture = await fakeBrowser(t, 'file-null')
    const kill = process.kill
    const signals = []
    process.kill = (pid, signal) => {
      if (pid >= 0) return kill(pid, signal)
      signals.push({ pid, signal })
      if (fault === 'permission') throw Object.assign(new Error('test-only denied signal'), { code: 'EPERM' })
      return true // Controlled fault injection: no signal is delivered to this test's group.
    }
    try {
      await assert.rejects(startHeadlessBrowser({ executable: fixture.executable, origin, graceMs: 20, killMs: 20 }), error => {
        assert.equal(error.reason, 'invalid_endpoint_file')
        assert.match(error.message, new RegExp(`"cleanup":"${fault === 'timeout' ? 'cleanup_timeout' : 'termination_failed'}"`))
        assert.ok(!error.message.includes('test-secret-canary'))
        return true
      })
    } finally { process.kill = kill }
    const observed = await fixture.record()
    assert.equal(pidExists(observed.pid), true)
    assert.equal(await exists(observed.profile), true, 'failed group cleanup must not remove the private profile')
    assert.ok(signals.every(entry => entry.pid === -observed.pid))
    assert.deepEqual(signals.filter(entry => entry.signal !== 0).map(entry => entry.signal), fault === 'timeout' ? ['SIGTERM', 'SIGKILL'] : ['SIGTERM'])
  })
})

test('startup deadline includes a stalled JSON body and headers', async t => {
  for (const mode of ['body', 'headers']) await t.test(mode, async t => {
    const fixture = await fakeBrowser(t, mode)
    const began = performance.now()
    await assert.rejects(startHeadlessBrowser({ executable: fixture.executable, origin, timeoutMs: 1000 }), error => error.reason === 'startup_timeout')
    assert.ok(performance.now() - began < 2500)
    const observed = await fixture.record()
    assert.ok(observed.requests.includes('/json/version'))
    assert.equal(pidExists(observed.pid), false)
    assert.equal(await exists(observed.profile), false)
  })
})

test('port zero binds the private file to actual browser and page endpoints', async t => {
  const fixture = await fakeBrowser(t)
  const browser = await startHeadlessBrowser({ executable: fixture.executable, origin })
  const observed = await fixture.record()
  try {
    assert.equal(browser.pageWebSocketUrl, `ws://127.0.0.1:${observed.port}/devtools/page/test-page`)
    assert.equal((await lstat(observed.profile)).mode & 0o777, 0o700)
    assert.deepEqual(observed.requests, ['/json/version', '/json'])
  } finally { await browser.close() }
  assert.equal(pidExists(observed.pid), false)
  assert.equal(await exists(observed.profile), false)
  await browser.close()
})

test('missing, malformed and linked endpoint files fail closed', async t => {
  for (const mode of ['missing', 'file-empty', 'file-port', 'file-leading-zero', 'file-path', 'file-null', 'file-huge', 'file-link', 'file-hardlink']) await t.test(mode, async t => {
    const fixture = await fakeBrowser(t, mode)
    await assert.rejects(startHeadlessBrowser({ executable: fixture.executable, origin, timeoutMs: 1000 }), error => error.reason === (['missing', 'file-empty'].includes(mode) ? 'startup_timeout' : 'invalid_endpoint_file'))
    const observed = await fixture.record()
    assert.deepEqual(observed.requests, [])
    assert.equal(pidExists(observed.pid), false)
    assert.equal(await exists(observed.profile), false)
  })
})

test('debug responses reject foreign identities, ambiguity and oversized or invalid bodies', async t => {
  for (const mode of ['wrong-browser', 'foreign-page', 'duplicate-page', 'huge', 'declared-huge', 'encoded', 'malformed', 'null', 'invalid-utf8', 'redirect', 'wrong-origin']) await t.test(mode, async t => {
    const fixture = await fakeBrowser(t, mode)
    const expectedReason = ['redirect', 'wrong-origin'].includes(mode) ? 'startup_timeout'
      : ['wrong-browser', 'null'].includes(mode) ? 'browser_identity_mismatch' : 'invalid_debug_response'
    await assert.rejects(startHeadlessBrowser({ executable: fixture.executable, origin, timeoutMs: 1000 }), error => error.reason === expectedReason)
    const observed = await fixture.record()
    assert.ok(observed.requests.includes('/json/version'))
    assert.equal(observed.requests.includes('/redirected'), false)
    assert.equal(pidExists(observed.pid), false)
    assert.equal(await exists(observed.profile), false)
  })
})

test('early exit, signal and spawn errors are prompt and never expose stderr or paths', async t => {
  for (const mode of ['exit', 'signal', 'missing-executable', 'not-executable']) await t.test(mode, async t => {
    const fixture = await fakeBrowser(t, mode)
    let executable = fixture.executable
    if (mode === 'missing-executable') executable += '-missing-private-path'
    if (mode === 'not-executable') await chmod(executable, 0o600)
    const began = performance.now()
    await assert.rejects(startHeadlessBrowser({ executable, origin }), error => {
      assert.equal(error.reason, mode.endsWith('executable') ? 'spawn_failed' : 'child_exited')
      for (const sensitive of ['test-secret-canary', 'private.invalid', executable]) assert.equal(error.message.includes(sensitive), false)
      return true
    })
    assert.ok(performance.now() - began < 1500)
  })
})

test('browser cleanup escalates before removing its private profile', async t => {
  const fixture = await fakeBrowser(t, 'ready', { ignoreTerm: true })
  const browser = await startHeadlessBrowser({ executable: fixture.executable, origin, graceMs: 10, killMs: 1000 })
  const observed = await fixture.record()
  await browser.close()
  assert.equal(pidExists(observed.pid), false)
  assert.equal(await exists(observed.profile), false)
})

test('profile replacement is preserved and cleanup failure retains the startup failure', async t => {
  const fixture = await fakeBrowser(t, 'replace-profile')
  await assert.rejects(startHeadlessBrowser({ executable: fixture.executable, origin, timeoutMs: 1000 }), error => {
    assert.equal(error.reason, 'startup_timeout')
    assert.ok(error.message.includes('cleanup_identity_mismatch'))
    return true
  })
  const observed = await fixture.record()
  assert.equal(pidExists(observed.pid), false)
  assert.equal(await exists(observed.profile), true)
  assert.equal(await exists(observed.moved), true)
})

test('outer journey timeout preserves its deadline then terminates and reaps', async () => {
  const owner = spawnFixtureChild(process.execPath, ['-e', "process.on('SIGTERM',()=>process.exit(143));console.log('ready');setInterval(()=>{},1000)"])
  try {
    await once(owner.child.stdout, 'data')
    await assert.rejects(waitForFixtureChild(owner, 20), error => error.reason === 'journey_timeout')
    assert.equal((await owner.terminate()).code, 143)
    assert.equal(pidExists(owner.child.pid), false)
  } finally { await owner.terminate() }
})

test('SIGTERM during browser startup cleans up and cannot emit Passed', async t => {
  const fixture = await fakeBrowser(t, 'missing')
  const moduleUrl = new URL('./browser-process.mjs', import.meta.url).href
  const script = `import {qualificationSignals,startHeadlessBrowser} from ${JSON.stringify(moduleUrl)};const signals=qualificationSignals();try{const browser=await startHeadlessBrowser({executable:${JSON.stringify(fixture.executable)},origin:${JSON.stringify(origin)},signal:signals.signal});await browser.close();console.log('Passed')}catch(e){console.error(e.message)}finally{signals.dispose()}process.exit(process.exitCode??1)`
  const owner = spawnFixtureChild(process.execPath, ['--input-type=module', '-e', script])
  let stdout = ''
  owner.child.stdout.on('data', chunk => { stdout += chunk })
  try {
    for (let attempt = 0; attempt < 100 && !await fixture.record(); attempt++) await delay(10)
    const observed = await fixture.record()
    assert.ok(observed)
    assert.equal((await owner.terminate()).code, 143)
    assert.equal(stdout.includes('Passed'), false)
    assert.equal(pidExists(observed.pid), false)
    assert.equal(await exists(observed.profile), false)
  } finally { await owner.terminate() }
})

test('the real journey entry retains safe startup errors and does not pass on cleanup failure', async t => {
  for (const mode of ['exit', 'missing', 'replace-profile-invalid']) await t.test(mode, async t => {
    const fixture = await fakeBrowser(t, mode, { pauseAfterRecord: mode === 'replace-profile-invalid' })
    const bundle = join(dirname(fixture.executable), 'bundle')
    await mkdir(bundle)
    await writeFile(join(bundle, 'index.html'), '<!doctype html><title>startup failure fixture</title>')
    const entry = new URL('./real-gateway-journey.mjs', import.meta.url)
    const owner = spawnFixtureChild(process.execPath, [entry.pathname], { env: {
      INSIGHT_CONSOLE_BROWSER_BIN: fixture.executable,
      INSIGHT_CONSOLE_BUNDLE_ROOT: bundle,
      INSIGHT_CONSOLE_GATEWAY_ORIGIN: origin,
      INSIGHT_CONSOLE_MANAGEMENT_GATEWAY_ORIGIN: origin,
      INSIGHT_CONSOLE_ACCESS_TOKEN: 'entry-secret-canary',
      INSIGHT_CONSOLE_RUN_ID: 'run_0198f1c3-8f49-7c3e-b1f3-773c28367b90',
      INSIGHT_CONSOLE_TASK_ID: 'int_0198f1c3-8f49-7c3e-b1f3-773c28367b91',
      INSIGHT_CONSOLE_TASK_SAFE_PROMPT_KEY: 'interaction.confirm_release',
      INSIGHT_CONSOLE_TASK_RESPONSE: '{}',
    } })
    let stdout = ''
    let stderr = ''
    owner.child.stdout.on('data', chunk => { stdout += chunk })
    owner.child.stderr.on('data', chunk => { stderr += chunk })
    try {
      for (let attempt = 0; attempt < 200 && !await fixture.record(); attempt++) await delay(10)
      const initial = await fixture.record()
      assert.ok(initial)
      if (mode === 'replace-profile-invalid') {
        assert.equal(initial.moved, undefined, 'capture the initial record before profile replacement')
        await fixture.release()
      }
      const status = mode === 'missing' ? await owner.terminate() : await waitForFixtureChild(owner, 2000)
      const observed = await fixture.record()
      assert.ok(observed)
      assert.equal(observed.pid, initial.pid)
      assert.equal(observed.profile, initial.profile)
      assert.equal(status.code, mode === 'missing' ? 143 : 1)
      assert.equal(stdout, '')
      assert.ok(stderr.includes(mode === 'missing' ? 'interrupted' : mode === 'exit' ? 'child_exited' : 'invalid_endpoint_file'))
      for (const sensitive of ['entry-secret-canary', 'test-secret-canary', 'private.invalid', observed.profile]) assert.equal(stderr.includes(sensitive), false)
      assert.equal(pidExists(observed.pid), false)
      assert.equal(await exists(observed.profile), mode === 'replace-profile-invalid')
      if (mode === 'replace-profile-invalid') {
        assert.ok(stderr.includes('cleanup_identity_mismatch'))
        assert.equal(await exists(observed.moved), true)
      }
    } finally {
      try { if (mode === 'replace-profile-invalid') await fixture.release() }
      finally { await owner.terminate() }
    }
  })
})

test('fixture readiness uses the actual loopback port and rejects malformed identities', async () => {
  const suffix = 'run=run_0198f1c3-8f49-7c3e-b1f3-773c28367b90 task=int_0198f1c3-8f49-7c3e-b1f3-773c28367b91'
  const good = `console fixture ready http://127.0.0.1:45678 ${suffix}\n`
  assert.equal(fixtureReadyOrigin(good), 'http://127.0.0.1:45678')
  assert.equal(fixtureReadyOrigin(good.slice(0, -1)), null)
  for (const value of [good + good, good.replace('127.0.0.1', 'example.invalid'), good.replace('45678', '0'), good.replace('45678', '65536'), good.replace('45678', '045678'), good.replace(suffix, 'run=wrong task=wrong')]) assert.throws(() => fixtureReadyOrigin(value), BrowserProcessError)
  const owner = spawnFixtureChild(process.execPath, ['-e', 'process.exit(7)'])
  try { await assert.rejects(waitForFixture(() => '', owner, 1000, new AbortController().signal), error => error.reason === 'child_exited') } finally { await owner.terminate() }
})
