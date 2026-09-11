// Offline local-input tests only. These never start Chrome or call a Gateway/provider.
import assert from 'node:assert/strict'
import test from 'node:test'
import { chmod, link, mkdtemp, readFile, readdir, rm, symlink, writeFile } from 'node:fs/promises'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import {
  parseArguments,
  readPrivateSession,
  runInstalledModelChat,
} from './model-chat-installation-journey.ts'

test('installed Model chat requires an explicit fresh intent and safe Gateway origin', () => {
  const args = [
    '--endpoint',
    'http://127.0.0.1:8080',
    '--session-file',
    '/private/tmp/session',
    '--agent-name',
    'qwen-check',
    '--evidence-directory',
    '/private/tmp/new-observation',
  ]
  assert.equal(parseArguments(args).agentName, 'qwen-check')
  for (const endpoint of ['https://model.example', 'http://localhost:8080', 'http://[::1]:8080']) {
    assert.ok(parseArguments([...args.slice(0, 1), endpoint, ...args.slice(2)]))
  }
  for (const endpoint of [
    'http://external.example',
    'https://user:secret@model.example',
    'https://model.example/path',
    'https://model.example/?secret=x',
    'https://model.example/#x',
    'file:///tmp/a',
  ]) {
    assert.throws(() => parseArguments([...args.slice(0, 1), endpoint, ...args.slice(2)]))
  }
  for (const invalid of [
    args.slice(0, -1),
    [...args, '--endpoint', 'https://model.example'],
    [...args, '--approve', 'true'],
    [...args.slice(0, 5), '../bad', ...args.slice(6)],
  ]) {
    assert.throws(() => parseArguments(invalid))
  }
})

test('an existing evidence directory fails before browser startup and preserves the previous intent', async () => {
  const directory = await mkdtemp(join(tmpdir(), 'insight-model-chat-existing-'))
  try {
    const session = join(directory, 'session')
    await writeFile(session, 'synthetic-session', { mode: 0o600 })
    await writeFile(join(directory, 'report.json'), 'previous unknown outcome', { mode: 0o600 })
    const report = await runInstalledModelChat({
      endpoint: 'http://127.0.0.1:1',
      sessionFile: session,
      agentName: 'existing',
      evidenceDirectory: directory,
    })
    assert.equal(report.status, 'failed')
    assert.equal(report.stage, 'preflight')
    assert.equal(await readFile(join(directory, 'report.json'), 'utf8'), 'previous unknown outcome')
    assert.deepEqual((await readdir(directory)).sort(), ['report.json', 'session'])
    assert.ok(!JSON.stringify(report).includes('synthetic-session'))
  } finally {
    await rm(directory, { recursive: true, force: true })
  }
})

test('session read uses a bounded private regular inode and rejects ambiguous or unsafe bytes', async () => {
  const directory = await mkdtemp(join(tmpdir(), 'insight-model-chat-input-'))
  try {
    const path = join(directory, 'session')
    await writeFile(path, 'synthetic-session\n', { mode: 0o600 })
    assert.equal(await readPrivateSession(path), 'synthetic-session')
    await chmod(path, 0o644)
    await assert.rejects(readPrivateSession(path))
    await chmod(path, 0o600)
    const alias = join(directory, 'alias')
    await symlink(path, alias)
    await assert.rejects(readPrivateSession(alias))
    await rm(alias)
    await link(path, alias)
    await assert.rejects(readPrivateSession(path))
    await rm(alias)
    for (const bytes of [
      '',
      'two\nlines',
      'trailing\n\n',
      ' leading',
      Buffer.from([0xff]),
      'x'.repeat(65538),
    ]) {
      await writeFile(path, bytes)
      await assert.rejects(readPrivateSession(path))
    }
    await writeFile(path, 'x'.repeat(65536) + '\n')
    assert.equal((await readPrivateSession(path)).length, 65536)
    await assert.rejects(readPrivateSession(directory))
  } finally {
    await rm(directory, { recursive: true, force: true })
  }
})
