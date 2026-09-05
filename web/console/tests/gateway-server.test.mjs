import assert from 'node:assert/strict'
import { mkdtempSync, mkdirSync, readFileSync, rmSync, symlinkSync, writeFileSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import test from 'node:test'
import { checkedBundleRoot } from './gateway-server.mjs'

test('accepts an explicit regular candidate bundle root', () => {
  const root = mkdtempSync(join(tmpdir(), 'insight-console-candidate-'))
  writeFileSync(join(root, 'index.html'), '<!doctype html><title>candidate console</title>')
  try {
    const checked = checkedBundleRoot(root)
    assert.match(readFileSync(join(checked, 'index.html'), 'utf8'), /candidate console/)
  } finally {
    rmSync(root, { recursive: true })
  }
})

test('rejects symbolic links anywhere in a candidate bundle', () => {
  const root = mkdtempSync(join(tmpdir(), 'insight-console-candidate-'))
  const outside = mkdtempSync(join(tmpdir(), 'insight-console-outside-'))
  try {
    writeFileSync(join(root, 'index.html'), '<!doctype html>')
    mkdirSync(join(root, 'assets'))
    writeFileSync(join(outside, 'asset.js'), 'throw new Error("outside")')
    symlinkSync(join(outside, 'asset.js'), join(root, 'assets', 'asset.js'))
    assert.throws(
      () => checkedBundleRoot(root),
      /must not contain symbolic links/,
    )
  } finally {
    rmSync(root, { recursive: true })
    rmSync(outside, { recursive: true })
  }
})
