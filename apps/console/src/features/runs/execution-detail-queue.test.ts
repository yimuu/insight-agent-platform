import { test } from 'node:test'
import assert from 'node:assert/strict'
import { nextDetailBatch } from './execution-detail-queue.ts'

test('frequently changing early nodes cannot starve later nodes, even with a selected priority', () => {
  const targets = Array.from({ length: 12 }, (_, i) => ({
    id: `${i}`,
    stamp: '1',
    priority: i === 0,
  }))
  const seen = new Map<string, string>()
  let cursor = 0
  for (let turn = 0; turn < 5; turn++) {
    for (const target of targets.slice(0, 4)) target.stamp = String(turn + 1)
    const next = nextDetailBatch(targets, seen, cursor)
    assert.ok(next.batch.length <= 4)
    assert.equal(next.batch[0]?.id, '0')
    for (const target of next.batch) seen.set(target.id, target.stamp)
    cursor = next.cursor
  }
  assert.equal(seen.size, targets.length)
})

test('unchanged versions are not fetched again; a newly selected later node has priority', () => {
  const targets = Array.from({ length: 129 }, (_, i) => ({
    id: `${i}`,
    stamp: '1',
    priority: i === 128,
  }))
  assert.equal(nextDetailBatch(targets, new Map(), 50).batch[0]?.id, '128')
  const seen = new Map(targets.map((target) => [target.id, target.stamp]))
  assert.deepEqual(nextDetailBatch(targets, seen, 0).batch, [])
})
