import test from 'node:test'
import assert from 'node:assert/strict'
import { parseEventStream } from './sse.ts'

test('bounded SSE pages preserve opaque cursor, event name, and multiline JSON', () => {
  const events = parseEventStream('id: opaque-a\r\nevent: run.updated\r\ndata: {"state":\r\ndata: "waiting"}\r\n\r\nid: opaque-b\r\nevent: interaction.required\r\ndata: {"task_id":"int_01234567-89ab-7cde-8fab-0123456789ab"}\r\n\r\n')
  assert.equal(events.length, 2)
  assert.deepEqual(events[0], { id: 'opaque-a', event: 'run.updated', data: { state: 'waiting' } })
  assert.equal(events[1].id, 'opaque-b')
})

test('comments and incomplete events do not become authority events', () => {
  assert.deepEqual(parseEventStream(': keepalive\n\nevent: run.updated\ndata: {}\n\n'), [])
})

test('invalid or oversized public event data fails closed', () => {
  assert.throws(() => parseEventStream('id: cursor\ndata: []\n\n'), /JSON object/)
  assert.throws(() => parseEventStream(`id: cursor\ndata: {"value":"${'x'.repeat(270_000)}"}\n\n`), /256 KiB/)
})
