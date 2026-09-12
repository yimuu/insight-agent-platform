import { test } from 'node:test'
import assert from 'node:assert/strict'
import { parseLiveFrame, readLiveTextStream } from './live-text.ts'
const run = 'run-test'
const frame = (kind: string, data: object) =>
  `event: ${kind}\ndata: ${JSON.stringify({ schema_version: 1, run_id: run, kind, ...data })}\n\n`
test('live frames have no durable cursor and scope must match', () => {
  assert.equal(parseLiveFrame(frame('opened', { partial: true }), run)?.kind, 'opened')
  assert.throws(() => parseLiveFrame('id: cursor\n' + frame('opened', { partial: true }), run))
  assert.throws(() => parseLiveFrame(frame('opened', { partial: true }), 'other'))
  assert.throws(() =>
    parseLiveFrame(
      frame('text', {
        model_turn_id: 'mturn_0198f1cc-32e4-75e1-a9e8-d95ca0f80002',
        attempt_no: 1,
        text_sequence: 0,
        text: 'x',
      }),
      run,
    ),
  )
})
test('UTF-8 split live text is delivered before final close', async () => {
  const frames =
    frame('opened', { partial: true }) +
    frame('text', {
      model_turn_id: 'mturn_0198f1cc-32e4-75e1-a9e8-d95ca0f80002',
      attempt_no: 1,
      text_sequence: 1,
      text: '你好🌍',
    }) +
    frame('closed', { reason: 'terminal' })
  const bytes = new TextEncoder().encode(frames)
  let at = 0
  const received: string[] = []
  await readLiveTextStream(
    new Response(
      new ReadableStream({
        pull(c) {
          if (at < bytes.length) c.enqueue(bytes.slice(at, ++at))
          else c.close()
        },
      }),
      { headers: { 'content-type': 'text/event-stream' } },
    ),
    run,
    new AbortController().signal,
    (f) => received.push(f.kind === 'text' ? f.text : f.kind),
  )
  assert.deepEqual(received, ['opened', '你好🌍', 'closed'])
})
test('truncated streams fail explicitly rather than appearing complete', async () => {
  await assert.rejects(
    readLiveTextStream(
      new Response(frame('opened', { partial: true }), {
        headers: { 'content-type': 'text/event-stream' },
      }),
      run,
      new AbortController().signal,
      () => {},
    ),
    /中断/,
  )
})

test('initial authorization closure reaches the consumer without requiring opened', async () => {
  let text = 'previous text'
  await readLiveTextStream(
    new Response(frame('closed', { reason: 'authorization_changed' }), {
      headers: { 'content-type': 'text/event-stream' },
    }),
    run,
    new AbortController().signal,
    (f) => {
      if (f.kind === 'closed' && f.reason === 'authorization_changed') text = ''
    },
  )
  assert.equal(text, '')
})
