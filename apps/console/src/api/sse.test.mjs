import test from 'node:test'
import assert from 'node:assert/strict'
import { PlatformClient, PlatformProblem } from './client.ts'
import {
  followRunEventPages,
  parseEventStream,
  readRunEventStream,
  RunEventProtocolError,
  RunEventStreamDecoder,
  RunEventTransportError,
} from './sse.ts'

const encode = (text) => new TextEncoder().encode(text)
const event = (eventId, cursor = `opaque-${eventId}`) => ({ id: cursor, event: 'run.updated', data: { event_id: eventId } })
const frame = (item) => `id: ${item.id}\nevent: ${item.event}\ndata: ${JSON.stringify(item.data)}\n\n`
const historyHeaders = { 'x-insight-run-replay-floor': '0', 'x-insight-run-high-water': '100', 'x-insight-history-truncated': 'false' }
const response = (body) => new Response(body, { headers: { 'content-type': 'text/event-stream', ...historyHeaders } })
const flush = () => new Promise(setImmediate)
const options = (controller, updates, clears = []) => ({
  signal: controller.signal,
  onUpdate: (snapshot) => updates.push(snapshot),
  onClear: () => clears.push(true),
})

test('bounded SSE pages preserve opaque cursor, event name, and multiline JSON', () => {
  const events = parseEventStream('id: opaque-a\r\nevent: run.updated\r\ndata: {"state":\r\ndata: "waiting"}\r\n\r\nid: opaque-b\r\nevent: interaction.required\r\ndata: {"task_id":"int_01234567-89ab-7cde-8fab-0123456789ab"}\r\n\r\n')
  assert.equal(events.length, 2)
  assert.deepEqual(events[0], { id: 'opaque-a', event: 'run.updated', data: { state: 'waiting' } })
  assert.equal(events[1].id, 'opaque-b')
})

test('comments and unfinished frames do not become authority events', () => {
  assert.deepEqual(parseEventStream(': keepalive\n\nevent: run.updated\ndata: {}\n\n'), [])
  assert.deepEqual(parseEventStream('id: cursor\ndata: {}'), [])
  assert.deepEqual(parseEventStream('id: cursor\ndata: {}\n'), [])
})

test('incremental UTF-8, CRLF, CR and JSON survive every byte split', () => {
  const wire = encode('id: opaque+/==\r\nevent: run.updated\r\ndata: {"message":"中文🦀",\r\ndata: "state":"waiting"}\r\n\r\nid: second\revent: run.updated\rdata: {}\r\r')
  const expected = parseEventStream(new TextDecoder().decode(wire))
  for (let split = 0; split <= wire.length; split++) {
    const decoder = new RunEventStreamDecoder()
    const events = []
    decoder.push(wire.slice(0, split), (item) => events.push(item))
    decoder.push(wire.slice(split), (item) => events.push(item))
    decoder.finish()
    assert.deepEqual(events, expected, `split at byte ${split}`)
  }
  const decoder = new RunEventStreamDecoder()
  const events = []
  for (const byte of wire) decoder.push(Uint8Array.of(byte), (item) => events.push(item))
  decoder.finish()
  assert.deepEqual(events, expected)
})

test('invalid or oversized public event data and unbounded framing fail closed', () => {
  assert.throws(() => parseEventStream('id: cursor\ndata: []\n\n'), /JSON object/)
  assert.throws(() => parseEventStream('id: cursor\ndata: {\n\n'), /complete JSON/)
  assert.throws(() => parseEventStream(`id: cursor\ndata: {"value":"${'x'.repeat(270_000)}"}\n\n`), /256 KiB/)
  assert.throws(() => parseEventStream(`:${'x'.repeat(300_000)}`), /framing budget/)
  assert.throws(() => parseEventStream(Array.from({ length: 129 }, (_, index) => frame(event(String(index)))).join('')), /128 events/)
  const decoder = new RunEventStreamDecoder()
  assert.throws(() => decoder.push(Uint8Array.of(0xff), () => {}), /invalid UTF-8/)
  assert.throws(() => new RunEventStreamDecoder().push(new Uint8Array(34 * 1024 * 1024 + 1), () => {}), /34 MiB/)
})

test('EOF inside a frame or UTF-8 resumes after only the last complete event', async () => {
  const complete = frame(event('first'))
  for (const tail of [encode('id: second\ndata: {"message":'), encode('id: second\ndata: {"message":"中').slice(0, -1)]) {
    const accepted = []
    await assert.rejects(readRunEventStream(response(new ReadableStream({
      start(controller) {
        controller.enqueue(encode(complete))
        controller.enqueue(tail)
        controller.close()
      },
    })), { onEvent: (item) => accepted.push(item) }), RunEventTransportError)
    assert.deepEqual(accepted, [event('first')])
  }
})

test('a failed response body keeps earlier complete events and cancels its reader', async () => {
  const accepted = []
  let reads = 0
  const body = new ReadableStream({
    pull(controller) {
      if (reads++ === 0) controller.enqueue(encode(frame(event('first')) + 'id: second\ndata: {'))
      else controller.error(new Error('connection reset'))
    },
  })
  await assert.rejects(readRunEventStream(response(body), { onEvent: (item) => accepted.push(item) }), RunEventTransportError)
  assert.deepEqual(accepted, [event('first')])
  assert.equal(body.locked, false)
})

test('abort cancels a blocked body and cannot publish late events', async () => {
  const controller = new AbortController()
  let cancelled = false
  const accepted = []
  const pending = readRunEventStream(response(new ReadableStream({
    cancel() { cancelled = true },
  })), { signal: controller.signal, onEvent: (item) => accepted.push(item) })
  controller.abort()
  await assert.rejects(pending, { name: 'AbortError' })
  assert.equal(cancelled, true)
  assert.deepEqual(accepted, [])
})

test('a finite response must declare SSE and bounded content length', async () => {
  await assert.rejects(readRunEventStream(new Response('{}')), /text\/event-stream/)
  await assert.rejects(readRunEventStream(new Response('', { headers: {
    'content-type': 'text/event-stream', 'content-length': String(35 * 1024 * 1024),
  } })), /oversized SSE/)
})

test('reissued cursors deduplicate by durable event_id and the projection remains bounded', async () => {
  const controller = new AbortController()
  const updates = []
  const cursors = []
  let page = 0
  await followRunEventPages(async (cursor, { onEvent }) => {
    cursors.push(cursor)
    if (page++ === 0) {
      for (let index = 0; index < 128; index++) onEvent(event(String(index)))
    } else if (page === 2) {
      onEvent(event('127', 'reissued-127'))
      onEvent(event('128'))
    } else {
      controller.abort()
    }
  }, options(controller, updates))
  assert.deepEqual(cursors, [undefined, 'opaque-127', 'opaque-128'])
  assert.equal(updates.at(-2).cursor, 'reissued-127')
  assert.equal(updates.at(-2).events.length, 128)
  assert.deepEqual(updates.at(-1).events.map((item) => item.data.event_id), Array.from({ length: 128 }, (_, i) => String(i + 1)))
})

test('interruption retries with the last complete opaque cursor, never a partial frame', async (context) => {
  context.mock.timers.enable({ apis: ['setTimeout'] })
  const controller = new AbortController()
  const updates = []
  const cursors = []
  const pending = followRunEventPages(async (cursor, pageOptions) => {
    cursors.push(cursor)
    if (cursors.length === 1) {
      return readRunEventStream(response(frame(event('first', 'opaque+/cursor==')) + 'id: partial\ndata: {'), pageOptions)
    }
    pageOptions.onEvent(event('second'))
    controller.abort()
  }, options(controller, updates))
  await flush()
  assert.deepEqual(cursors, [undefined])
  context.mock.timers.tick(250)
  await pending
  assert.deepEqual(cursors, [undefined, 'opaque+/cursor=='])
  assert.deepEqual(updates.at(-1).events.map((item) => item.data.event_id), ['first', 'second'])
})

test('empty pages back off to a bounded delay and abort clears the timer', async (context) => {
  context.mock.timers.enable({ apis: ['setTimeout'] })
  const controller = new AbortController()
  let calls = 0
  const pending = followRunEventPages(async () => { calls++ }, options(controller, []))
  await flush()
  assert.equal(calls, 1)
  for (const delay of [250, 500, 1000, 2000, 4000, 5000, 5000]) {
    context.mock.timers.tick(delay - 1)
    await flush()
    const before = calls
    context.mock.timers.tick(1)
    await flush()
    assert.equal(calls, before + 1)
  }
  controller.abort()
  await pending
  const stopped = calls
  context.mock.timers.tick(50_000)
  await flush()
  assert.equal(calls, stopped)
})

test('replayed duplicate-only pages cannot cause a busy reconnect loop', async (context) => {
  context.mock.timers.enable({ apis: ['setTimeout'] })
  const controller = new AbortController()
  let calls = 0
  const updates = []
  const pending = followRunEventPages(async (_cursor, { onEvent }) => {
    onEvent(event('same-event', `reissued-${++calls}`))
  }, options(controller, updates))
  await flush()
  assert.equal(calls, 2)
  context.mock.timers.tick(249)
  await flush()
  assert.equal(calls, 2)
  context.mock.timers.tick(1)
  await flush()
  assert.equal(calls, 3)
  assert.deepEqual(updates.at(-1).events.map((item) => item.data.event_id), ['same-event'])
  assert.equal(updates.at(-1).cursor, 'reissued-3')
  controller.abort()
  await pending
})

test('cursor rejection, history unavailability, and malformed events stop without resetting history', async () => {
  // Codes are passed through, not interpreted or manufactured by the client.
  for (const code of ['cursor_expired', 'cursor_invalid', 'history_unavailable']) {
    const controller = new AbortController()
    const updates = []
    const clears = []
    const problem = Object.assign(new Error(`Public history cannot continue: ${code}`), { code, retryable: false, status: 400 })
    let calls = 0
    await assert.rejects(followRunEventPages(async (_cursor, { onEvent }) => {
      if (++calls === 1) onEvent(event('first'))
      else throw problem
    }, options(controller, updates, clears)), (error) => error === problem)
    assert.equal(calls, 2)
    assert.equal(clears.length, 1)
    assert.equal(updates.at(-1).cursor, 'opaque-first')
  }
  await assert.rejects(followRunEventPages(async (_cursor, { onEvent }) => {
    onEvent({ id: 'cursor', event: 'run.updated', data: {} })
  }, options(new AbortController(), [])), RunEventProtocolError)
})

test('authorization loss clears protected content and stops even if marked retryable', async () => {
  for (const status of [401, 403]) {
    const updates = []
    const clears = []
    let calls = 0
    const problem = Object.assign(new Error('Authorization was revoked'), { status, retryable: true })
    await assert.rejects(followRunEventPages(async (_cursor, { onEvent }) => {
      if (++calls === 1) onEvent(event('first'))
      else throw problem
    }, options(new AbortController(), updates, clears)), (error) => error === problem)
    assert.equal(calls, 2)
    assert.equal(clears.length, 2)
  }
})

test('a new principal or Run starts without the old cursor and abort fences late callbacks', async () => {
  const oldController = new AbortController()
  const oldUpdates = []
  const clears = []
  let release
  let oldAccept
  const old = followRunEventPages(async (_cursor, { onEvent }) => {
    oldAccept = onEvent
    onEvent(event('old-principal'))
    await new Promise((resolve) => { release = resolve })
  }, options(oldController, oldUpdates, clears))
  await flush()
  oldController.abort()
  oldAccept(event('late-private-content'))
  release()
  await old
  assert.equal(clears.length, 2)
  assert.equal(oldUpdates.length, 1)

  const nextController = new AbortController()
  const nextUpdates = []
  await followRunEventPages(async (cursor, { onEvent }) => {
    assert.equal(cursor, undefined)
    onEvent(event('new-principal'))
    nextController.abort()
  }, options(nextController, nextUpdates))
  assert.deepEqual(nextUpdates.at(-1).events, [event('new-principal')])
})

test('PlatformClient streams finite pages with scoped auth, opaque resume header, and AbortSignal', async (context) => {
  const controller = new AbortController()
  const expected = event('first', 'opaque+/cursor==')
  expected.data.message = '中文🦀'
  let request
  context.mock.method(globalThis, 'fetch', async (url, init) => {
    request = { url, init }
    return response(new ReadableStream({
      start(stream) {
        for (const byte of encode(frame(expected))) stream.enqueue(Uint8Array.of(byte))
        stream.close()
      },
    }))
  })
  const accepted = []
  const client = new PlatformClient('https://platform.example/v1', 'memory-only-token')
  const page = await client.getRunEvents('run_example', 'opaque previous+/==', {
    signal: controller.signal, onEvent: (item) => accepted.push(item),
  })
  assert.deepEqual(page, [expected])
  assert.deepEqual(accepted, page)
  assert.equal(request.url, 'https://platform.example/v1/runs/run_example/events')
  assert.equal(request.init.headers.get('authorization'), 'Bearer memory-only-token')
  assert.equal(request.init.headers.get('last-event-id'), 'opaque previous+/==')
  assert.equal(request.init.headers.get('accept'), 'text/event-stream')
  assert.equal(request.init.signal, controller.signal)
  assert.equal(request.init.credentials, 'omit')
  assert.equal(request.init.redirect, 'error')
})

test('PlatformClient follows a transport failure but preserves a closed cursor error for the UI', async (context) => {
  context.mock.timers.enable({ apis: ['setTimeout'] })
  let calls = 0
  context.mock.method(globalThis, 'fetch', async () => {
    if (++calls === 1) throw new TypeError('fetch failed')
    return new Response(JSON.stringify({ code: 'cursor_expired', detail: 'The Run event cursor has expired.', retryable: false }), {
      status: 400, headers: { 'content-type': 'application/problem+json', 'trace-id': 'safe-trace-id' },
    })
  })
  const client = new PlatformClient('https://platform.example', 'token')
  const pending = client.followRunEvents('run_example', options(new AbortController(), []))
  const rejected = assert.rejects(pending, (error) => error instanceof PlatformProblem
    && error.code === 'cursor_expired' && error.traceId === 'safe-trace-id')
  await flush()
  assert.equal(calls, 1)
  context.mock.timers.tick(250)
  await rejected
  assert.equal(calls, 2)
})

test('current SSE headers are required, bounded u64 values and retained history is reported before an empty page', async () => {
  const seen = []
  const headers = { 'content-type': 'text/event-stream', 'x-insight-run-replay-floor': '9007199254740993', 'x-insight-run-high-water': '18446744073709551615', 'x-insight-history-truncated': 'true' }
  assert.deepEqual(await readRunEventStream(new Response('', { headers }), { onHistory: (history) => seen.push(history) }), [])
  assert.deepEqual(seen, [{ replayFloor: '9007199254740993', highWaterSequence: '18446744073709551615', truncated: true }])
  for (const changed of [{ 'x-insight-run-replay-floor': undefined }, { 'x-insight-run-high-water': '18446744073709551616' }, { 'x-insight-run-replay-floor': '01' }, { 'x-insight-run-high-water': '8' }, { 'x-insight-history-truncated': 'unknown' }]) {
    const current = { ...headers, ...changed }
    for (const key of Object.keys(current)) if (current[key] === undefined) delete current[key]
    await assert.rejects(readRunEventStream(new Response('', { headers: current })), /Required Run history boundaries/)
  }
})
