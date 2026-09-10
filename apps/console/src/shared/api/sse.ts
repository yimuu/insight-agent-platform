import type { JsonObject, RunEvent } from './types.ts'

const MAX_EVENT_BYTES = 256 * 1024
const MAX_FRAME_BYTES = MAX_EVENT_BYTES + 16 * 1024
const MAX_PAGE_BYTES = 34 * 1024 * 1024
const MAX_PAGE_EVENTS = 128
const MAX_SEEN_EVENTS = 512
const INITIAL_RETRY_MILLISECONDS = 250
const MAX_RETRY_MILLISECONDS = 5_000
const encoder = new TextEncoder()

export class RunEventProtocolError extends Error {
  constructor(message: string) {
    super(message)
    this.name = 'RunEventProtocolError'
  }
}

export class RunEventTransportError extends Error {
  constructor() {
    super('event_stream_interrupted: Reconnecting from the last complete durable event.')
    this.name = 'RunEventTransportError'
  }
}

function parseBlock(lines: string[]): RunEvent | null {
  let id = ''
  let event = 'message'
  const data: string[] = []
  for (const line of lines) {
    if (line.startsWith(':')) continue
    const separator = line.indexOf(':')
    const field = separator < 0 ? line : line.slice(0, separator)
    let value = separator < 0 ? '' : line.slice(separator + 1)
    if (value.startsWith(' ')) value = value.slice(1)
    if (field === 'id') id = value
    if (field === 'event') event = value
    if (field === 'data') data.push(value)
  }
  if (!id || data.length === 0) return null
  if (id.includes('\0'))
    throw new RunEventProtocolError('invalid_event: SSE cursor contains a null byte')
  const raw = data.join('\n')
  if (encoder.encode(raw).byteLength > MAX_EVENT_BYTES) {
    throw new RunEventProtocolError('event_too_large: SSE event exceeded 256 KiB')
  }
  let decoded: unknown
  try {
    decoded = JSON.parse(raw)
  } catch {
    throw new RunEventProtocolError('invalid_event: SSE data must contain complete JSON')
  }
  if (decoded === null || typeof decoded !== 'object' || Array.isArray(decoded)) {
    throw new RunEventProtocolError('invalid_event: SSE data must be a JSON object')
  }
  return { id, event, data: decoded as JsonObject }
}

/** Only a blank line commits a frame; EOF never commits a partial event. */
export class RunEventStreamDecoder {
  private readonly decoder = new TextDecoder('utf-8', { fatal: true })
  private line = ''
  private lines: string[] = []
  private skipLf = false
  private frameBytes = 0
  private pageBytes = 0
  private eventCount = 0

  push(bytes: Uint8Array, accept: (event: RunEvent) => void): void {
    this.pageBytes += bytes.byteLength
    if (this.pageBytes > MAX_PAGE_BYTES) {
      throw new RunEventProtocolError('response_too_large: SSE page exceeded 34 MiB')
    }
    let text: string
    try {
      text = this.decoder.decode(bytes, { stream: true })
    } catch {
      throw new RunEventProtocolError('invalid_event: SSE contains invalid UTF-8')
    }
    let start = 0
    for (let index = 0; index < text.length; index++) {
      const character = text[index]
      if (this.skipLf) {
        this.skipLf = false
        if (character === '\n') {
          start = index + 1
          continue
        }
      }
      if (character !== '\r' && character !== '\n') continue
      this.append(text.slice(start, index))
      this.frameBytes++
      if (this.line === '') {
        const event = parseBlock(this.lines)
        this.lines = []
        this.frameBytes = 0
        if (event) {
          if (++this.eventCount > MAX_PAGE_EVENTS) {
            throw new RunEventProtocolError('too_many_events: SSE page exceeded 128 events')
          }
          accept(event)
        }
      } else {
        this.lines.push(this.line)
        this.line = ''
      }
      this.skipLf = character === '\r'
      start = index + 1
    }
    this.append(text.slice(start))
  }

  private append(fragment: string): void {
    this.frameBytes += encoder.encode(fragment).byteLength
    if (this.frameBytes > MAX_FRAME_BYTES) {
      throw new RunEventProtocolError(
        'event_too_large: SSE frame exceeded the 256 KiB data and bounded framing budget',
      )
    }
    this.line += fragment
  }

  finish(): void {
    try {
      this.decoder.decode()
    } catch {
      throw new RunEventTransportError()
    }
    if (this.line || this.lines.length) throw new RunEventTransportError()
  }
}

export function parseEventStream(text: string): RunEvent[] {
  const events: RunEvent[] = []
  new RunEventStreamDecoder().push(encoder.encode(text), (event) => events.push(event))
  return events
}

export interface RunEventHistory {
  readonly replayFloor: string
  readonly highWaterSequence: string
  readonly truncated: boolean
}

export interface RunEventPageOptions {
  onHistory?: (history: RunEventHistory) => void
  signal?: AbortSignal
  onEvent?: (event: RunEvent) => void
}

export async function readRunEventStream(
  response: Response,
  options: RunEventPageOptions = {},
): Promise<RunEvent[]> {
  const { signal, onEvent } = options
  signal?.throwIfAborted()
  if (
    response.headers.get('content-type')?.split(';', 1)[0].trim().toLowerCase() !==
    'text/event-stream'
  ) {
    await response.body?.cancel()
    throw new RunEventProtocolError('invalid_event_stream: Expected text/event-stream')
  }
  const declared = response.headers.get('content-length')
  if (declared !== null && (!/^\d+$/.test(declared) || Number(declared) > MAX_PAGE_BYTES)) {
    await response.body?.cancel()
    throw new RunEventProtocolError('response_too_large: Invalid or oversized SSE page length')
  }
  const replayFloor = response.headers.get('x-insight-run-replay-floor')
  const highWaterSequence = response.headers.get('x-insight-run-high-water')
  const truncated = response.headers.get('x-insight-history-truncated')
  const validU64 = (value: string | null): value is string =>
    value !== null &&
    /^(0|[1-9][0-9]{0,19})$/.test(value) &&
    BigInt(value) <= 18_446_744_073_709_551_615n
  if (
    !validU64(replayFloor) ||
    !validU64(highWaterSequence) ||
    BigInt(replayFloor) > BigInt(highWaterSequence) ||
    (truncated !== 'true' && truncated !== 'false')
  ) {
    await response.body?.cancel()
    throw new RunEventProtocolError(
      'invalid_event_history: Required Run history boundaries are missing or invalid.',
    )
  }
  options.onHistory?.({ replayFloor, highWaterSequence, truncated: truncated === 'true' })
  if (!response.body) return []
  const reader = response.body.getReader()
  const abort = () => {
    void reader.cancel().catch(() => {})
  }
  signal?.addEventListener('abort', abort, { once: true })
  const decoder = new RunEventStreamDecoder()
  const events: RunEvent[] = []
  try {
    while (true) {
      signal?.throwIfAborted()
      let chunk: ReadableStreamReadResult<Uint8Array>
      try {
        chunk = await reader.read()
      } catch {
        signal?.throwIfAborted()
        throw new RunEventTransportError()
      }
      signal?.throwIfAborted()
      if (chunk.done) break
      decoder.push(chunk.value, (event) => {
        signal?.throwIfAborted()
        onEvent?.(event)
        events.push(event)
      })
    }
    decoder.finish()
    return events
  } finally {
    signal?.removeEventListener('abort', abort)
    await reader.cancel().catch(() => {})
    reader.releaseLock()
  }
}

export interface RunEventFollowSnapshot {
  readonly events: readonly RunEvent[]
  readonly cursor: string | undefined
}

export interface RunEventFollowOptions {
  onHistory?: (history: RunEventHistory) => void
  signal: AbortSignal
  onUpdate: (snapshot: RunEventFollowSnapshot) => void
  /** Clear all protected projections on a new scope, cancellation, or lost authorization. */
  onClear: () => void
}

export type RunEventPageReader = (
  cursor: string | undefined,
  options: RunEventPageOptions,
) => Promise<unknown>

function retryable(error: unknown): boolean {
  if (error instanceof RunEventTransportError) return true
  // A closed API Problem owns retryability. Never discard a rejected cursor and restart history.
  return (
    error instanceof Error &&
    'retryable' in error &&
    error.retryable === true &&
    !('status' in error && (error.status === 401 || error.status === 403))
  )
}

function wait(milliseconds: number, signal: AbortSignal): Promise<void> {
  return new Promise((resolve) => {
    if (signal.aborted) {
      resolve()
      return
    }
    const complete = () => {
      clearTimeout(timer)
      signal.removeEventListener('abort', complete)
      resolve()
    }
    const timer = setTimeout(complete, milliseconds)
    signal.addEventListener('abort', complete, { once: true })
  })
}

/** One call owns one in-memory scope. Abort it before changing Run, principal, token, or Gateway. */
export async function followRunEventPages(
  readPage: RunEventPageReader,
  options: RunEventFollowOptions,
): Promise<void> {
  const { signal, onUpdate, onClear } = options
  let cursor: string | undefined
  let events: RunEvent[] = []
  const seen = new Set<string>()
  let retryMilliseconds = INITIAL_RETRY_MILLISECONDS
  const clear = () => {
    cursor = undefined
    events = []
    seen.clear()
    onClear()
  }
  clear()
  signal.addEventListener('abort', clear, { once: true })
  try {
    while (!signal.aborted) {
      let accepted = 0
      try {
        await readPage(cursor, {
          signal,
          onHistory(history) {
            if (!signal.aborted) options.onHistory?.(history)
          },
          onEvent(event) {
            if (signal.aborted) return
            const eventId = event.data.event_id
            if (typeof eventId !== 'string' || !eventId) {
              throw new RunEventProtocolError(
                'invalid_event: Durable public event is missing event_id',
              )
            }
            if (!seen.has(eventId)) {
              seen.add(eventId)
              if (seen.size > MAX_SEEN_EVENTS) seen.delete(seen.values().next().value!)
              events = [...events, event].slice(-MAX_PAGE_EVENTS)
              accepted++
            }
            // A reissued cursor can differ for the same durable event. Keep it opaque.
            cursor = event.id
            onUpdate({ events: [...events], cursor })
          },
        })
        if (signal.aborted) break
        if (accepted > 0) {
          retryMilliseconds = INITIAL_RETRY_MILLISECONDS
          continue
        }
      } catch (error) {
        if (signal.aborted) break
        if (!retryable(error)) {
          if (
            error instanceof Error &&
            'status' in error &&
            (error.status === 401 || error.status === 403)
          )
            clear()
          throw error
        }
      }
      await wait(retryMilliseconds, signal)
      retryMilliseconds = Math.min(retryMilliseconds * 2, MAX_RETRY_MILLISECONDS)
    }
  } finally {
    signal.removeEventListener('abort', clear)
  }
}
