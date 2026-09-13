export type LiveTextFrame = { schema_version: 1; run_id: string } & (
  | { kind: 'opened'; partial: true }
  | { kind: 'reset'; model_turn_id: string; attempt_no: number }
  | { kind: 'text'; model_turn_id: string; attempt_no: number; text_sequence: number; text: string }
  | {
      kind: 'gap'
      reason: 'late_subscription' | 'sequence_gap' | 'transport_reconnected' | 'slow_consumer'
    }
  | {
      kind: 'closed'
      reason:
        | 'terminal'
        | 'cancelled'
        | 'expired'
        | 'authorization_changed'
        | 'unavailable'
        | 'duration_limit'
    }
)
const encoder = new TextEncoder()
export function parseLiveFrame(raw: string, runId: string): LiveTextFrame | null {
  let event = ''
  const data: string[] = []
  for (const line of raw.split('\n')) {
    if (!line || line.startsWith(':')) continue
    const separator = line.indexOf(':')
    const field = separator < 0 ? line : line.slice(0, separator)
    const value = (separator < 0 ? '' : line.slice(separator + 1)).replace(/^ /, '')
    if (field === 'id') throw new Error('实时输出不能携带历史游标。')
    if (field === 'event') event = value
    if (field === 'data') data.push(value)
  }
  if (!data.length) return null
  const value: unknown = JSON.parse(data.join('\n'))
  if (!value || typeof value !== 'object' || Array.isArray(value))
    throw new Error('实时输出格式错误。')
  const frame = value as Record<string, unknown>
  if (frame.schema_version !== 1 || frame.run_id !== runId || frame.kind !== event)
    throw new Error('实时输出身份不匹配。')
  const positive = (n: unknown) => typeof n === 'number' && Number.isSafeInteger(n) && n > 0
  const identity =
    typeof frame.model_turn_id === 'string' &&
    /^mturn_[0-9a-f]{8}-[0-9a-f]{4}-7[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/.test(
      frame.model_turn_id,
    ) &&
    positive(frame.attempt_no) &&
    Number(frame.attempt_no) <= 4294967295
  const valid =
    event === 'opened'
      ? frame.partial === true
      : event === 'reset'
        ? identity
        : event === 'text'
          ? identity &&
            positive(frame.text_sequence) &&
            typeof frame.text === 'string' &&
            encoder.encode(frame.text).length <= 65536 &&
            !frame.text.includes('\0')
          : event === 'gap'
            ? [
                'late_subscription',
                'sequence_gap',
                'transport_reconnected',
                'slow_consumer',
              ].includes(String(frame.reason))
            : event === 'closed'
              ? [
                  'terminal',
                  'cancelled',
                  'expired',
                  'authorization_changed',
                  'unavailable',
                  'duration_limit',
                ].includes(String(frame.reason))
              : false
  const fields = [
    'schema_version',
    'run_id',
    'kind',
    ...(event === 'opened'
      ? ['partial']
      : event === 'reset'
        ? ['model_turn_id', 'attempt_no']
        : event === 'text'
          ? ['model_turn_id', 'attempt_no', 'text_sequence', 'text']
          : ['reason']),
  ]
  if (!valid || Object.keys(frame).some((key) => !fields.includes(key)))
    throw new Error('实时输出字段不合法。')
  return frame as LiveTextFrame
}
export async function readLiveTextStream(
  response: Response,
  runId: string,
  signal: AbortSignal,
  accept: (frame: LiveTextFrame) => void,
): Promise<void> {
  if (
    response.headers.get('content-type')?.split(';')[0]?.trim() !== 'text/event-stream' ||
    !response.body
  ) {
    await response.body?.cancel()
    throw new Error('服务未返回实时输出流。')
  }
  const reader = response.body.getReader()
  const decoder = new TextDecoder('utf-8', { fatal: true })
  let buffer = ''
  let closed = false
  let opened = false
  let skipLf = false
  let line = ''
  const abort = () => {
    void reader.cancel().catch(() => {})
  }
  signal.addEventListener('abort', abort, { once: true })
  const consume = (text: string) => {
    for (const character of text) {
      if (skipLf) {
        skipLf = false
        if (character === '\n') continue
      }
      if (character !== '\n' && character !== '\r') {
        line += character
        if (line.length + buffer.length > 262144) throw new Error('实时输出帧过大。')
        continue
      }
      skipLf = character === '\r'
      if (line) {
        buffer += line + '\n'
        line = ''
        continue
      }
      const frame = parseLiveFrame(buffer, runId)
      buffer = ''
      if (!frame) continue
      if (
        closed ||
        (!opened && frame.kind !== 'opened' && frame.kind !== 'closed') ||
        (opened && frame.kind === 'opened')
      )
        throw new Error('实时输出顺序错误。')
      opened = true
      closed = frame.kind === 'closed'
      signal.throwIfAborted()
      accept(frame)
    }
  }
  try {
    while (!closed) {
      signal.throwIfAborted()
      const chunk = await reader.read()
      signal.throwIfAborted()
      if (chunk.done) {
        consume(decoder.decode())
        break
      }
      consume(decoder.decode(chunk.value, { stream: true }))
    }
    if (!closed) throw new Error('实时连接中断，临时回答可能不完整。最终结果仍将独立读取。')
  } finally {
    signal.removeEventListener('abort', abort)
    await reader.cancel().catch(() => {})
    reader.releaseLock()
  }
}
