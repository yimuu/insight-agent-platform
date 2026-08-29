import type { JsonObject, RunEvent } from './types.ts'

const MAX_EVENT_BYTES = 256 * 1024

function parseBlock(block: string): RunEvent | null {
  let id = ''
  let event = 'message'
  const data: string[] = []

  for (const line of block.split('\n')) {
    if (!line || line.startsWith(':')) continue
    const separator = line.indexOf(':')
    const field = separator < 0 ? line : line.slice(0, separator)
    let value = separator < 0 ? '' : line.slice(separator + 1)
    if (value.startsWith(' ')) value = value.slice(1)
    if (field === 'id') id = value
    if (field === 'event') event = value
    if (field === 'data') data.push(value)
  }

  if (!id || data.length === 0) return null
  const raw = data.join('\n')
  if (new TextEncoder().encode(raw).byteLength > MAX_EVENT_BYTES) {
    throw new Error('event_too_large: SSE event exceeded 256 KiB')
  }

  const decoded: unknown = JSON.parse(raw)
  if (decoded === null || typeof decoded !== 'object' || Array.isArray(decoded)) {
    throw new Error('invalid_event: SSE data must be a JSON object')
  }
  return { id, event, data: decoded as JsonObject }
}

export function parseEventStream(text: string): RunEvent[] {
  return text.replaceAll('\r\n', '\n').split('\n\n').map(parseBlock).filter((event): event is RunEvent => event !== null)
}
