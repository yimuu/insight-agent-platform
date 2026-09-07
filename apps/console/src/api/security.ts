const SENSITIVE_KEY = /(?:authorization|cookie|credential|password|secret|token|completion_proof|upload_target|raw_(?:prompt|body)|prompt|tool_(?:input|output))/i

export const REDACTED = '[redacted]'

export function redact(value: unknown, depth = 0): unknown {
  if (depth > 8) return '[truncated]'
  if (Array.isArray(value)) return value.slice(0, 128).map((item) => redact(item, depth + 1))
  if (value === null || typeof value !== 'object') return value

  return Object.fromEntries(
    Object.entries(value as Record<string, unknown>)
      .slice(0, 256)
      .map(([key, item]) => [key, key !== 'safe_prompt_key' && SENSITIVE_KEY.test(key) ? REDACTED : redact(item, depth + 1)]),
  )
}

export function safeJson(value: unknown): string {
  return JSON.stringify(redact(value), null, 2)
}

export function discoverTaskIds(value: unknown): string[] {
  const matches = safeJson(value).match(/\b(?:int|apv)_[0-9a-f-]{36}\b/g) ?? []
  return [...new Set(matches)]
}

export function newReceipt(scope: string): string {
  const normalized = scope.toLowerCase().replace(/[^a-z0-9._-]+/g, '-').slice(0, 48)
  return `console-${normalized}-${crypto.randomUUID()}`
}
