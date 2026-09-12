import type { ModelDestinationChoice } from '../../shared/api/model-types.ts'

export function destinationLabel(destination: ModelDestinationChoice): string {
  const host = new URL(destination.base_url).hostname
  const names: Record<string, string> = {
    'dashscope.aliyuncs.com': '阿里百炼 · 北京',
    'dashscope-intl.aliyuncs.com': '阿里百炼 · 国际',
    'api.openai.com': 'OpenAI',
    'api.anthropic.com': 'Anthropic',
  }
  return names[host] ?? `${host} · ${destination.region}`
}

// Generated once when opening a new form, and kept stable through retries.
export function configurationAlias(kind: 'source' | 'model'): string {
  return `${kind}.${crypto.randomUUID()}`
}
