import type { ModelEndpoint, ModelProtocol } from '../../shared/api/model-types.ts'
export const MODEL_SERVICES = [
  {
    id: 'dashscope',
    name: '阿里百炼 · 北京',
    url: 'https://dashscope.aliyuncs.com/compatible-mode/v1',
    protocol: 'open_ai_responses',
    region: 'cn-beijing',
  },
  {
    id: 'openai',
    name: 'OpenAI',
    url: 'https://api.openai.com/v1',
    protocol: 'open_ai_responses',
    region: 'global',
  },
  {
    id: 'anthropic',
    name: 'Anthropic',
    url: 'https://api.anthropic.com',
    protocol: 'anthropic_messages',
    region: 'global',
  },
] as const
export function modelEndpoint(value: string): ModelEndpoint {
  if (/[\\%?#@\s]/.test(value) || value.split('/').some((part) => part === '.' || part === '..'))
    throw new Error('服务地址不能包含账号、查询参数或转义路径。')
  let url: URL
  try {
    url = new URL(value.trim())
  } catch {
    throw new Error('请输入有效的 HTTPS 服务地址。')
  }
  if (
    url.protocol !== 'https:' ||
    url.username ||
    url.password ||
    url.search ||
    url.hash ||
    url.hostname === 'localhost' ||
    url.hostname.endsWith('.localhost') ||
    url.hostname.includes(':') ||
    /^\d+\.\d+\.\d+\.\d+$/.test(url.hostname)
  )
    throw new Error('请使用公网 HTTPS 服务地址，不要包含账号、查询参数或本机地址。')
  const base_path = url.pathname.replace(/\/$/, '').replace(/\/v1$/, '') || '/'
  if (
    url.pathname.includes('//') ||
    !/^[a-zA-Z0-9/_.~-]+$/.test(url.pathname) ||
    /\/(responses|messages)\/?$/.test(url.pathname)
  )
    throw new Error('请填写 API 基础地址，例如 https://api.example.com/v1。')
  if (url.hostname.length > 253 || base_path.length > 2048) throw new Error('服务地址过长。')
  return { scheme: 'https', host: url.hostname, port: Number(url.port || 443), base_path }
}
export function endpointUrl(endpoint: ModelEndpoint): string {
  return `https://${endpoint.host}${endpoint.port === 443 ? '' : `:${endpoint.port}`}${endpoint.base_path}`
}
export function validEndpoint(value: unknown): value is ModelEndpoint {
  if (!value || typeof value !== 'object' || Array.isArray(value)) return false
  const endpoint = value as ModelEndpoint
  if (
    Object.keys(endpoint).sort().join(',') !== 'base_path,host,port,scheme' ||
    typeof endpoint.host !== 'string' ||
    typeof endpoint.base_path !== 'string' ||
    endpoint.scheme !== 'https' ||
    !Number.isInteger(endpoint.port) ||
    endpoint.port < 1 ||
    endpoint.port > 65535
  )
    return false
  try {
    return (
      JSON.stringify(modelEndpoint(endpointUrl(endpoint))) ===
      JSON.stringify({
        scheme: endpoint.scheme,
        host: endpoint.host,
        port: endpoint.port,
        base_path: endpoint.base_path,
      })
    )
  } catch {
    return false
  }
}
export function validProtocol(value: unknown): value is ModelProtocol {
  return value === 'open_ai_responses' || value === 'anthropic_messages'
}
