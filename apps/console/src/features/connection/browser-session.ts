export interface BrowserSessionState {
  schema_version: 1
  authentication: 'local_owner' | 'bearer'
  setup_required: boolean
  authenticated: boolean
  display_name: string | null
  expires_at: string | null
}
const descriptions: Record<string, string> = {
  invalid_credentials: '邮箱或密码不正确。',
  owner_already_configured: '管理员已创建，请使用已有账号登录。',
  login_temporarily_locked: '尝试次数过多，请五分钟后再试。',
  capacity_exhausted: '登录服务繁忙，请稍后重试。',
  invalid_input: '请填写有效邮箱，密码需为 12–256 个字符。',
  origin_rejected: '当前访问地址与安装地址不一致，请使用本机控制台地址。',
}
export async function browserAuth(
  action: 'session' | 'setup' | 'login' | 'logout',
  input?: Record<string, unknown>,
  signal?: AbortSignal,
): Promise<unknown> {
  const response = await fetch(`/_console/v1/auth/${action}`, {
    method: action === 'session' ? 'GET' : 'POST',
    credentials: 'same-origin',
    cache: 'no-store',
    redirect: 'error',
    headers: input ? { 'Content-Type': 'application/json' } : undefined,
    body: input ? JSON.stringify(input) : undefined,
    signal,
  })
  const expected = action === 'setup' ? 201 : action === 'logout' ? 204 : 200
  let value: Record<string, unknown> = {}
  if (response.status !== 204) {
    const reader = response.body?.getReader()
    const chunks: Uint8Array[] = []
    let size = 0
    if (reader)
      for (;;) {
        const part = await reader.read()
        if (part.done) break
        size += part.value.length
        if (size > 4096) {
          await reader.cancel()
          throw new Error('登录服务返回了无法识别的响应。')
        }
        chunks.push(part.value)
      }
    try {
      value = JSON.parse(await new Blob(chunks as BlobPart[]).text())
    } catch {
      throw new Error('无法读取登录服务，请确认服务已启动后重试。')
    }
  }
  if (response.status !== expected)
    throw new Error(descriptions[String(value.code)] ?? '暂时无法连接登录服务，请稍后重试。')
  return value
}
export function checkedBrowserSession(value: unknown): BrowserSessionState {
  const input = value as BrowserSessionState
  if (
    !input ||
    typeof input !== 'object' ||
    input.schema_version !== 1 ||
    !['local_owner', 'bearer'].includes(input.authentication) ||
    typeof input.setup_required !== 'boolean' ||
    typeof input.authenticated !== 'boolean' ||
    (input.display_name !== null &&
      (typeof input.display_name !== 'string' || input.display_name.length > 128)) ||
    (input.expires_at !== null &&
      (typeof input.expires_at !== 'string' || !Number.isFinite(Date.parse(input.expires_at)))) ||
    (input.authenticated && (input.expires_at === null || input.setup_required))
  )
    throw new Error('登录服务返回了无法识别的会话。')
  return input
}
