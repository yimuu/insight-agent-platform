import { PlatformClient } from '../../shared/api/client.ts'

export interface ConsoleSession {
  key: string
  client: PlatformClient
  expiresAt: number | null
}

/** Expiry is a UX hint only. The Gateway still verifies identity and permissions. */
export function tokenExpiry(token: string): number | null {
  try {
    const payload: unknown = JSON.parse(
      atob(token.split('.')[1].replace(/-/g, '+').replace(/_/g, '/')),
    )
    if (
      payload &&
      typeof payload === 'object' &&
      'exp' in payload &&
      typeof payload.exp === 'number' &&
      Number.isSafeInteger(payload.exp) &&
      payload.exp > 0
    )
      return payload.exp * 1000
  } catch {
    /* Opaque access tokens have no local expiry hint. */
  }
  return null
}

export function readTokenFile(text: string): string {
  if (new TextEncoder().encode(text).byteLength > 32_768)
    throw new Error('令牌文件超过 32 KiB 限制。')
  let token = text.trim()
  if (token.startsWith('{')) {
    const value: unknown = JSON.parse(token)
    if (
      !value ||
      typeof value !== 'object' ||
      !('access_token' in value) ||
      typeof value.access_token !== 'string'
    ) {
      throw new Error(
        '请选择令牌文件，或包含 access_token 的 JSON 文件。会话描述文件中的路径不能直接用于登录。',
      )
    }
    token = value.access_token.trim()
  }
  if (!token || /\s/.test(token)) throw new Error('令牌不能为空或包含空白字符。')
  return token
}
