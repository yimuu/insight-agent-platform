import { AgentCompilerError } from '../compiler/compiler'
import { PlatformProblem } from '../api/client'

export type Notice = {
  tone: 'error' | 'success' | 'info'
  text: string
  traceId?: string | null
  detail?: string
}
export function errorNotice(error: unknown): Notice {
  if (error instanceof AgentCompilerError)
    return {
      tone: 'error',
      text: '配置未通过编译，请检查源码、字段约束和依赖绑定。',
      detail: `${error.code}: ${error.message}`,
    }
  if (error instanceof PlatformProblem) {
    if (error.status === 401)
      return {
        tone: 'error',
        text: '会话无效或已过期，请使用新的访问令牌重新连接。',
        traceId: error.traceId,
      }
    if (error.status === 403)
      return {
        tone: 'error',
        text: '当前账户无权执行此操作，请联系工作空间管理员。',
        traceId: error.traceId,
      }
    const actions: Record<string, string> = {
      authentication_required: '请重新连接。',
      permission_denied: '请向管理员申请所需权限。',
      precondition_failed: '配置已更新，请重新加载并比较服务端版本。',
      etag_mismatch: '配置已更新，请重新加载并比较服务端版本。',
      idempotency_conflict: '请保留恢复记录并恢复原始请求内容。',
      capacity_exhausted: '当前容量不足，请稍后重试同一操作。',
      cursor_expired: '请从首页重新加载列表。',
      cursor_invalid: '请从首页重新加载列表。',
    }
    const action =
      actions[error.code] ??
      (error.retryable
        ? '请求暂未完成，请稍后重试同一操作。'
        : '请求未完成，请查看高级诊断或联系管理员。')
    return { tone: 'error', text: `${action}（${error.code}）`, traceId: error.traceId }
  }
  const message = error instanceof Error ? error.message : ''
  return /[\u4e00-\u9fff]/.test(message)
    ? { tone: 'error', text: message }
    : {
        tone: 'error',
        text: '操作未完成，请检查配置或查看诊断详情。',
        detail: message || undefined,
      }
}

export function formatTime(value: string | null): string {
  return value ? new Date(value).toLocaleString('zh-CN') : '—'
}
