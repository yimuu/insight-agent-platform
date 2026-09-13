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
      object_storage_unavailable: '存储服务暂时不可用，请稍后重试保存。',
      object_upload_rejected: '配置上传未被接受，请检查存储权限或本次上传是否已过期。',
      object_transport_unconfigured: '控制台的存储连接尚未配置，请联系管理员完成部署。',
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
    return { tone: 'error', text: action, detail: error.code, traceId: error.traceId }
  }
  const message = error instanceof Error ? error.message : ''
  const known: Record<string, string> = {
    unexpected_response_status: '服务已响应，但返回结果与预期不一致。已保留保存进度，请重试继续。',
    artifact_upload_unreachable: '配置上传中断，请检查工作空间连接后重试保存。',
    artifact_upload_failed: '对象存储未接受配置文件，请检查存储容量和上传权限后重试。',
    credential_import_pending: 'API Key 尚未确认保存。请重新输入同一个 Key，然后重试。',
    model_configuration_conflict: '上次保存尚未完成或配置已被修改，请先继续原操作。',
    artifact_upload_conflict: '上传已过期或状态发生变化，请联系管理员核对本次操作。',
    model_validation_failed: '模型配置未通过校验，请检查服务与模型参数。',
  }
  const knownText = known[message.split(':')[0]]
  if (knownText)
    return { tone: 'error', text: knownText, detail: message.includes(':') ? message : undefined }
  if (error instanceof TypeError && /fetch|network|load failed/i.test(message))
    return { tone: 'error', text: '无法连接工作空间，请检查服务是否运行，然后重试。' }
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
