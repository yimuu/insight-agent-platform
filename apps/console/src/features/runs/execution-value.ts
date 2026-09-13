export interface ValueSection {
  label: string
  text: string
  collapsed?: boolean
}
const object = (value: unknown): Record<string, unknown> | null =>
  value !== null && typeof value === 'object' && !Array.isArray(value)
    ? (value as Record<string, unknown>)
    : null
const text = (value: unknown) =>
  typeof value === 'string' ? value : (JSON.stringify(value, null, 2) ?? '无内容')
function parts(value: unknown): string {
  return Array.isArray(value)
    ? value
        .map((part) => {
          const p = object(part)
          return p?.kind === 'text' && typeof p.value === 'string'
            ? p.value
            : '[非文本内容，请查看原始数据]'
        })
        .join('\n')
    : ''
}
/** Display actual value bodies; event envelopes never stand in for execution output. */
export function executionValueSections(value: unknown, model: boolean): ValueSection[] {
  const body = object(value)
  if (model && body && Array.isArray(body.messages)) {
    const roles: Record<string, string> = {
      platform: '系统指令',
      user: '用户输入',
      assistant: '历史助手回答',
      tool: '工具结果',
    }
    return body.messages.map((message) => {
      const m = object(message)
      return {
        label: roles[String(m?.role)] ?? '消息',
        text: parts(m?.parts),
        collapsed: m?.role === 'platform',
      }
    })
  }
  if (model && body && 'structured_output' in body) {
    const sections: ValueSection[] = []
    const structured = object(body.structured_output)
    if (structured && 'value' in structured)
      sections.push(...executionValueSections(structured.value, false))
    const message = object(body.message)
    if (message) sections.push({ label: '模型回答', text: parts(message.parts) })
    if (Array.isArray(body.tool_intents) && body.tool_intents.length)
      sections.push({ label: '工具调用请求', text: text(body.tool_intents) })
    const usage = object(body.usage)
    if (usage)
      sections.push({
        label: 'Token 用量',
        text: `输入 ${usage.input_tokens ?? '未报告'} · 输出 ${usage.output_tokens ?? '未报告'}`,
      })
    if (body.finish_reason)
      sections.push({
        label: '结束原因',
        text:
          body.finish_reason === 'completed'
            ? '正常结束'
            : body.finish_reason === 'tool_use'
              ? '请求工具调用'
              : String(body.finish_reason),
      })
    const observation = object(body.observation)
    if (typeof observation?.actual_model_identity === 'string')
      sections.push({ label: '实际模型', text: observation.actual_model_identity })
    return sections.length ? sections : [{ label: '结果', text: '没有文本回答或工具调用。' }]
  }
  if (body)
    return Object.entries(body).map(([key, value]) => ({
      label: key === 'answer' ? '回答' : key === 'message' ? '用户消息' : key,
      text: text(value),
    }))
  return [{ label: '内容', text: text(value) }]
}
