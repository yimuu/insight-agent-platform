import type { RunEvent } from '../../shared/api/types.ts'

export const executionLabels: Record<string, string> = {
  node_execution: '编排节点',
  model_turn: '模型调用',
  capability_invocation: '工具 / 能力',
  skill_activation: 'Skill',
  context_query: '上下文查询',
  child_run_link: '子任务',
  interaction: '用户输入',
  approval_task: '审批',
}
export interface ExecutionNode {
  id: string
  kind: string
  sourceId: string
  events: RunEvent[]
  latest: RunEvent
  summary: string
}
/** Events describe objects, not graph edges. Never infer causality from delivery order. */
export function executionNodes(events: RunEvent[]): ExecutionNode[] {
  const nodes = new Map<string, ExecutionNode>()
  const seen = new Set<string>()
  for (const event of events) {
    if (event.data.durability !== 'durable') continue
    const eventId = event.data.event_id
    if (typeof eventId !== 'string' || seen.has(eventId)) continue
    seen.add(eventId)
    const data = event.data.data
    if (!data || typeof data !== 'object' || Array.isArray(data)) continue
    const kind = (data as Record<string, unknown>).source_kind
    const sourceId = (data as Record<string, unknown>).source_id
    if (typeof kind !== 'string' || !executionLabels[kind] || typeof sourceId !== 'string') continue
    const id = `${kind}:${sourceId}`
    const existing = nodes.get(id)
    if (existing) existing.events.push(event)
    else nodes.set(id, { id, kind, sourceId, events: [event], latest: event, summary: '' })
  }
  for (const node of nodes.values()) {
    node.events.sort((a, b) => Number(a.data.sequence) - Number(b.data.sequence))
    node.latest = node.events[node.events.length - 1]!
    for (const event of node.events) {
      const data = event.data.data as Record<string, unknown>
      if (typeof data.safe_summary === 'string') node.summary = data.safe_summary
    }
  }
  return [...nodes.values()].sort(
    (a, b) => Number(a.events[0]!.data.sequence) - Number(b.events[0]!.data.sequence),
  )
}
export function executionTone(event: string): string {
  if (/\.(failed|rejected|timed_out)$/.test(event)) return 'error'
  if (/\.(completed|resolved|activated)$/.test(event)) return 'success'
  if (/\.(waiting|input_required|required|cancelled)$/.test(event)) return 'waiting'
  return 'active'
}
export function executionDuration(node: ExecutionNode): string {
  const start = node.events.find((event) => event.event.endsWith('.started'))
  const end = node.events.find((event) =>
    /\.(completed|failed|cancelled|timed_out)$/.test(event.event),
  )
  if (!start || !end) return '—'
  const milliseconds =
    Date.parse(String(end.data.occurred_at)) - Date.parse(String(start.data.occurred_at))
  return Number.isFinite(milliseconds) && milliseconds >= 0
    ? `${(milliseconds / 1000).toFixed(1)} 秒`
    : '—'
}
