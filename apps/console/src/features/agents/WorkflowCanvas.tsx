import { useId } from 'react'
import type { Json } from '../../shared/api/types'
import { object } from '../../shared/schema/tree'
import styles from './Agents.module.css'
export const NODE_NAMES: Record<string, string> = {
  start: '开始',
  compute: '转换数据',
  branch: '条件分支',
  fork: '并行分支',
  join: '合并',
  map: '遍历',
  loop: '循环',
  error_boundary: '错误处理',
  model_loop: '模型对话',
  capability_call: '调用工具',
  context_query: '检索知识',
  child_agent_call: '调用智能体',
  human_task: '人工处理',
  timer_wait: '等待时间',
  signal_wait: '等待事件',
  return: '输出结果',
  raise: '报告错误',
}
export function nodeLabel(kind: string): string {
  return NODE_NAMES[kind] ?? kind
}
export function workflowEdges(
  nodes: Record<string, Json>,
): { from: string; to: string; label: string }[] {
  const edges: { from: string; to: string; label: string }[] = []
  for (const [from, node] of Object.entries(nodes)) {
    if (!object(node)) continue
    const add = (to: Json | undefined, label: string) => {
      if (typeof to === 'string' && Object.hasOwn(nodes, to)) edges.push({ from, to, label })
    }
    for (const field of ['next', 'resume', 'body', 'exit', 'otherwise', 'join'])
      add(node[field], field)
    if (Array.isArray(node.ordered_arms))
      for (const arm of node.ordered_arms) if (object(arm)) add(arm.target, '分支')
    if (Array.isArray(node.legs))
      for (const [index, to] of node.legs.entries()) add(to, `并行 ${index + 1}`)
    if (object(node.handlers)) for (const [name, to] of Object.entries(node.handlers)) add(to, name)
  }
  return edges
}
export function WorkflowCanvas({
  nodes,
  entry,
  selected,
  onSelect,
}: {
  nodes: Record<string, Json>
  entry: string
  selected: string
  onSelect(id: string): void
}) {
  const markerId = useId()
  const edges = workflowEdges(nodes)
  const remaining = Object.keys(nodes)
  const order: string[] = []
  const visit = (id: string) => {
    if (!remaining.includes(id) || order.includes(id) || order.length >= 128) return
    order.push(id)
    for (const edge of edges.filter((e) => e.from === id)) visit(edge.to)
  }
  visit(entry)
  for (const id of remaining) visit(id)
  const columns = 3
  const positions = new Map(
    order.map((id, index) => [
      id,
      { x: 36 + (index % columns) * 250, y: 36 + Math.floor(index / columns) * 142 },
    ]),
  )
  const height = Math.max(260, Math.ceil(order.length / columns) * 142 + 32)
  return (
    <div className={styles['workflow-canvas']} aria-label="工作流画布">
      <div className={styles['workflow-surface']} style={{ width: 800, height }}>
        <svg width="800" height={height} aria-hidden="true" className={styles['workflow-wires']}>
          <defs>
            <marker
              id={markerId}
              viewBox="0 0 10 10"
              refX="8"
              refY="5"
              markerWidth="5"
              markerHeight="5"
              orient="auto-start-reverse"
            >
              <path d="M 0 0 L 10 5 L 0 10 z" fill="#94a3b8" />
            </marker>
          </defs>
          {edges.map((edge, index) => {
            const a = positions.get(edge.from),
              b = positions.get(edge.to)
            if (!a || !b) return null
            return (
              <path
                key={index}
                d={`M ${a.x + 96} ${a.y + 76} C ${a.x + 96} ${a.y + 114}, ${b.x + 96} ${b.y - 32}, ${b.x + 96} ${b.y}`}
                fill="none"
                stroke="#94a3b8"
                strokeWidth="2"
                markerEnd={`url(#${markerId})`}
              />
            )
          })}
        </svg>
        {order.map((id) => {
          const node = nodes[id],
            position = positions.get(id)!
          const kind = object(node) ? String(node.kind) : 'unknown'
          return (
            <button
              key={id}
              type="button"
              aria-pressed={selected === id}
              className={styles['workflow-node']}
              style={{ left: position.x, top: position.y }}
              onClick={() => onSelect(id)}
            >
              <span className={styles['workflow-node-kind']}>
                {nodeLabel(kind)}
                {entry === id ? ' · 入口' : ''}
              </span>
              <span>{id}</span>
              <i aria-hidden="true" />
            </button>
          )
        })}
      </div>
      {remaining.length > 128 && <p>画布显示前 128 个节点，完整内容可在源码中查看。</p>}
    </div>
  )
}
