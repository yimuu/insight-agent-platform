import { WorkflowCanvas, nodeLabel } from './WorkflowCanvas'
import sharedStyles from '../../shared/ui/Primitives.module.css'
import { classNames } from '../../shared/ui/class-names.ts'
import localStyles from './Agents.module.css'
const cx = classNames(sharedStyles, localStyles)
import { useEffect, useLayoutEffect, useMemo, useRef, useState } from 'react'
import descriptorText from '../../../../../contracts/platform-v1/agent-node-editor.v1.json?raw'
import { SchemaTree } from '../../shared/schema/SchemaTree'
import { object, pointer } from '../../shared/schema/tree'
import type { Json } from '../../shared/api/types'
import {
  authoredExpressions,
  draftFields,
  readNodeEditorDescriptor,
  referencedPorts,
  replaceAt,
  sourceLocations,
} from './plan-editor'
import type { CompiledAgent } from '../../shared/compiler/compiler'
import { digestJson, rebuildExpression } from '../../shared/compiler/compiler'

const descriptor = readNodeEditorDescriptor(descriptorText)
export function PlanEditor({
  source,
  onChange,
  compiled,
  disabled,
}: {
  source: string
  onChange(source: string): void
  compiled: CompiledAgent | null
  disabled: boolean
}) {
  const [selected, setSelected] = useState('')
  const [newId, setNewId] = useState(`node-${crypto.randomUUID().slice(0, 8)}`)
  const [newKind, setNewKind] = useState(descriptor.nodes[0].kind)
  const [error, setError] = useState('')
  const [building, setBuilding] = useState(false)
  const pending = useRef<AbortController | null>(null)
  const revision = useRef(source)
  useLayoutEffect(() => {
    revision.current = source
  }, [source])
  useEffect(() => () => pending.current?.abort(), [])
  const plan = useMemo(() => {
    if (new TextEncoder().encode(source).length > 1_048_576) return null
    try {
      const json: unknown = JSON.parse(source)
      return object(json) && object(json.nodes) ? json : null
    } catch {
      return null
    }
  }, [source])
  if (!plan || !object(plan.nodes))
    return (
      <p className={cx('body-copy')}>
        提供完整 Plan 或静态框架 JSON 后，可以编辑其中的 Platform 节点。
      </p>
    )
  const nodes = plan.nodes
  const nodeIds = Object.keys(nodes)
  const id = Object.hasOwn(nodes, selected) ? selected : nodeIds[0]
  const value = nodes[id]
  const template = object(value)
    ? descriptor.nodes.find((node) => node.kind === value.kind)?.template
    : undefined
  const locations = sourceLocations(compiled?.sourceMap, id)
  const ports = referencedPorts(value ?? null)
  const expressions = authoredExpressions(value ?? null)
  const availablePorts = [
    ...new Map(
      referencedPorts(nodes).map((entry) => [JSON.stringify(entry.value), entry.value]),
    ).values(),
  ]
  const update = (next: Json) => {
    const encoded = JSON.stringify(next, null, 2)
    if (new TextEncoder().encode(encoded).length > 1_048_576) {
      setError('Plan 源码超过 1 MiB 编辑限制。')
      return
    }
    setError('')
    onChange(encoded)
  }
  const suggestions = (path: string) => {
    const field = path.split('/').at(-1) ?? ''
    if (
      [
        'next',
        'resume',
        'body',
        'exit',
        'target',
        'otherwise',
        'join',
        'entry_node_id',
        'producer_node_id',
      ].includes(field) ||
      path.includes('/legs/') ||
      path.includes('/handlers/')
    )
      return nodeIds
    if (field.endsWith('schema_digest') || field === 'schema_digest')
      return object(plan.schema_documents) ? Object.keys(plan.schema_documents) : []
    if (field.endsWith('_slot_id') || path.includes('_slot_ids/'))
      return object(plan.dependency_slots) ? Object.keys(plan.dependency_slots) : []
    return []
  }
  const build = async (entry: (typeof expressions)[number]) => {
    if (building || disabled) return
    const controller = new AbortController()
    pending.current = controller
    const snapshot = source
    setBuilding(true)
    setError('')
    try {
      const expression = structuredClone(entry.value)
      if (Array.isArray(expression.instructions))
        expression.instructions = await Promise.all(
          expression.instructions.map(async (instruction) => {
            if (
              object(instruction) &&
              instruction.op === 'literal' &&
              object(instruction.value) &&
              Object.hasOwn(instruction.value, 'value')
            )
              return {
                ...instruction,
                value: {
                  ...instruction.value,
                  canonical_digest: await digestJson(instruction.value.value),
                },
              }
            return instruction
          }),
        )
      const rebuilt = await rebuildExpression(expression, controller.signal)
      if (revision.current !== snapshot) throw new Error('重建表达式期间源码发生变化，请重新操作。')
      if (!controller.signal.aborted)
        update({ ...plan, nodes: { ...nodes, [id]: replaceAt(value, entry.keys, rebuilt) } })
    } catch (error) {
      if (!controller.signal.aborted)
        setError(error instanceof Error ? error.message : '表达式重建失败。')
    } finally {
      if (!controller.signal.aborted) setBuilding(false)
    }
  }
  return (
    <details data-ui="nested-panel" className={cx('nested-panel')} open>
      <summary>工作流</summary>
      <WorkflowCanvas
        nodes={nodes}
        entry={String(plan.entry_node_id ?? '')}
        selected={id}
        onSelect={setSelected}
      />
      <div className={cx('workflow-toolbar')} data-editor-view>
        <label>
          <span>添加步骤</span>
          <select
            value={newKind}
            onChange={(event) => setNewKind(event.target.value)}
            disabled={disabled}
          >
            {descriptor.nodes.map((node) => (
              <option key={node.kind} value={node.kind}>
                {nodeLabel(node.kind)}
              </option>
            ))}
          </select>
        </label>
        <button
          type="button"
          className={cx('button')}
          disabled={
            disabled ||
            !/^[A-Za-z0-9][A-Za-z0-9_.:-]{0,127}$/.test(newId) ||
            Object.hasOwn(nodes, newId)
          }
          onClick={() => {
            update({
              ...plan,
              nodes: {
                ...nodes,
                [newId]: structuredClone(
                  descriptor.nodes.find((n) => n.kind === newKind)!.template,
                ),
              },
            })
            setSelected(newId)
            setNewId(`node-${crypto.randomUUID().slice(0, 8)}`)
          }}
        >
          添加节点
        </button>
      </div>
      <label>
        <span>入口节点</span>
        <select
          disabled={disabled}
          value={String(plan.entry_node_id ?? '')}
          onChange={(event) => update({ ...plan, entry_node_id: event.target.value })}
        >
          {!nodeIds.includes(String(plan.entry_node_id)) && (
            <option value={String(plan.entry_node_id ?? '')}>
              {String(plan.entry_node_id ?? 'Select entry')}
            </option>
          )}
          {nodeIds.map((key) => (
            <option key={key}>{key}</option>
          ))}
        </select>
      </label>
      <details open>
        <summary>节点配置 · {object(value) ? nodeLabel(String(value.kind)) : id}</summary>
        {id && template && (
          <SchemaTree
            key={id}
            node={draftFields(descriptor, template, value)}
            value={value}
            onChange={(next) => update({ ...plan, nodes: { ...nodes, [id]: next } })}
            label={`节点 ${id}`}
            path={pointer('/nodes', id)}
            disabled={disabled}
            suggestions={suggestions}
          />
        )}
      </details>
      {ports.length > 0 && (
        <details>
          <summary>复用精确端口引用</summary>
          <p className={cx('body-copy')}>
            选择源码中已声明的引用。编译器负责检查作用域、Schema 身份和数据流。
          </p>
          {ports.slice(0, 128).map((port) => (
            <label key={JSON.stringify(port.keys)}>
              <span>端口 {port.keys.join('/')}</span>
              <select
                value=""
                disabled={disabled}
                onChange={(event) =>
                  update({
                    ...plan,
                    nodes: {
                      ...nodes,
                      [id]: replaceAt(
                        value,
                        port.keys,
                        structuredClone(availablePorts[Number(event.target.value)]),
                      ),
                    },
                  })
                }
              >
                <option value="" disabled>
                  选择已有精确引用
                </option>
                {availablePorts.map((candidate, index) => (
                  <option key={index} value={index}>
                    {candidate.source === 'run_input'
                      ? 'Run input'
                      : `${candidate.producer_node_id}/${candidate.port_id}`}{' '}
                    · {String(candidate.schema_digest)}
                  </option>
                ))}
              </select>
            </label>
          ))}
        </details>
      )}
      {expressions.length > 0 && (
        <div>
          <p className={cx('body-copy')}>修改表达式后，先重建栈深度与摘要，再校验完整 Plan。</p>
          <div className={cx('actions')}>
            {expressions.map((entry) => (
              <button
                type="button"
                className={cx('button')}
                key={JSON.stringify(entry.keys)}
                disabled={disabled || building}
                onClick={() => void build(entry)}
              >
                {building ? '正在重建表达式…' : `重建表达式 ${entry.keys.join('/')}`}
              </button>
            ))}
          </div>
        </div>
      )}
      {id && !template && (
        <p data-ui="notice--error" className={cx('notice notice--error')}>
          此节点类型没有编辑描述，原始字段保留在源码中，供编译器诊断。
        </p>
      )}
      {id && (
        <button
          type="button"
          className={cx('button')}
          disabled={disabled}
          onClick={() => {
            update({
              ...plan,
              nodes: Object.fromEntries(Object.entries(nodes).filter(([key]) => key !== id)),
            })
            setSelected('')
          }}
        >
          删除节点 {id}
        </button>
      )}
      <details>
        <summary>依赖槽与 Schema 文档</summary>
        {['dependency_slots', 'schema_documents'].map((field) => (
          <SchemaTree
            key={field}
            node={draftFields(descriptor, {}, plan[field] ?? {}, field)}
            value={plan[field] ?? {}}
            onChange={(next) => update({ ...plan, [field]: next })}
            label={field}
            path={`/${field}`}
            disabled={disabled}
            suggestions={suggestions}
          />
        ))}
      </details>
      {locations.length > 0 && (
        <details>
          <summary>编译源码位置： {id}</summary>
          <ul>
            {locations.map((location, index) => (
              <li key={index}>
                <code>
                  {location.file}:{location.line}:{location.column}
                </code>{' '}
                · {location.kind} · <code>{location.source_pointer || '/'}</code>
              </li>
            ))}
          </ul>
        </details>
      )}
      {!compiled && <p className={cx('body-copy muted')}>校验当前源码以更新位置映射。</p>}
      {error && (
        <p data-ui="notice--error" className={cx('notice notice--error')} role="alert">
          {error}
        </p>
      )}
    </details>
  )
}
