import { useEffect, useMemo, useRef, useState } from 'react'
import type { PlatformClient } from '../../shared/api/client'
import type { RunEvent } from '../../shared/api/types'
import { displayState } from '../../shared/i18n/display'
import { ExecutionValue } from './ExecutionValue'
import { useExecutionDetails } from './use-execution-details'
import {
  executionDuration,
  executionLabels,
  executionNodes,
  executionTone,
} from './execution-graph'
import styles from './ExecutionCanvas.module.css'

export function ExecutionCanvas({
  events,
  client,
  runId,
}: {
  events: RunEvent[]
  client?: PlatformClient | null
  runId?: string
}) {
  const scopedEvents = useMemo(
    () => events.filter((event) => Boolean(runId) && event.data.run_id === runId),
    [events, runId],
  )
  const nodes = useMemo(() => executionNodes(scopedEvents), [scopedEvents])
  const [selected, setSelected] = useState('')
  const [filter, setFilter] = useState('')
  const [zoom, setZoom] = useState(1)
  const [columns, setColumns] = useState(2)
  const viewport = useRef<HTMLDivElement>(null)
  const terminalEpoch = scopedEvents
    .filter((event) => /^run\.(completed|failed|cancelled|timed_out)$/.test(event.event))
    .map((event) => String(event.data.sequence))
    .join(',')
  const { details, notice, retry } = useExecutionDetails(
    client,
    runId,
    nodes,
    selected,
    terminalEpoch,
  )
  useEffect(() => {
    const element = viewport.current
    if (!element) return
    const observer = new ResizeObserver(() =>
      setColumns(Math.max(1, Math.min(3, Math.floor((element.clientWidth - 32) / 264)))),
    )
    observer.observe(element)
    return () => observer.disconnect()
  }, [])
  const drag = useRef<{ x: number; y: number; left: number; top: number } | null>(null)
  const tone = (item: (typeof nodes)[number]) =>
    details[item.id]
      ? ({ succeeded: 'success', failed: 'error', timed_out: 'error', cancelled: 'waiting' }[
          details[item.id]!.state
        ] ?? 'active')
      : executionTone(item.latest.event)
  const stateLabel = (item: (typeof nodes)[number]) =>
    displayState(details[item.id]?.state ?? item.latest.event)
  const duration = (item: (typeof nodes)[number]) => {
    const value = details[item.id]
    return value?.started_at && value.terminal_at
      ? `${Math.max(0, (Date.parse(value.terminal_at) - Date.parse(value.started_at)) / 1000).toFixed(1)} 秒`
      : executionDuration(item)
  }
  const visible = nodes.filter((node) => !filter || tone(node) === 'error')
  const node = nodes.find((item) => item.id === selected)
  const detail = node ? details[node.id] : undefined
  const failures = nodes.filter((item) => tone(item) === 'error').length
  const width = columns * 264 + 32
  const height = Math.max(240, Math.ceil(visible.length / columns) * 152 + 32)
  return (
    <section className={styles.shell} aria-label="执行画布">
      <header className={styles.toolbar}>
        <div>
          <strong>执行画布</strong>
          <span>{nodes.length} 个执行对象</span>
        </div>
        <div>
          <button
            aria-pressed={filter === 'error'}
            onClick={() => setFilter(filter ? '' : 'error')}
          >
            仅看异常{failures ? ` · ${failures}` : ''}
          </button>
          <button
            aria-label="缩小画布"
            disabled={zoom <= 0.5}
            onClick={() => setZoom(Math.max(0.5, zoom - 0.1))}
          >
            −
          </button>
          <button
            title="重置画布"
            onClick={() => {
              setZoom(1)
              viewport.current?.scrollTo(0, 0)
            }}
          >
            {Math.round(zoom * 100)}%
          </button>
          <button
            aria-label="放大画布"
            disabled={zoom >= 1.5}
            onClick={() => setZoom(Math.min(1.5, zoom + 0.1))}
          >
            ＋
          </button>
        </div>
      </header>
      {notice && (
        <p role="status" className={styles.notice}>
          {notice} <button onClick={retry}>重试详情</button>
        </p>
      )}
      <div className={styles.workspace}>
        <div
          className={styles.viewport}
          ref={viewport}
          onPointerDown={(event) => {
            if (event.button !== 0 || (event.target as HTMLElement).closest('button')) return
            const target = event.currentTarget
            drag.current = {
              x: event.clientX,
              y: event.clientY,
              left: target.scrollLeft,
              top: target.scrollTop,
            }
            target.setPointerCapture(event.pointerId)
          }}
          onPointerMove={(event) => {
            if (!drag.current) return
            event.currentTarget.scrollLeft = drag.current.left + drag.current.x - event.clientX
            event.currentTarget.scrollTop = drag.current.top + drag.current.y - event.clientY
          }}
          onPointerUp={() => {
            drag.current = null
          }}
          onPointerCancel={() => {
            drag.current = null
          }}
        >
          {visible.length === 0 ? (
            <div className={styles.empty}>
              <strong>{filter ? '没有异常执行对象' : '等待执行节点'}</strong>
              <p>
                {filter
                  ? '切换回全部查看执行过程。'
                  : '模型、工具和节点的执行事件到达后会自动显示。'}
              </p>
            </div>
          ) : (
            <div style={{ width: width * zoom, height: height * zoom }}>
              <div className={styles.stage} style={{ width, height, transform: `scale(${zoom})` }}>
                {visible.map((item, index) => (
                  <button
                    key={item.id}
                    className={styles.node}
                    data-tone={tone(item)}
                    aria-pressed={item.id === selected}
                    onClick={() => setSelected(item.id)}
                    style={{
                      left: 16 + (index % columns) * 264,
                      top: 16 + Math.floor(index / columns) * 152,
                    }}
                  >
                    <span className={styles.nodeTitle}>
                      <span className={styles.dot} />
                      {executionLabels[item.kind]}
                      <small>#{nodes.indexOf(item) + 1}</small>
                    </span>
                    <strong>{details[item.id]?.plan_node_key ?? stateLabel(item)}</strong>
                    <span className={styles.summary}>
                      {item.summary ||
                        `${stateLabel(item)} · ${details[item.id]?.node_kind ?? executionLabels[item.kind]}`}
                    </span>
                    <span className={styles.meta}>
                      {details[item.id]
                        ? `${details[item.id]!.values.length} 项关联数据`
                        : `${item.events.length} 次状态更新`}
                      <span>{duration(item)}</span>
                    </span>
                  </button>
                ))}
              </div>
            </div>
          )}
        </div>
        {node && (
          <aside className={styles.inspector} aria-label="执行对象详情">
            <header>
              <div>
                <small>{executionLabels[node.kind]}</small>
                <h3>
                  {detail?.plan_node_key ?? executionLabels[node.kind]} · {stateLabel(node)}
                </h3>
              </div>
              <button aria-label="关闭执行详情" onClick={() => setSelected('')}>
                ×
              </button>
            </header>
            {node.summary && <p>{node.summary}</p>}
            <dl>
              <dt>执行耗时</dt>
              <dd>{duration(node)}</dd>
            </dl>
            {detail && client && (
              <ExecutionValue
                key={`${detail.source_id}:${detail.version}`}
                client={client}
                detail={detail}
              />
            )}
            {!detail && <p>正在读取节点详情；其它类型的执行对象目前仅提供状态记录。</p>}
            <details>
              <summary>状态记录（{node.events.length}）</summary>
              <ol>
                {node.events.map((event) => (
                  <li key={String(event.data.event_id)}>
                    <strong>{displayState(event.event)}</strong>
                    <time>{new Date(String(event.data.occurred_at)).toLocaleTimeString()}</time>
                    <p>{String((event.data.data as Record<string, unknown>).safe_summary ?? '')}</p>
                  </li>
                ))}
              </ol>
            </details>
            <details>
              <summary>诊断标识</summary>
              <p className={styles.identifier}>{node.sourceId}</p>
              <p className={styles.identifier}>
                Run：{runId ?? String(node.latest.data.run_id ?? '')}
              </p>
            </details>
          </aside>
        )}
      </div>
      <footer>点击节点查看其关联数据 · 节点与模型状态来自数据库 · 排列顺序不代表依赖关系</footer>
    </section>
  )
}
