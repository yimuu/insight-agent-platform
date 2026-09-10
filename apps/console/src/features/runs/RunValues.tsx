import { displayState } from '../../shared/i18n/display'
import sharedStyles from '../../shared/ui/Primitives.module.css'
import { classNames } from '../../shared/ui/class-names.ts'
import localStyles from './Runs.module.css'
const cx = classNames(sharedStyles, localStyles)
import { useEffect, useRef, useState } from 'react'
import { PlatformClient, PlatformProblem } from '../../shared/api/client'
import type { JsonObject, RunValueMetadata } from '../../shared/api/types'
import { safeJson } from '../../shared/api/security'
import { AuthorizedContent } from './AuthorizedContent'

/** Caller keys this component by session and Run; each content request is explicit. */
export function RunValues({ client, runId }: { client: PlatformClient; runId: string }) {
  const [node, setNode] = useState('')
  const [items, setItems] = useState<RunValueMetadata[]>([])
  const [cursor, setCursor] = useState<string | null>(null)
  const [content, setContent] = useState<JsonObject | null>(null)
  const [loaded, setLoaded] = useState(false)
  const [message, setMessage] = useState('')
  const [busy, setBusy] = useState(false)
  const pending = useRef<AbortController | null>(null)
  useEffect(() => () => pending.current?.abort(), [client, runId])
  const clear = () => {
    pending.current?.abort()
    setItems([])
    setCursor(null)
    setContent(null)
    setLoaded(false)
    setBusy(false)
    setMessage('')
  }
  const begin = () => {
    pending.current?.abort()
    const controller = new AbortController()
    pending.current = controller
    setBusy(true)
    setMessage('')
    return controller.signal
  }
  const failed = (error: unknown, signal: AbortSignal) => {
    if (signal.aborted) return
    if (error instanceof PlatformProblem && [401, 403].includes(error.status)) {
      setItems([])
      setCursor(null)
      setContent(null)
      setLoaded(false)
    }
    setMessage(error instanceof Error ? error.message : 'Run value read failed.')
  }
  const list = async (next?: string) => {
    const signal = begin()
    setContent(null)
    try {
      const page = await client.listRunValues(
        runId,
        { nodeId: node.trim() || undefined, cursor: next },
        { signal },
      )
      if (signal.aborted) return
      if (
        page.data.schema_version !== 1 ||
        !Array.isArray(page.data.items) ||
        page.data.items.length > 25 ||
        page.data.items.some(
          (item) =>
            item.schema_version !== 1 ||
            item.run_id !== runId ||
            (node.trim() && item.node_id !== node.trim()),
        )
      )
        throw new Error('Run value metadata does not match this selection.')
      setItems(page.data.items)
      setCursor(page.data.next_cursor)
      setLoaded(true)
    } catch (error) {
      failed(error, signal)
    } finally {
      if (!signal.aborted) setBusy(false)
    }
  }
  const read = async (item: RunValueMetadata) => {
    const signal = begin()
    setContent(null)
    try {
      const result = await client.getRunValueContent(runId, item.value_id, { signal })
      if (signal.aborted) return
      if (
        result.data.run_id !== runId ||
        result.data.value_id !== item.value_id ||
        result.data.schema_digest !== item.schema_digest ||
        result.data.content_digest !== item.content_digest ||
        result.data.classification !== item.classification
      )
        throw new Error('Run value content does not match the selected metadata.')
      setContent(result.data)
    } catch (error) {
      failed(error, signal)
    } finally {
      if (!signal.aborted) setBusy(false)
    }
  }
  return (
    <article data-ui="panel run-values" className={cx('panel run-values')}>
      <h2>运行中间值</h2>
      <p className={cx('body-copy')}>
        先查询中间值，再选择需要读取的内容。服务端会检查内容访问权限。
      </p>
      <label>
        <span>节点执行 ID</span>
        <input
          value={node}
          maxLength={64}
          placeholder="全部节点"
          onChange={(event) => {
            clear()
            setNode(event.target.value)
          }}
        />
      </label>
      <div className={cx('actions')}>
        <button className={cx('button')} disabled={busy} onClick={() => list()}>
          查询运行值
        </button>
        {cursor && (
          <button className={cx('button')} disabled={busy} onClick={() => list(cursor)}>
            下一页运行值
          </button>
        )}
      </div>
      {loaded && !items.length && (
        <p role="status">{cursor ? '本页没有运行值，可继续查看下一页。' : '本页没有运行值。'}</p>
      )}
      <div className={cx('stack')}>
        {items.map((item) => (
          <div data-ui="nested-panel" className={cx('nested-panel')} key={item.value_id}>
            <strong>{item.value_id}</strong>
            <p>
              {displayState(item.classification)} · {item.storage_kind} ·{' '}
              {item.node_id ?? '运行输入或结果'}
            </p>
            <details>
              <summary>Schema 与内容摘要</summary>
              <pre>{safeJson(item)}</pre>
            </details>
            <button className={cx('button')} disabled={busy} onClick={() => read(item)}>
              读取内容 {item.value_id}
            </button>
          </div>
        ))}
      </div>
      <p role="status" aria-live="polite">
        {busy ? '正在读取运行值…' : message}
      </p>
      {content && (
        <div>
          <div className={cx('actions')}>
            <h3>所选值的内容</h3>
            <button className={cx('button')} onClick={() => setContent(null)}>
              清除显示内容
            </button>
          </div>
          <AuthorizedContent
            client={client}
            content={content}
            onError={(error) => {
              setContent(null)
              setMessage(
                error instanceof Error ? error.message : 'Artifact content is unavailable.',
              )
            }}
          />
        </div>
      )}
    </article>
  )
}
