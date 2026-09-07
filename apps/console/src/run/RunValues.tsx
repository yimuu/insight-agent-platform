import { useEffect, useRef, useState } from 'react'
import { PlatformClient, PlatformProblem } from '../api/client'
import type { JsonObject, RunValueMetadata } from '../api/types'
import { safeJson } from '../api/security'
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
  const clear = () => { pending.current?.abort(); setItems([]); setCursor(null); setContent(null); setLoaded(false); setBusy(false); setMessage('') }
  const begin = () => { pending.current?.abort(); const controller = new AbortController(); pending.current = controller; setBusy(true); setMessage(''); return controller.signal }
  const failed = (error: unknown, signal: AbortSignal) => {
    if (signal.aborted) return
    if (error instanceof PlatformProblem && [401, 403].includes(error.status)) { setItems([]); setCursor(null); setContent(null); setLoaded(false) }
    setMessage(error instanceof Error ? error.message : 'Run value read failed.')
  }
  const list = async (next?: string) => {
    const signal = begin(); setContent(null)
    try {
      const page = await client.listRunValues(runId, { nodeId: node.trim() || undefined, cursor: next }, { signal })
      if (signal.aborted) return
      if (page.data.schema_version !== 1 || !Array.isArray(page.data.items) || page.data.items.length > 25 || page.data.items.some((item) => item.schema_version !== 1 || item.run_id !== runId || (node.trim() && item.node_id !== node.trim()))) throw new Error('Run value metadata does not match this selection.')
      setItems(page.data.items); setCursor(page.data.next_cursor); setLoaded(true)
    } catch (error) { failed(error, signal) } finally { if (!signal.aborted) setBusy(false) }
  }
  const read = async (item: RunValueMetadata) => {
    const signal = begin(); setContent(null)
    try {
      const result = await client.getRunValueContent(runId, item.value_id, { signal })
      if (signal.aborted) return
      if (result.data.run_id !== runId || result.data.value_id !== item.value_id || result.data.schema_digest !== item.schema_digest || result.data.content_digest !== item.content_digest || result.data.classification !== item.classification) throw new Error('Run value content does not match the selected metadata.')
      setContent(result.data)
    } catch (error) { failed(error, signal) } finally { if (!signal.aborted) setBusy(false) }
  }
  return <article className="panel run-values"><h2>Run values</h2><p className="body-copy">List value metadata, then explicitly read an individual value. Content access is checked by the server.</p>
    <label><span>Value Node execution ID</span><input value={node} maxLength={64} placeholder="All nodes" onChange={(event) => { clear(); setNode(event.target.value) }} /></label>
    <div className="actions"><button className="button" disabled={busy} onClick={() => list()}>List Run values</button>{cursor && <button className="button" disabled={busy} onClick={() => list(cursor)}>Next value page</button>}</div>
    {loaded && !items.length && <p role="status">{cursor ? 'No values in this page. Continue with the next page.' : 'No values in this page.'}</p>}
    <div className="stack">{items.map((item) => <div className="nested-panel" key={item.value_id}><strong>{item.value_id}</strong><p>{item.classification} · {item.storage_kind} · {item.node_id ?? 'Run input or result'}</p><details><summary>Schema and content digests</summary><pre>{safeJson(item)}</pre></details><button className="button" disabled={busy} onClick={() => read(item)}>Read content {item.value_id}</button></div>)}</div>
    <p role="status" aria-live="polite">{busy ? 'Reading Run values…' : message}</p>
    {content && <div><div className="actions"><h3>Selected value content</h3><button className="button" onClick={() => setContent(null)}>Clear value content</button></div><AuthorizedContent client={client} content={content} onError={(error) => { setContent(null); setMessage(error instanceof Error ? error.message : 'Artifact content is unavailable.') }} /></div>}
  </article>
}
