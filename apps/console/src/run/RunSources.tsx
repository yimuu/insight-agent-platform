import { useEffect, useRef, useState } from 'react'
import type { PlatformClient } from '../api/client'
import type { RunView } from '../api/types'
import type { CompiledAgent } from '../agent/compiler'
import { frozenRunSources } from '../agent/published-run'
import { sourceLocations } from '../agent/plan-editor'

export function RunSources({ client, run }: { client: PlatformClient; run: RunView }) {
  const [compiled, setCompiled] = useState<CompiledAgent | null>(null)
  const [selected, setSelected] = useState('')
  const [error, setError] = useState('')
  const [busy, setBusy] = useState(false)
  const controller = useRef<AbortController | null>(null)
  useEffect(() => {
    let active = true
    controller.current?.abort()
    queueMicrotask(() => { if (active) { setCompiled(null); setSelected(''); setError(''); setBusy(false) } })
    return () => { active = false; controller.current?.abort() }
  }, [client, run.run_id])
  const load = async () => {
    controller.current?.abort()
    const current = new AbortController(); controller.current = current
    setCompiled(null); setSelected(''); setError(''); setBusy(true)
    try {
      const result = await frozenRunSources(client, run, current.signal)
      if (!current.signal.aborted) { setCompiled(result); setSelected(Object.keys(JSON.parse(result.typedPlan).nodes)[0] ?? '') }
    } catch (reason) { if (!current.signal.aborted) { setCompiled(null); setSelected(''); setError(reason instanceof Error ? reason.message : 'Frozen source is unavailable.') } }
    finally { if (!current.signal.aborted) setBusy(false) }
  }
  const nodes = compiled ? Object.keys(JSON.parse(compiled.typedPlan).nodes) : []
  const locations = sourceLocations(compiled?.sourceMap, selected)
  return <article className="panel"><p className="kicker">FROZEN RUN SOURCE</p><h2>Plan source locations</h2>
    <p className="body-copy">Explicitly read this Run’s published source with your current content permissions. These locations describe its frozen Plan; execution progress still comes from the Run timeline.</p>
    <button className="button" onClick={() => void load()} disabled={busy}>{busy ? 'Verifying frozen source…' : 'Inspect frozen source'}</button>
    {error && <p className="notice notice--error" role="alert">{error}</p>}
    {compiled && <><label><span>Frozen Plan node</span><select value={selected} onChange={(event) => setSelected(event.target.value)}>{nodes.map((node) => <option key={node}>{node}</option>)}</select></label>
      <ul>{locations.map((location, index) => <li key={index}><code>{location.file}:{location.line}:{location.column}</code> · {location.kind} · <code>{location.source_pointer || '/'}</code></li>)}</ul>
      <details><summary>Verified source identity</summary><p>Plan <code>{compiled.typedPlanDigest}</code></p><p>Source map <code>{compiled.sourceMapDigest}</code></p></details></>}
  </article>
}
