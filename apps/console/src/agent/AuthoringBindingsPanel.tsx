import { useEffect, useRef, useState } from 'react'
import { PlatformClient, PlatformProblem } from '../api/client'
import type { Json } from '../api/types'
import { parseBindingSelections } from './authoring-query'
import type { AuthoringDependency, BindingResolution, DependencyKind } from './authoring-query'

const EMPTY = '{\n  "schema_version": 1,\n  "slots": []\n}'
export function AuthoringBindingsPanel({ client, disabled, onResolved }: { client: PlatformClient | null; disabled: boolean; onResolved: (slots: Json[]) => void }) {
  const [kind, setKind] = useState<DependencyKind>('model')
  const [environment, setEnvironment] = useState('')
  const [contract, setContract] = useState('')
  const [items, setItems] = useState<AuthoringDependency[]>([])
  const [cursor, setCursor] = useState<string | null>(null)
  const [selections, setSelections] = useState(EMPTY)
  const [slotId, setSlotId] = useState('')
  const [resolution, setResolution] = useState<BindingResolution | null>(null)
  const [message, setMessage] = useState('')
  const [busy, setBusy] = useState(false)
  const controller = useRef<AbortController | null>(null)
  useEffect(() => () => controller.current?.abort(), [client])
  const begin = () => { controller.current?.abort(); const next = new AbortController(); controller.current = next; setBusy(true); setMessage(''); return next.signal }
  const clearQuery = () => { controller.current?.abort(); setItems([]); setCursor(null); setResolution(null); setBusy(false); setMessage('') }
  const fail = (error: unknown, signal: AbortSignal) => {
    if (signal.aborted) return
    if (error instanceof PlatformProblem && [401, 403].includes(error.status)) { setItems([]); setCursor(null); setResolution(null); setSelections(EMPTY); setSlotId('') }
    setMessage(error instanceof Error ? error.message : 'The authoring query failed.')
  }
  const discover = async (next?: string) => {
    if (!client) return
    const signal = begin()
    try {
      const page = await client.listAuthoringDependencies({ kind, environment: environment.trim() || undefined, interfaceContractDigest: contract.trim() || undefined, cursor: next }, { signal })
      if (signal.aborted) return
      setItems(page.data.items); setCursor(page.data.next_cursor)
      setMessage(page.data.items.length ? 'Deployments were read from the current tenant.' : page.data.next_cursor ? 'No visible deployments in this page. Continue with the next page.' : 'No visible deployments in this page.')
    } catch (error) { fail(error, signal) } finally { if (!signal.aborted) setBusy(false) }
  }
  const resolve = async () => {
    if (!client) return
    const signal = begin(); setResolution(null)
    try {
      const request = parseBindingSelections(selections)
      const response = await client.resolveAgentBindings(request, { signal })
      if (signal.aborted) return
      setResolution(response.data)
      setMessage('Review contract matching and call authorization separately before applying exact inputs.')
    } catch (error) { fail(error, signal) } finally { if (!signal.aborted) setBusy(false) }
  }
  const select = (item: AuthoringDependency, active: boolean) => {
    try {
      const request = parseBindingSelections(selections)
      const slot = request.slots.find((entry) => entry.slot_id === slotId.trim())
      if (!slot || slot.target.kind !== item.kind) throw new Error('Enter a slot ID from the selections JSON with the same dependency kind.')
      const selector: Json = active ? { kind: 'active', resource_id: item.resource_id, environment: item.environment } : { kind: 'exact', deployment: { ...item.deployment } }
      if (slot.target.kind === 'context') slot.target.deployment = selector
      else slot.target.candidates = [selector]
      setSelections(JSON.stringify(request, null, 2)); setResolution(null); setMessage(`Replaced the target selection for ${slot.slot_id}. Resolve it before applying.`)
    } catch (error) { setMessage(error instanceof Error ? error.message : 'The target could not be selected.') }
  }
  const apply = () => {
    if (!resolution || resolution.slots.some((slot) => slot.resolution.kind !== 'resolved')) return
    const slots = resolution.slots.flatMap((slot) => slot.resolution.kind === 'resolved' ? [slot.resolution.binding as Json] : [])
    onResolved(slots); setMessage('Applied exact authoring inputs. Validate the complete sources before publishing.')
  }
  return <details className="nested-panel authoring-bindings"><summary>Discover and resolve dependency bindings</summary>
    <fieldset disabled={disabled || busy || !client} style={{ border: 0, margin: 0, padding: 0, minWidth: 0 }}>
      <div className="form-grid">
        <label><span>Dependency kind</span><select value={kind} onChange={(event) => { clearQuery(); setKind(event.target.value as DependencyKind) }}>{['model', 'capability', 'context', 'child_agent', 'skill'].map((value) => <option key={value}>{value}</option>)}</select></label>
        <label><span>Dependency environment</span><input maxLength={64} value={environment} onChange={(event) => { clearQuery(); setEnvironment(event.target.value) }} placeholder="Any environment" /></label>
        <label className="field--wide"><span>Expected interface contract digest</span><input maxLength={71} value={contract} onChange={(event) => { clearQuery(); setContract(event.target.value) }} placeholder="Optional; blank means compatibility is not checked" /></label>
      </div>
      <div className="actions"><button className="button" onClick={() => discover()}>Find deployments</button>{cursor && <button className="button" onClick={() => discover(cursor)}>Next dependency page</button>}</div>
      {items.length > 0 && <><label><span>Target slot ID</span><input value={slotId} maxLength={128} onChange={(event) => setSlotId(event.target.value)} placeholder="Slot to replace in selections JSON" /></label><div className="stack">{items.map((item) => <div className="nested-panel" key={item.deployment.deployment_id}><strong>{item.resource_id} · {item.environment}</strong><p>Contract match: {item.contract_match === null ? 'Not checked' : item.contract_match ? 'Yes' : 'No'} · Call authorized: {item.call_authorized ? 'Yes' : 'No'}</p><code>{item.deployment.deployment_id}</code><div className="actions"><button className="button" onClick={() => select(item, false)}>Use exact deployment</button><button className="button" onClick={() => select(item, true)}>Use active selection</button></div><details><summary>Exact deployment and interface contract</summary><pre>{JSON.stringify(item, null, 2)}</pre></details></div>)}</div></>}
      <label className="json-field"><span>Binding selections JSON</span><textarea rows={12} value={selections} maxLength={262_144} spellCheck={false} onChange={(event) => { setSelections(event.target.value); setResolution(null) }} /><small>Provide schema_version 1 and slots with slot_id, requirement_digest, optional interface_contract_digest (null when unchecked), and an Active/Exact target. Target policies must be exact. Resolution reads current authority and creates no deployment.</small></label>
      <button className="button" onClick={resolve}>Resolve binding selections</button>
      {resolution && <div>{resolution.slots.map((slot) => <p key={slot.slot_id}><strong>{slot.slot_id}</strong>: {slot.resolution.kind === 'rejected' ? `Rejected: ${slot.resolution.code}` : `Contract match: ${slot.resolution.contract_match === null ? 'Not checked' : slot.resolution.contract_match ? 'Yes' : 'No'} · Call authorized: ${slot.resolution.call_authorized ? 'Yes' : 'No'}`}</p>)}<button className="button" disabled={resolution.slots.some((slot) => slot.resolution.kind !== 'resolved')} onClick={apply}>Apply resolved exact inputs</button></div>}
    </fieldset>
    <p role="status" aria-live="polite">{busy ? 'Reading authoring authority…' : message}</p>
  </details>
}
