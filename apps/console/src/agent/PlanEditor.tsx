import { useEffect, useLayoutEffect, useMemo, useRef, useState } from 'react'
import descriptorText from '../../../../contracts/platform-v1/agent-node-editor.v1.json?raw'
import { SchemaTree } from '../schema/SchemaTree'
import { object, pointer } from '../schema/tree'
import type { Json } from '../api/types'
import { authoredExpressions, draftFields, readNodeEditorDescriptor, referencedPorts, replaceAt, sourceLocations } from './plan-editor'
import type { CompiledAgent } from './compiler'
import { digestJson, rebuildExpression } from './compiler'

const descriptor = readNodeEditorDescriptor(descriptorText)
export function PlanEditor({ source, onChange, compiled, disabled }: { source: string; onChange(source: string): void; compiled: CompiledAgent | null; disabled: boolean }) {
  const [selected, setSelected] = useState('')
  const [newId, setNewId] = useState('')
  const [newKind, setNewKind] = useState(descriptor.nodes[0].kind)
  const [error, setError] = useState('')
  const [building, setBuilding] = useState(false)
  const pending = useRef<AbortController | null>(null)
  const revision = useRef(source)
  useLayoutEffect(() => { revision.current = source }, [source])
  useEffect(() => () => pending.current?.abort(), [])
  const plan = useMemo(() => {
    if (new TextEncoder().encode(source).length > 1_048_576) return null
    try { const json: unknown = JSON.parse(source); return object(json) && object(json.nodes) ? json : null } catch { return null }
  }, [source])
  if (!plan || !object(plan.nodes)) return <p className="body-copy">Supply a complete Plan or static framework JSON document to edit its Platform nodes.</p>
  const nodes = plan.nodes
  const nodeIds = Object.keys(nodes)
  const id = Object.hasOwn(nodes, selected) ? selected : nodeIds[0]
  const value = nodes[id]
  const template = object(value) ? descriptor.nodes.find((node) => node.kind === value.kind)?.template : undefined
  const locations = sourceLocations(compiled?.sourceMap, id)
  const ports = referencedPorts(value ?? null)
  const expressions = authoredExpressions(value ?? null)
  const availablePorts = [...new Map(referencedPorts(nodes).map((entry) => [JSON.stringify(entry.value), entry.value])).values()]
  const update = (next: Json) => {
    const encoded = JSON.stringify(next, null, 2)
    if (new TextEncoder().encode(encoded).length > 1_048_576) { setError('Plan source exceeds the 1 MiB editing limit.'); return }
    setError(''); onChange(encoded)
  }
  const suggestions = (path: string) => {
    const field = path.split('/').at(-1) ?? ''
    if (['next', 'resume', 'body', 'exit', 'target', 'otherwise', 'join', 'entry_node_id', 'producer_node_id'].includes(field) || path.includes('/legs/') || path.includes('/handlers/')) return nodeIds
    if (field.endsWith('schema_digest') || field === 'schema_digest') return object(plan.schema_documents) ? Object.keys(plan.schema_documents) : []
    if (field.endsWith('_slot_id') || path.includes('_slot_ids/')) return object(plan.dependency_slots) ? Object.keys(plan.dependency_slots) : []
    return []
  }
  const build = async (entry: (typeof expressions)[number]) => {
    if (building || disabled) return
    const controller = new AbortController(); pending.current = controller
    const snapshot = source
    setBuilding(true); setError('')
    try {
      const expression = structuredClone(entry.value)
      if (Array.isArray(expression.instructions)) expression.instructions = await Promise.all(expression.instructions.map(async (instruction) => {
        if (object(instruction) && instruction.op === 'literal' && object(instruction.value) && Object.hasOwn(instruction.value, 'value')) return { ...instruction, value: { ...instruction.value, canonical_digest: await digestJson(instruction.value.value) } }
        return instruction
      }))
      const rebuilt = await rebuildExpression(expression, controller.signal)
      if (revision.current !== snapshot) throw new Error('Source changed during expression rebuilding. Rebuild its current fields again.')
      if (!controller.signal.aborted) update({ ...plan, nodes: { ...nodes, [id]: replaceAt(value, entry.keys, rebuilt) } })
    } catch (error) { if (!controller.signal.aborted) setError(error instanceof Error ? error.message : 'Expression rebuilding failed.') }
    finally { if (!controller.signal.aborted) setBuilding(false) }
  }
  return <details className="nested-panel" open><summary>Typed Platform node editor</summary>
    <p className="body-copy">Edit the same source JSON below. Templates are incomplete drafts; shared Rust compilation checks types, ports, expressions, bounds and control flow.</p>
    <div className="actions" data-editor-view><label><span>Selected node</span><select value={id ?? ''} disabled={disabled} onChange={(event) => setSelected(event.target.value)}>{nodeIds.map((key) => <option value={key} key={key}>{key} · {object(nodes[key]) ? String(nodes[key].kind) : 'invalid node'}</option>)}</select></label><label><span>New node ID</span><input value={newId} maxLength={128} onChange={(event) => setNewId(event.target.value)} disabled={disabled} /></label><label><span>New node kind</span><select value={newKind} onChange={(event) => setNewKind(event.target.value)} disabled={disabled}>{descriptor.nodes.map((node) => <option key={node.kind}>{node.kind}</option>)}</select></label><button type="button" className="button" disabled={disabled || !/^[A-Za-z0-9][A-Za-z0-9_.:-]{0,127}$/.test(newId) || Object.hasOwn(nodes, newId)} onClick={() => { update({ ...plan, nodes: { ...nodes, [newId]: structuredClone(descriptor.nodes.find((n) => n.kind === newKind)!.template) } }); setSelected(newId); setNewId('') }}>Add Platform node</button></div>
    <label><span>Entry node</span><select disabled={disabled} value={String(plan.entry_node_id ?? '')} onChange={(event) => update({ ...plan, entry_node_id: event.target.value })}>{!nodeIds.includes(String(plan.entry_node_id)) && <option value={String(plan.entry_node_id ?? '')}>{String(plan.entry_node_id ?? 'Select entry')}</option>}{nodeIds.map((key) => <option key={key}>{key}</option>)}</select></label>
    {id && template && <SchemaTree key={id} node={draftFields(descriptor, template, value)} value={value} onChange={(next) => update({ ...plan, nodes: { ...nodes, [id]: next } })} label={`Node ${id}`} path={pointer('/nodes', id)} disabled={disabled} suggestions={suggestions} />}
    {ports.length > 0 && <details><summary>Reuse exact typed port references</summary><p className="body-copy">Choose a reference already declared in this source. Rust checks producer scope, schema identity and data flow.</p>{ports.slice(0, 128).map((port) => <label key={JSON.stringify(port.keys)}><span>Port {port.keys.join('/')}</span><select value="" disabled={disabled} onChange={(event) => update({ ...plan, nodes: { ...nodes, [id]: replaceAt(value, port.keys, structuredClone(availablePorts[Number(event.target.value)])) } })}><option value="" disabled>Choose an existing exact reference</option>{availablePorts.map((candidate, index) => <option key={index} value={index}>{candidate.source === 'run_input' ? 'Run input' : `${candidate.producer_node_id}/${candidate.port_id}`} · {String(candidate.schema_digest)}</option>)}</select></label>)}</details>}
    {expressions.length > 0 && <div><p className="body-copy">After editing expression instructions, rebuild their derived stack depth and digest with Rust, then validate the complete Plan.</p><div className="actions">{expressions.map((entry) => <button type="button" className="button" key={JSON.stringify(entry.keys)} disabled={disabled || building} onClick={() => void build(entry)}>{building ? 'Rebuilding expression…' : `Rebuild expression ${entry.keys.join('/')}`}</button>)}</div></div>}
    {id && !template && <p className="notice notice--error">This node kind has no editing descriptor. Its original fields remain in source JSON for the Rust diagnostic.</p>}
    {id && <button type="button" className="button" disabled={disabled} onClick={() => { update({ ...plan, nodes: Object.fromEntries(Object.entries(nodes).filter(([key]) => key !== id)) }); setSelected('') }}>Remove node {id}</button>}
    <details><summary>Dependency slots and schema documents</summary>{['dependency_slots', 'schema_documents'].map((field) => <SchemaTree key={field} node={draftFields(descriptor, {}, plan[field] ?? {}, field)} value={plan[field] ?? {}} onChange={(next) => update({ ...plan, [field]: next })} label={field} path={`/${field}`} disabled={disabled} suggestions={suggestions} />)}</details>
    {locations.length > 0 && <details open><summary>Compiled source locations for {id}</summary><ul>{locations.map((location, index) => <li key={index}><code>{location.file}:{location.line}:{location.column}</code> · {location.kind} · <code>{location.source_pointer || '/'}</code></li>)}</ul></details>}
    {!compiled && <p className="body-copy muted">Validate the current sources to refresh their source locations.</p>}
    {error && <p className="notice notice--error" role="alert">{error}</p>}
  </details>
}
