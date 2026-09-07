import { useId, useState } from 'react'
import type { Json } from '../api/types'
import { branchIndex, equal, initialValue, object, pointer, treeSchema, TREE_LIMITS } from './tree'
import type { TreeSchema } from './tree'

export interface SchemaTreeProps {
  node: TreeSchema
  value: Json
  onChange(value: Json): void
  label?: string
  path?: string
  errors?: Record<string, string>
  disabled?: boolean
  suggestions?(path: string, node: TreeSchema): string[]
  depth?: number
}
const unconstrained = treeSchema({})
const cloneWithout = (value: { [key: string]: Json }, key: string) => Object.fromEntries(Object.entries(value).filter(([name]) => name !== key))
function variantLabel(node: TreeSchema, index: number): string {
  const s = node.schema
  if (typeof s.title === 'string') return s.title
  if (Object.hasOwn(s, 'const')) return JSON.stringify(s.const)
  const tag = Object.entries(node.properties).find(([, p]) => typeof p.schema.const === 'string')
  return tag ? `${tag[0]}: ${String(tag[1].schema.const)}` : typeof s.type === 'string' ? `${s.type} ${index + 1}` : `Variant ${index + 1}`
}

/** This tree edits one JSON value. It creates no execution model and performs no I/O. */
export function SchemaTree({ node, value, onChange, label = 'Response', path = '', errors = {}, disabled = false, suggestions, depth = 0 }: SchemaTreeProps) {
  const id = useId()
  const [page, setPage] = useState(0)
  const [newKey, setNewKey] = useState('')
  const s = node.schema
  const name = typeof s.title === 'string' ? s.title : label
  const error = errors[path]
  const common = { disabled, 'aria-invalid': Boolean(error), 'aria-describedby': error ? `${id}-error` : undefined }
  const child = (spec: TreeSchema, v: Json, change: (next: Json) => void, key: string | number, title: string) => <SchemaTree key={key} node={spec} value={v} onChange={change} label={title} path={pointer(path, key)} errors={errors} disabled={disabled} suggestions={suggestions} depth={depth + 1} />
  const windowSize = 25
  const pagination = (total: number) => total > windowSize && <div className="actions"><button type="button" className="button" disabled={disabled || page === 0} onClick={() => setPage((p) => Math.max(0, p - 1))}>Previous fields</button><span>{page * windowSize + 1}–{Math.min(total, (page + 1) * windowSize)} of {total}</span><button type="button" className="button" disabled={disabled || (page + 1) * windowSize >= total} onClick={() => setPage((p) => p + 1)}>Next fields</button></div>
  const rows = (all: string[]) => all.slice(Math.min(page * windowSize, Math.max(0, all.length - 1)), Math.min(page * windowSize, Math.max(0, all.length - 1)) + windowSize)
  let control
  if (depth > TREE_LIMITS.depth) control = <p role="alert">This value exceeds the tree depth limit.</p>
  else if (Object.hasOwn(s, 'const')) control = <div><strong>{name}</strong><p className="body-copy">Fixed by the schema.</p><pre>{JSON.stringify(s.const, null, 2).slice(0, TREE_LIMITS.bytes)}</pre>{!equal(s.const, value) && <button type="button" className="button" disabled={disabled} onClick={() => onChange(structuredClone(s.const))}>Use schema constant</button>}</div>
  else if (Array.isArray(s.enum)) control = <label><span>{name}</span><select {...common} value={s.enum.findIndex((choice) => equal(choice, value))} onChange={(event) => onChange(structuredClone((s.enum as Json[])[Number(event.target.value)]))}><option value={-1} disabled>Select a value</option>{s.enum.map((choice, i) => <option value={i} key={i}>{Array.isArray(s.enumNames) && typeof s.enumNames[i] === 'string' ? String(s.enumNames[i]) : JSON.stringify(choice)}</option>)}</select></label>
  else if (node.alternatives.length) {
    const selected = branchIndex(node, value)
    control = <fieldset className="task-schema-form__group"><legend>{name}</legend><label><span>{name} variant</span><select {...common} value={selected} onChange={(event) => onChange(initialValue(node.alternatives[Number(event.target.value)]))}><option value={-1} disabled>Select a variant</option>{node.alternatives.map((branch, i) => <option value={i} key={i}>{variantLabel(branch, i)}</option>)}</select></label>{selected >= 0 && <SchemaTree node={node.alternatives[selected]} value={value} onChange={onChange} label={name} path={path} errors={errors} disabled={disabled} suggestions={suggestions} depth={depth + 1} />}</fieldset>
  } else if (s.type === 'object' || (s.type === undefined && object(value))) {
    const fields = object(value) ? value : {}
    const keys = [...new Set([...Object.keys(node.properties), ...Object.keys(fields)])]
    control = <fieldset className="task-schema-form__group"><legend>{name}</legend>{!object(value) && <button type="button" className="button" disabled={disabled} onClick={() => onChange(initialValue(node))}>Use object</button>}{rows(keys).map((key) => {
      const required = Array.isArray(s.required) && s.required.includes(key)
      const included = Object.hasOwn(fields, key)
      const spec = node.properties[key] ?? node.additional ?? unconstrained
      return <div key={key} className="task-schema-form__item">{(!required || !included) && <label className="task-schema-form__include"><input type="checkbox" checked={included} disabled={disabled} onChange={(event) => onChange(event.target.checked ? { ...fields, [key]: initialValue(spec) } : cloneWithout(fields, key))} /><span>Include {key}{required ? ' (required)' : ''}</span></label>}{included && child(spec, fields[key], (next) => onChange({ ...fields, [key]: next }), key, `${key}${required ? ' (required)' : ''}`)}</div>
    })}{pagination(keys.length)}{s.additionalProperties !== false && <div className="actions"><label><span>{name} new property</span><input {...common} value={newKey} maxLength={256} onChange={(event) => setNewKey(event.target.value)} /></label><button type="button" className="button" disabled={disabled || !newKey || Object.hasOwn(fields, newKey) || keys.length >= TREE_LIMITS.properties} onClick={() => { onChange({ ...fields, [newKey]: initialValue(node.additional ?? unconstrained) }); setNewKey(''); setPage(Math.floor(keys.length / windowSize)) }}>Add property</button></div>}</fieldset>
  } else if (s.type === 'array' || (s.type === undefined && Array.isArray(value))) {
    const values = Array.isArray(value) ? value : []
    const maximum = Math.min(TREE_LIMITS.items, typeof s.maxItems === 'number' ? s.maxItems : TREE_LIMITS.items)
    control = <fieldset className="task-schema-form__group"><legend>{name}</legend><p className="body-copy">{String(s.minItems ?? 0)}–{maximum} items. Order is preserved.</p>{values.slice(page * windowSize, (page + 1) * windowSize).map((item, at) => {
      const i = page * windowSize + at
      return <div key={i} className="task-schema-form__item">{child(node.items ?? unconstrained, item, (next) => onChange(values.map((v, j) => j === i ? next : v)), i, `${name} item ${i + 1}`)}<div className="actions"><button type="button" className="button" disabled={disabled} onClick={() => onChange(values.filter((_, j) => i !== j))}>Remove {name} item {i + 1}</button>{i > 0 && <button type="button" className="button" disabled={disabled} onClick={() => { const next = [...values]; [next[i - 1], next[i]] = [next[i], next[i - 1]]; onChange(next) }}>Move {name} item {i + 1} up</button>}</div></div>
    })}{pagination(values.length)}<button type="button" className="button" disabled={disabled || values.length >= maximum} onClick={() => { onChange([...values, initialValue(node.items ?? unconstrained)]); setPage(Math.floor(values.length / windowSize)) }}>Add {name} item</button></fieldset>
  } else if (s.type === 'null') control = <p>{name}: null</p>
  else if (s.type === 'boolean' || (s.type === undefined && typeof value === 'boolean')) control = <label><span>{name}</span><select {...common} value={String(value)} onChange={(event) => onChange(event.target.value === 'true')}><option value="false">No</option><option value="true">Yes</option></select></label>
  else if (s.type === 'number' || s.type === 'integer' || (s.type === undefined && typeof value === 'number')) control = <NumberField key={path} value={typeof value === 'number' ? value : Number.NaN} label={name} disabled={disabled} error={error} onChange={onChange} />
  else if (s.type === 'string' || (s.type === undefined && typeof value === 'string')) {
    const options = suggestions?.(path, node) ?? []
    control = <label><span>{name}</span>{options.length ? <><input {...common} list={`${id}-choices`} value={typeof value === 'string' ? value : ''} maxLength={TREE_LIMITS.stringBytes + 1} onChange={(event) => onChange(event.target.value)} /><datalist id={`${id}-choices`}>{options.map((choice) => <option value={choice} key={choice} />)}</datalist></> : <textarea {...common} rows={2} value={typeof value === 'string' ? value : ''} maxLength={TREE_LIMITS.stringBytes + 1} onChange={(event) => onChange(event.target.value)} />}</label>
  } else control = <p>{name}: null</p>
  return <div className="task-schema-field" data-field-path={path}>{s.type === undefined && !node.alternatives.length && !Array.isArray(s.enum) && !Object.hasOwn(s, 'const') && <label><span>{name} JSON type</span><select {...common} value={value === null ? 'null' : Array.isArray(value) ? 'array' : typeof value} onChange={(event) => onChange(({ object: {}, array: [], string: '', number: 0, boolean: false, null: null } as Record<string, Json>)[event.target.value])}>{['object', 'array', 'string', 'number', 'boolean', 'null'].map((type) => <option key={type}>{type}</option>)}</select></label>}{control}{typeof s.description === 'string' && <p className="body-copy muted">{s.description}</p>}{error && <p id={`${id}-error`} className="task-schema-form__error" role="alert">{error}</p>}</div>
}

function NumberField({ value, label, disabled, error, onChange }: { value: number; label: string; disabled: boolean; error?: string; onChange(value: number): void }) {
  const [draft, setDraft] = useState({ value, raw: String(value) })
  const raw = Object.is(draft.value, value) ? draft.raw : String(value)
  const valid = /^-?(0|[1-9][0-9]*)(\.[0-9]+)?([eE][+-]?[0-9]+)?$/.test(raw) && Number.isFinite(Number(raw))
  return <label><span>{label}</span><input type="text" inputMode="decimal" value={raw} maxLength={129} disabled={disabled} aria-invalid={Boolean(error) || !valid} onChange={(event) => {
    const next = event.target.value
    const number = /^-?(0|[1-9][0-9]*)(\.[0-9]+)?([eE][+-]?[0-9]+)?$/.test(next) ? Number(next) : Number.NaN
    setDraft({ value: number, raw: next })
    // An invalid draft must never silently submit the previously valid number.
    onChange(number)
  }} />{!valid && <span className="task-schema-form__error" role="alert">Enter a complete JSON number.</span>}</label>
}
