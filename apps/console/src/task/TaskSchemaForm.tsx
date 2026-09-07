import { useEffect, useId, useRef, useState } from 'react'
import { analyzeTaskSchema, changeRaw, childPath, createDraft, FORM_LIMITS, schemaIdentity, validateDraft } from './schema-form.ts'
import type { FormDraft, FormNode, TaskFormSchema } from './schema-form.ts'
import type { Json } from '../api/types.ts'
import { SchemaTree } from '../schema/SchemaTree'
import { initialValue, object, treeSchema, validateTree } from '../schema/tree'
import type { TreeSchema } from '../schema/tree'
import { pinnedNominalSchemas } from '../schema/nominals'
import './TaskSchemaForm.css'

export interface TaskSchemaFormProps {
  schema: TaskFormSchema
  responseSchemaDigest: string
  onSubmit(value: Json): Promise<void>
  disabled: boolean
}

/** Caller keys this component by Task/subject identity; a schema change also resets the draft. */
export function TaskSchemaForm(props: TaskSchemaFormProps) {
  return <TaskFormSession key={schemaIdentity(props.schema, props.responseSchemaDigest)} {...props} />
}

function TaskFormSession(props: TaskSchemaFormProps) {
  const [common] = useState(() => analyzeTaskSchema(props.schema, props.responseSchemaDigest))
  return common.node ? <SchemaFormSession {...props} /> : <TreeFormSession {...props} />
}

function TreeFormSession({ schema, responseSchemaDigest, onSubmit, disabled }: TaskSchemaFormProps) {
  const [node, setNode] = useState<TreeSchema>()
  const [value, setValue] = useState<Json>(null)
  const [failure, setFailure] = useState('')
  const [attempted, setAttempted] = useState(false)
  const [pending, setPending] = useState(false)
  const active = useRef(true)
  const submitting = useRef(false)
  useEffect(() => {
    active.current = true
    void (async () => {
      try {
        if (!/^sha256:[0-9a-f]{64}$/.test(responseSchemaDigest) || !object(schema)) throw new Error('The frozen response schema identity is invalid.')
        let raw = schema
        if (Object.hasOwn(schema, 'schema')) {
          if (schema.schema_version !== 1 || !['insight.closed-json-schema/1', 'mcp.form-json-schema/2025-11-25'].includes(String(schema.profile)) || schema.canonical_digest !== responseSchemaDigest || Object.keys(schema).some((key) => !['schema_version', 'profile', 'schema', 'canonical_digest'].includes(key)) || !object(schema.schema)) throw new Error('Unsupported or mismatched response schema envelope.')
          raw = schema.schema
        }
        if (raw.type !== 'object' && typeof raw.$ref !== 'string') throw new Error('The response schema must declare an object or exact nominal root.')
        const prepared = treeSchema(raw, await pinnedNominalSchemas())
        if (active.current) { setNode(prepared); setValue(initialValue(prepared)) }
      } catch (error) { if (active.current) setFailure(error instanceof Error ? error.message : 'Schema cannot be edited.') }
    })()
    return () => { active.current = false }
  }, [schema, responseSchemaDigest])
  const errors = attempted && node ? validateTree(node, value) : {}
  return <form className="task-schema-form" noValidate onSubmit={(event) => {
    event.preventDefault()
    if (!node || disabled || submitting.current) return
    setAttempted(true)
    if (Object.keys(validateTree(node, value)).length) return
    submitting.current = true; setPending(true); setFailure('')
    void onSubmit(value).catch((error: unknown) => { if (active.current) setFailure(error instanceof Error ? error.message.slice(0, 2_048) : 'The server did not accept this response.') }).finally(() => { if (active.current) { submitting.current = false; setPending(false) } })
  }}>
    <p className="body-copy">Typed response tree. Required fields, variants, constants and local schema references come from this Task’s frozen schema.</p>
    {!node && !failure && <p role="status">Preparing the frozen schema…</p>}
    {node && <fieldset disabled={disabled || pending} className="task-schema-form__body"><SchemaTree node={node} value={value} onChange={(next) => { setValue(next); setFailure('') }} errors={errors} disabled={disabled || pending} /></fieldset>}
    {failure && <p className="notice notice--error" role="alert">{failure}</p>}
    <p className="body-copy muted">The server validates the complete response and current permission when you submit.</p>
    <button type="submit" className="button button--primary" disabled={disabled || pending || !node}>{pending ? 'Submitting…' : 'Submit response'}</button>
  </form>
}

function SchemaFormSession({ schema, responseSchemaDigest, onSubmit, disabled }: TaskSchemaFormProps) {
  const [analysis] = useState(() => analyzeTaskSchema(schema, responseSchemaDigest))
  const [draft, setDraft] = useState(() => analysis.node ? createDraft(analysis.node) : undefined)
  const [attempted, setAttempted] = useState(false)
  const [pending, setPending] = useState(false)
  const [submissionError, setSubmissionError] = useState('')
  const active = useRef(true)
  const submitting = useRef(false)
  const id = useId()
  useEffect(() => { active.current = true; return () => { active.current = false } }, [])
  const validation = draft && analysis.node ? validateDraft(analysis.node, draft) : { errors: {} }
  const errors = attempted ? validation.errors : {}
  const locked = disabled || pending
  return <form className="task-schema-form" noValidate onSubmit={(event) => {
    event.preventDefault()
    if (locked || submitting.current || !analysis.node || !draft) return
    setAttempted(true)
    const checked = validateDraft(analysis.node, draft)
    if (checked.value === undefined) return
    submitting.current = true
    setPending(true)
    setSubmissionError('')
    void (async () => {
      try { await onSubmit(checked.value!) } catch (error: unknown) {
        if (active.current) setSubmissionError(error instanceof Error ? error.message.slice(0, 2_048) : 'The server did not accept this response.')
      } finally {
        if (active.current) { submitting.current = false; setPending(false) }
      }
    })()
  }}>
    {analysis.issues.length > 0 ? <div className="notice notice--error" role="alert">
      <strong>This response schema cannot be filled in this form.</strong>
      <ul>{analysis.issues.map((issue, index) => <li key={index}>{issue}</li>)}</ul>
    </div> : analysis.node && draft && <fieldset disabled={locked} className="task-schema-form__body">
      <SchemaField node={analysis.node} draft={draft} onChange={(next) => { setDraft(next); setSubmissionError('') }} path="" name="Response" required id={id} errors={errors} root />
    </fieldset>}
    {submissionError && <p className="notice notice--error" role="alert">{submissionError}</p>}
    <p className="body-copy muted">The server validates this response against the Task’s frozen schema when you submit.</p>
    <div className="actions"><button className="button button--primary" type="submit" disabled={locked || !analysis.node}>{pending ? 'Submitting…' : 'Submit response'}</button></div>
  </form>
}

interface SchemaFieldProps {
  node: FormNode
  draft: FormDraft
  onChange(value: FormDraft): void
  path: string
  name: string
  required: boolean
  id: string
  errors: Record<string, string>
  root?: boolean
}

function SchemaField({ node, draft, onChange, path, name, required, id, errors, root }: SchemaFieldProps) {
  const inputId = `${id}-${path}`
  const errorId = `${inputId}-error`
  const descriptionId = `${inputId}-description`
  const error = errors[path]
  const title = node.title || name
  const label = `${title}${required && !root ? ' (required)' : ''}`
  const describedBy = [node.description ? descriptionId : '', error ? errorId : ''].filter(Boolean).join(' ') || undefined
  const common = { id: inputId, 'aria-invalid': Boolean(error), 'aria-describedby': describedBy }
  const setRaw = (value: string) => onChange(changeRaw(draft, value))
  return <div className={`task-schema-field${root ? ' task-schema-field--root' : ''}`} data-field-path={path}>
    {!required && <label className="task-schema-form__include"><input type="checkbox" checked={draft.included} onChange={(event) => onChange({ ...draft, included: event.target.checked })} /><span>Include {title}</span></label>}
    {draft.included && <>
      {(node.type === 'object' || node.type === 'array') ? <fieldset className="task-schema-form__group" aria-describedby={describedBy} aria-invalid={Boolean(error)}>
        <legend>{label}</legend>
        {node.description && <p id={descriptionId} className="body-copy muted">{node.description}</p>}
        {node.type === 'object' ? <div className="form-grid">{node.properties?.map((property) => <SchemaField key={property.name} node={property.node} draft={draft.children[property.name]} onChange={(next) => onChange({ ...draft, children: { ...draft.children, [property.name]: next } })} path={childPath(path, property.name)} name={property.name} required={property.required} id={id} errors={errors} />)}</div> : <>
          <p className="body-copy muted">{node.minItems}–{node.maxItems} items{node.uniqueItems ? '; each value must be unique' : ''}.</p>
          {draft.items.map((item, index) => <div className="task-schema-form__item" key={index}>
            <SchemaField node={node.items!} draft={item} onChange={(next) => onChange({ ...draft, items: draft.items.map((previous, at) => at === index ? next : previous) })} path={childPath(path, index)} name={`${title} item ${index + 1}`} required id={id} errors={errors} />
            <button type="button" className="button" aria-label={`Remove ${title} item ${index + 1}`} onClick={() => onChange({ ...draft, items: draft.items.filter((_, at) => at !== index) })}>Remove item</button>
          </div>)}
          <button type="button" className="button" disabled={draft.items.length >= (node.maxItems ?? 0)} onClick={() => {
            if (draft.items.length < (node.maxItems ?? 0)) onChange({ ...draft, items: [...draft.items, createDraft(node.items!)] })
          }}>Add {title} item</button>
        </>}
      </fieldset> : <>
        <label htmlFor={inputId}><span>{label}</span>
          {node.choices ? <select {...common} value={draft.raw} onChange={(event) => setRaw(event.target.value)}>
            <option value="">Select a value</option>
            {node.choices.map((value, index) => <option key={index} value={String(index)}>{node.choiceNames?.[index] ?? String(value)}</option>)}
          </select> : node.type === 'boolean' ? <select {...common} value={draft.raw} onChange={(event) => setRaw(event.target.value)}>
            <option value="">Select Yes or No</option><option value="true">Yes</option><option value="false">No</option>
          </select> : node.type === 'string' ? <textarea {...common} value={draft.raw} rows={3} maxLength={FORM_LIMITS.text * 2 + 1} onChange={(event) => setRaw(event.target.value)} /> : <input {...common} type="text" inputMode="decimal" value={draft.raw} maxLength={129} onChange={(event) => setRaw(event.target.value)} />}
        </label>
        {node.description && <p id={descriptionId} className="body-copy muted">{node.description}</p>}
      </>}
    </>}
    {error && <p className="task-schema-form__error" id={errorId} role="alert">{error}</p>}
  </div>
}

export default TaskSchemaForm
