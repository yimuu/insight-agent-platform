import { validationMessage } from '../../shared/i18n/validation'
import sharedStyles from '../../shared/ui/Primitives.module.css'
import { classNames } from '../../shared/ui/class-names.ts'
import localStyles from './TaskSchemaForm.module.css'
const cx = classNames(sharedStyles, localStyles)
import { useEffect, useId, useRef, useState } from 'react'
import {
  analyzeTaskSchema,
  changeRaw,
  childPath,
  createDraft,
  FORM_LIMITS,
  schemaIdentity,
  validateDraft,
} from './schema-form.ts'
import type { FormDraft, FormNode, TaskFormSchema } from './schema-form.ts'
import type { Json } from '../../shared/api/types.ts'
import { SchemaTree } from '../../shared/schema/SchemaTree'
import { initialValue, object, treeSchema, validateTree } from '../../shared/schema/tree'
import type { TreeSchema } from '../../shared/schema/tree'
import { pinnedNominalSchemas } from '../../shared/schema/nominals'

export interface TaskSchemaFormProps {
  schema: TaskFormSchema
  responseSchemaDigest: string
  onSubmit(value: Json): Promise<void>
  disabled: boolean
}

/** Caller keys this component by Task/subject identity; a schema change also resets the draft. */
export function TaskSchemaForm(props: TaskSchemaFormProps) {
  return (
    <TaskFormSession key={schemaIdentity(props.schema, props.responseSchemaDigest)} {...props} />
  )
}

function TaskFormSession(props: TaskSchemaFormProps) {
  const [common] = useState(() => analyzeTaskSchema(props.schema, props.responseSchemaDigest))
  return common.node ? <SchemaFormSession {...props} /> : <TreeFormSession {...props} />
}

function TreeFormSession({
  schema,
  responseSchemaDigest,
  onSubmit,
  disabled,
}: TaskSchemaFormProps) {
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
        if (!/^sha256:[0-9a-f]{64}$/.test(responseSchemaDigest) || !object(schema))
          throw new Error('The frozen response schema identity is invalid.')
        let raw = schema
        if (Object.hasOwn(schema, 'schema')) {
          if (
            schema.schema_version !== 1 ||
            !['insight.closed-json-schema/1', 'mcp.form-json-schema/2025-11-25'].includes(
              String(schema.profile),
            ) ||
            schema.canonical_digest !== responseSchemaDigest ||
            Object.keys(schema).some(
              (key) => !['schema_version', 'profile', 'schema', 'canonical_digest'].includes(key),
            ) ||
            !object(schema.schema)
          )
            throw new Error('Unsupported or mismatched response schema envelope.')
          raw = schema.schema
        }
        if (raw.type !== 'object' && typeof raw.$ref !== 'string')
          throw new Error('The response schema must declare an object or exact nominal root.')
        const prepared = treeSchema(raw, await pinnedNominalSchemas())
        if (active.current) {
          setNode(prepared)
          setValue(initialValue(prepared))
        }
      } catch (error) {
        if (active.current)
          setFailure(error instanceof Error ? error.message : 'Schema cannot be edited.')
      }
    })()
    return () => {
      active.current = false
    }
  }, [schema, responseSchemaDigest])
  const errors = attempted && node ? validateTree(node, value) : {}
  return (
    <form
      data-ui="task-schema-form"
      className={cx('task-schema-form')}
      noValidate
      onSubmit={(event) => {
        event.preventDefault()
        if (!node || disabled || submitting.current) return
        setAttempted(true)
        if (Object.keys(validateTree(node, value)).length) return
        submitting.current = true
        setPending(true)
        setFailure('')
        void onSubmit(value)
          .catch((error: unknown) => {
            if (active.current)
              setFailure(
                error instanceof Error
                  ? error.message.slice(0, 2_048)
                  : 'The server did not accept this response.',
              )
          })
          .finally(() => {
            if (active.current) {
              submitting.current = false
              setPending(false)
            }
          })
      }}
    >
      <p className={cx('body-copy')}>表单字段、必填项和可选结构来自本次任务的固定 Schema。</p>
      {!node && !failure && <p role="status">正在准备表单结构…</p>}
      {node && (
        <fieldset disabled={disabled || pending} className={cx('task-schema-form__body')}>
          <SchemaTree
            node={node}
            value={value}
            onChange={(next) => {
              setValue(next)
              setFailure('')
            }}
            errors={errors}
            disabled={disabled || pending}
          />
        </fieldset>
      )}
      {failure && (
        <p data-ui="notice--error" className={cx('notice notice--error')} role="alert">
          {failure ? validationMessage(failure) : null}
        </p>
      )}
      <p className={cx('body-copy muted')}>提交时服务端会验证完整回复及当前权限。</p>
      <button
        type="submit"
        className={cx('button button--primary')}
        disabled={disabled || pending || !node}
      >
        {pending ? '提交中…' : '提交回复'}
      </button>
    </form>
  )
}

function SchemaFormSession({
  schema,
  responseSchemaDigest,
  onSubmit,
  disabled,
}: TaskSchemaFormProps) {
  const [analysis] = useState(() => analyzeTaskSchema(schema, responseSchemaDigest))
  const [draft, setDraft] = useState(() => (analysis.node ? createDraft(analysis.node) : undefined))
  const [attempted, setAttempted] = useState(false)
  const [pending, setPending] = useState(false)
  const [submissionError, setSubmissionError] = useState('')
  const active = useRef(true)
  const submitting = useRef(false)
  const id = useId()
  useEffect(() => {
    active.current = true
    return () => {
      active.current = false
    }
  }, [])
  const validation = draft && analysis.node ? validateDraft(analysis.node, draft) : { errors: {} }
  const errors = attempted ? validation.errors : {}
  const locked = disabled || pending
  return (
    <form
      data-ui="task-schema-form"
      className={cx('task-schema-form')}
      noValidate
      onSubmit={(event) => {
        event.preventDefault()
        if (locked || submitting.current || !analysis.node || !draft) return
        setAttempted(true)
        const checked = validateDraft(analysis.node, draft)
        if (checked.value === undefined) return
        submitting.current = true
        setPending(true)
        setSubmissionError('')
        void (async () => {
          try {
            await onSubmit(checked.value!)
          } catch (error: unknown) {
            if (active.current)
              setSubmissionError(
                error instanceof Error
                  ? error.message.slice(0, 2_048)
                  : 'The server did not accept this response.',
              )
          } finally {
            if (active.current) {
              submitting.current = false
              setPending(false)
            }
          }
        })()
      }}
    >
      {analysis.issues.length > 0 ? (
        <div data-ui="notice--error" className={cx('notice notice--error')} role="alert">
          <strong>此回复结构无法使用当前表单填写。</strong>
          <ul>
            {analysis.issues.map((issue, index) => (
              <li key={index}>{validationMessage(issue)}</li>
            ))}
          </ul>
        </div>
      ) : (
        analysis.node &&
        draft && (
          <fieldset disabled={locked} className={cx('task-schema-form__body')}>
            <SchemaField
              node={analysis.node}
              draft={draft}
              onChange={(next) => {
                setDraft(next)
                setSubmissionError('')
              }}
              path=""
              name="回复"
              required
              id={id}
              errors={errors}
              root
            />
          </fieldset>
        )
      )}
      {submissionError && (
        <p data-ui="notice--error" className={cx('notice notice--error')} role="alert">
          {submissionError}
        </p>
      )}
      <p className={cx('body-copy muted')}>提交时服务端将按本次任务的 Schema 校验回复。</p>
      <div className={cx('actions')}>
        <button
          className={cx('button button--primary')}
          type="submit"
          disabled={locked || !analysis.node}
        >
          {pending ? '提交中…' : '提交回复'}
        </button>
      </div>
    </form>
  )
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

function SchemaField({
  node,
  draft,
  onChange,
  path,
  name,
  required,
  id,
  errors,
  root,
}: SchemaFieldProps) {
  const inputId = `${id}-${path}`
  const errorId = `${inputId}-error`
  const descriptionId = `${inputId}-description`
  const error = errors[path]
  const title = node.title || name
  const label = `${title}${required && !root ? '（必填）' : ''}`
  const describedBy =
    [node.description ? descriptionId : '', error ? errorId : ''].filter(Boolean).join(' ') ||
    undefined
  const common = { id: inputId, 'aria-invalid': Boolean(error), 'aria-describedby': describedBy }
  const setRaw = (value: string) => onChange(changeRaw(draft, value))
  return (
    <div
      className={cx(`task-schema-field${root ? ' task-schema-field--root' : ''}`)}
      data-field-path={path}
    >
      {!required && (
        <label className={cx('task-schema-form__include')}>
          <input
            type="checkbox"
            checked={draft.included}
            onChange={(event) => onChange({ ...draft, included: event.target.checked })}
          />
          <span>包含 {title}</span>
        </label>
      )}
      {draft.included && (
        <>
          {node.type === 'object' || node.type === 'array' ? (
            <fieldset
              className={cx('task-schema-form__group')}
              aria-describedby={describedBy}
              aria-invalid={Boolean(error)}
            >
              <legend>{label}</legend>
              {node.description && (
                <p id={descriptionId} className={cx('body-copy muted')}>
                  {node.description}
                </p>
              )}
              {node.type === 'object' ? (
                <div className={cx('form-grid')}>
                  {node.properties?.map((property) => (
                    <SchemaField
                      key={property.name}
                      node={property.node}
                      draft={draft.children[property.name]}
                      onChange={(next) =>
                        onChange({
                          ...draft,
                          children: { ...draft.children, [property.name]: next },
                        })
                      }
                      path={childPath(path, property.name)}
                      name={property.name}
                      required={property.required}
                      id={id}
                      errors={errors}
                    />
                  ))}
                </div>
              ) : (
                <>
                  <p className={cx('body-copy muted')}>
                    {node.minItems}–{node.maxItems} 项
                    {node.uniqueItems ? '; each value must be unique' : ''}.
                  </p>
                  {draft.items.map((item, index) => (
                    <div className={cx('task-schema-form__item')} key={index}>
                      <SchemaField
                        node={node.items!}
                        draft={item}
                        onChange={(next) =>
                          onChange({
                            ...draft,
                            items: draft.items.map((previous, at) =>
                              at === index ? next : previous,
                            ),
                          })
                        }
                        path={childPath(path, index)}
                        name={`${title} 第 ${index + 1}`}
                        required
                        id={id}
                        errors={errors}
                      />
                      <button
                        type="button"
                        className={cx('button')}
                        aria-label={`Remove ${title} 第 ${index + 1}`}
                        onClick={() =>
                          onChange({ ...draft, items: draft.items.filter((_, at) => at !== index) })
                        }
                      >
                        删除此项
                      </button>
                    </div>
                  ))}
                  <button
                    type="button"
                    className={cx('button')}
                    disabled={draft.items.length >= (node.maxItems ?? 0)}
                    onClick={() => {
                      if (draft.items.length < (node.maxItems ?? 0))
                        onChange({ ...draft, items: [...draft.items, createDraft(node.items!)] })
                    }}
                  >
                    添加 {title} 项
                  </button>
                </>
              )}
            </fieldset>
          ) : (
            <>
              <label htmlFor={inputId}>
                <span>{label}</span>
                {node.choices ? (
                  <select
                    {...common}
                    value={draft.raw}
                    onChange={(event) => setRaw(event.target.value)}
                  >
                    <option value="">选择一个值</option>
                    {node.choices.map((value, index) => (
                      <option key={index} value={String(index)}>
                        {node.choiceNames?.[index] ?? String(value)}
                      </option>
                    ))}
                  </select>
                ) : node.type === 'boolean' ? (
                  <select
                    {...common}
                    value={draft.raw}
                    onChange={(event) => setRaw(event.target.value)}
                  >
                    <option value="">选择是或否</option>
                    <option value="true">是</option>
                    <option value="false">否</option>
                  </select>
                ) : node.type === 'string' ? (
                  <textarea
                    {...common}
                    value={draft.raw}
                    rows={3}
                    maxLength={FORM_LIMITS.text * 2 + 1}
                    onChange={(event) => setRaw(event.target.value)}
                  />
                ) : (
                  <input
                    {...common}
                    type="text"
                    inputMode="decimal"
                    value={draft.raw}
                    maxLength={129}
                    onChange={(event) => setRaw(event.target.value)}
                  />
                )}
              </label>
              {node.description && (
                <p id={descriptionId} className={cx('body-copy muted')}>
                  {node.description}
                </p>
              )}
            </>
          )}
        </>
      )}
      {error && (
        <p className={cx('task-schema-form__error')} id={errorId} role="alert">
          {error ? validationMessage(error) : null}
        </p>
      )}
    </div>
  )
}

export default TaskSchemaForm
