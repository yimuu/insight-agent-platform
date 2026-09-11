import { validationMessage } from '../i18n/validation'
import sharedStyles from '../ui/Primitives.module.css'
import { classNames } from '../ui/class-names.ts'
const cx = classNames(sharedStyles)
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
const cloneWithout = (value: { [key: string]: Json }, key: string) =>
  Object.fromEntries(Object.entries(value).filter(([name]) => name !== key))
function variantLabel(node: TreeSchema, index: number): string {
  const s = node.schema
  if (typeof s.title === 'string') return s.title
  if (Object.hasOwn(s, 'const')) return JSON.stringify(s.const)
  const tag = Object.entries(node.properties).find(([, p]) => typeof p.schema.const === 'string')
  return tag
    ? `${tag[0]}: ${String(tag[1].schema.const)}`
    : typeof s.type === 'string'
      ? `${s.type} ${index + 1}`
      : `分支 ${index + 1}`
}

/** This tree edits one JSON value. It creates no execution model and performs no I/O. */
export function SchemaTree({
  node,
  value,
  onChange,
  label = '回复',
  path = '',
  errors = {},
  disabled = false,
  suggestions,
  depth = 0,
}: SchemaTreeProps) {
  const id = useId()
  const [page, setPage] = useState(0)
  const [newKey, setNewKey] = useState('')
  const s = node.schema
  const name = typeof s.title === 'string' ? s.title : label
  const error = errors[path]
  const common = {
    disabled,
    'aria-invalid': Boolean(error),
    'aria-describedby': error ? `${id}-error` : undefined,
  }
  const child = (
    spec: TreeSchema,
    v: Json,
    change: (next: Json) => void,
    key: string | number,
    title: string,
  ) => (
    <SchemaTree
      key={key}
      node={spec}
      value={v}
      onChange={change}
      label={title}
      path={pointer(path, key)}
      errors={errors}
      disabled={disabled}
      suggestions={suggestions}
      depth={depth + 1}
    />
  )
  const windowSize = 25
  const pagination = (total: number) =>
    total > windowSize && (
      <div className={cx('actions')}>
        <button
          type="button"
          className={cx('button')}
          disabled={disabled || page === 0}
          onClick={() => setPage((p) => Math.max(0, p - 1))}
        >
          上一页字段
        </button>
        <span>
          {page * windowSize + 1}–{Math.min(total, (page + 1) * windowSize)} / {total}
        </span>
        <button
          type="button"
          className={cx('button')}
          disabled={disabled || (page + 1) * windowSize >= total}
          onClick={() => setPage((p) => p + 1)}
        >
          下一页字段
        </button>
      </div>
    )
  const rows = (all: string[]) =>
    all.slice(
      Math.min(page * windowSize, Math.max(0, all.length - 1)),
      Math.min(page * windowSize, Math.max(0, all.length - 1)) + windowSize,
    )
  let control
  if (depth > TREE_LIMITS.depth) control = <p role="alert">此值超过结构嵌套深度限制。</p>
  else if (Object.hasOwn(s, 'const'))
    control = (
      <div>
        <strong>{name}</strong>
        <p className={cx('body-copy')}>由 Schema 固定。</p>
        <pre>{JSON.stringify(s.const, null, 2).slice(0, TREE_LIMITS.bytes)}</pre>
        {!equal(s.const, value) && (
          <button
            type="button"
            className={cx('button')}
            disabled={disabled}
            onClick={() => onChange(structuredClone(s.const))}
          >
            使用固定值
          </button>
        )}
      </div>
    )
  else if (Array.isArray(s.enum))
    control = (
      <label>
        <span>{name}</span>
        <select
          {...common}
          value={s.enum.findIndex((choice) => equal(choice, value))}
          onChange={(event) =>
            onChange(structuredClone((s.enum as Json[])[Number(event.target.value)]))
          }
        >
          <option value={-1} disabled>
            选择一个值
          </option>
          {s.enum.map((choice, i) => (
            <option value={i} key={i}>
              {Array.isArray(s.enumNames) && typeof s.enumNames[i] === 'string'
                ? String(s.enumNames[i])
                : JSON.stringify(choice)}
            </option>
          ))}
        </select>
      </label>
    )
  else if (node.alternatives.length) {
    const selected = branchIndex(node, value)
    control = (
      <fieldset className={cx('task-schema-form__group')}>
        <legend>{name}</legend>
        <label>
          <span>{name} 分支</span>
          <select
            {...common}
            value={selected}
            onChange={(event) =>
              onChange(initialValue(node.alternatives[Number(event.target.value)]))
            }
          >
            <option value={-1} disabled>
              选择分支
            </option>
            {node.alternatives.map((branch, i) => (
              <option value={i} key={i}>
                {variantLabel(branch, i)}
              </option>
            ))}
          </select>
        </label>
        {selected >= 0 && (
          <SchemaTree
            node={node.alternatives[selected]}
            value={value}
            onChange={onChange}
            label={name}
            path={path}
            errors={errors}
            disabled={disabled}
            suggestions={suggestions}
            depth={depth + 1}
          />
        )}
      </fieldset>
    )
  } else if (s.type === 'object' || (s.type === undefined && object(value))) {
    const fields = object(value) ? value : {}
    const keys = [...new Set([...Object.keys(node.properties), ...Object.keys(fields)])]
    control = (
      <fieldset className={cx('task-schema-form__group')}>
        <legend>{name}</legend>
        {!object(value) && (
          <button
            type="button"
            className={cx('button')}
            disabled={disabled}
            onClick={() => onChange(initialValue(node))}
          >
            使用对象
          </button>
        )}
        {rows(keys).map((key) => {
          const required = Array.isArray(s.required) && s.required.includes(key)
          const included = Object.hasOwn(fields, key)
          const spec = node.properties[key] ?? node.additional ?? unconstrained
          return (
            <div key={key} className={cx('task-schema-form__item')}>
              {(!required || !included) && (
                <label className={cx('task-schema-form__include')}>
                  <input
                    type="checkbox"
                    checked={included}
                    disabled={disabled}
                    onChange={(event) =>
                      onChange(
                        event.target.checked
                          ? { ...fields, [key]: initialValue(spec) }
                          : cloneWithout(fields, key),
                      )
                    }
                  />
                  <span>
                    包含 {key}
                    {required ? '（必填）' : ''}
                  </span>
                </label>
              )}
              {included &&
                child(
                  spec,
                  fields[key],
                  (next) => onChange({ ...fields, [key]: next }),
                  key,
                  `${key}${required ? '（必填）' : ''}`,
                )}
            </div>
          )
        })}
        {pagination(keys.length)}
        {s.additionalProperties !== false && (
          <div className={cx('actions')}>
            <label>
              <span>{name} 新字段</span>
              <input
                {...common}
                value={newKey}
                maxLength={256}
                onChange={(event) => setNewKey(event.target.value)}
              />
            </label>
            <button
              type="button"
              className={cx('button')}
              disabled={
                disabled ||
                !newKey ||
                Object.hasOwn(fields, newKey) ||
                keys.length >= TREE_LIMITS.properties
              }
              onClick={() => {
                onChange({ ...fields, [newKey]: initialValue(node.additional ?? unconstrained) })
                setNewKey('')
                setPage(Math.floor(keys.length / windowSize))
              }}
            >
              添加字段
            </button>
          </div>
        )}
      </fieldset>
    )
  } else if (s.type === 'array' || (s.type === undefined && Array.isArray(value))) {
    const values = Array.isArray(value) ? value : []
    const maximum = Math.min(
      TREE_LIMITS.items,
      typeof s.maxItems === 'number' ? s.maxItems : TREE_LIMITS.items,
    )
    control = (
      <fieldset className={cx('task-schema-form__group')}>
        <legend>{name}</legend>
        <p className={cx('body-copy')}>
          {String(s.minItems ?? 0)}–{maximum} 项，保留原始顺序。
        </p>
        {values.slice(page * windowSize, (page + 1) * windowSize).map((item, at) => {
          const i = page * windowSize + at
          return (
            <div key={i} className={cx('task-schema-form__item')}>
              {child(
                node.items ?? unconstrained,
                item,
                (next) => onChange(values.map((v, j) => (j === i ? next : v))),
                i,
                `${name} 第 ${i + 1}`,
              )}
              <div className={cx('actions')}>
                <button
                  type="button"
                  className={cx('button')}
                  disabled={disabled}
                  onClick={() => onChange(values.filter((_, j) => i !== j))}
                >
                  删除 {name} 项 {i + 1}
                </button>
                {i > 0 && (
                  <button
                    type="button"
                    className={cx('button')}
                    disabled={disabled}
                    onClick={() => {
                      const next = [...values]
                      ;[next[i - 1], next[i]] = [next[i], next[i - 1]]
                      onChange(next)
                    }}
                  >
                    移动 {name} 项 {i + 1} 上移
                  </button>
                )}
              </div>
            </div>
          )
        })}
        {pagination(values.length)}
        <button
          type="button"
          className={cx('button')}
          disabled={disabled || values.length >= maximum}
          onClick={() => {
            onChange([...values, initialValue(node.items ?? unconstrained)])
            setPage(Math.floor(values.length / windowSize))
          }}
        >
          添加 {name} 项
        </button>
      </fieldset>
    )
  } else if (s.type === 'null') control = <p>{name}: null</p>
  else if (s.type === 'boolean' || (s.type === undefined && typeof value === 'boolean'))
    control = (
      <label>
        <span>{name}</span>
        <select
          {...common}
          value={String(value)}
          onChange={(event) => onChange(event.target.value === 'true')}
        >
          <option value="false">否</option>
          <option value="true">是</option>
        </select>
      </label>
    )
  else if (
    s.type === 'number' ||
    s.type === 'integer' ||
    (s.type === undefined && typeof value === 'number')
  )
    control = (
      <NumberField
        key={path}
        value={typeof value === 'number' ? value : Number.NaN}
        label={name}
        disabled={disabled}
        error={error ? validationMessage(error) : undefined}
        onChange={onChange}
      />
    )
  else if (s.type === 'string' || (s.type === undefined && typeof value === 'string')) {
    const options = suggestions?.(path, node) ?? []
    control = (
      <label>
        <span>{name}</span>
        {options.length ? (
          <>
            <input
              {...common}
              list={`${id}-choices`}
              value={typeof value === 'string' ? value : ''}
              maxLength={TREE_LIMITS.stringBytes + 1}
              onChange={(event) => onChange(event.target.value)}
            />
            <datalist id={`${id}-choices`}>
              {options.map((choice) => (
                <option value={choice} key={choice} />
              ))}
            </datalist>
          </>
        ) : (
          <textarea
            {...common}
            rows={2}
            value={typeof value === 'string' ? value : ''}
            maxLength={TREE_LIMITS.stringBytes + 1}
            onChange={(event) => onChange(event.target.value)}
          />
        )}
      </label>
    )
  } else control = <p>{name}: null</p>
  return (
    <div className={cx('task-schema-field')} data-field-path={path}>
      {s.type === undefined &&
        !node.alternatives.length &&
        !Array.isArray(s.enum) &&
        !Object.hasOwn(s, 'const') && (
          <label>
            <span>{name} JSON 类型</span>
            <select
              {...common}
              value={value === null ? 'null' : Array.isArray(value) ? 'array' : typeof value}
              onChange={(event) =>
                onChange(
                  (
                    {
                      object: {},
                      array: [],
                      string: '',
                      number: 0,
                      boolean: false,
                      null: null,
                    } as Record<string, Json>
                  )[event.target.value],
                )
              }
            >
              {['object', 'array', 'string', 'number', 'boolean', 'null'].map((type) => (
                <option key={type}>{type}</option>
              ))}
            </select>
          </label>
        )}
      {control}
      {typeof s.description === 'string' && (
        <p className={cx('body-copy muted')}>{s.description}</p>
      )}
      {error && (
        <p id={`${id}-error`} className={cx('task-schema-form__error')} role="alert">
          {error ? validationMessage(error) : null}
        </p>
      )}
    </div>
  )
}

function NumberField({
  value,
  label,
  disabled,
  error,
  onChange,
}: {
  value: number
  label: string
  disabled: boolean
  error?: string
  onChange(value: number): void
}) {
  const [draft, setDraft] = useState({ value, raw: String(value) })
  const raw = Object.is(draft.value, value) ? draft.raw : String(value)
  const valid =
    /^-?(0|[1-9][0-9]*)(\.[0-9]+)?([eE][+-]?[0-9]+)?$/.test(raw) && Number.isFinite(Number(raw))
  return (
    <label>
      <span>{label}</span>
      <input
        type="text"
        inputMode="decimal"
        value={raw}
        maxLength={129}
        disabled={disabled}
        aria-invalid={Boolean(error) || !valid}
        onChange={(event) => {
          const next = event.target.value
          const number = /^-?(0|[1-9][0-9]*)(\.[0-9]+)?([eE][+-]?[0-9]+)?$/.test(next)
            ? Number(next)
            : Number.NaN
          setDraft({ value: number, raw: next })
          // An invalid draft must never silently submit the previously valid number.
          onChange(number)
        }}
      />
      {!valid && (
        <span className={cx('task-schema-form__error')} role="alert">
          请输入完整的 JSON 数字。
        </span>
      )}
    </label>
  )
}
