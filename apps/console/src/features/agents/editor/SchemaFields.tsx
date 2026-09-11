import { useRef, useState } from 'react'
import { CodeEditor } from '../../../shared/ui/CodeEditor'
import {
  changeField,
  fieldTableSupported,
  isObject,
  newField,
  parseSchema,
} from './schema-document'
import type { SchemaObject } from './schema-document'
import styles from './SchemaFields.module.css'
import sharedStyles from '../../../shared/ui/Primitives.module.css'

export function SchemaFields({
  label,
  source,
  onChange,
  disabled,
}: {
  label: string
  source: string
  onChange(source: string): void
  disabled: boolean
}) {
  const [advanced, setAdvanced] = useState(false)
  const section = useRef<HTMLElement>(null)
  let schema: SchemaObject | null = null
  try {
    schema = parseSchema(source)
  } catch {
    /* Keep invalid source in the advanced editor. */
  }
  const supported = schema !== null && fieldTableSupported(schema)
  return (
    <section ref={section} className={styles.section} aria-label={label}>
      <header className={styles.heading}>
        <div>
          <h3>{label}</h3>
          <p>定义智能体接收或返回的数据字段。</p>
        </div>
        <button
          type="button"
          className={sharedStyles.button}
          disabled={disabled}
          onClick={() => {
            const invalid = section.current?.querySelector<HTMLElement>('[aria-invalid="true"]')
            if (invalid) invalid.focus()
            else setAdvanced(!advanced)
          }}
        >
          {advanced ? '字段表格' : '高级 JSON'}
        </button>
      </header>
      {advanced || !supported ? (
        <>
          {!supported && <p role="status">此结构需使用高级编辑，原始约束已完整保留。</p>}
          <CodeEditor
            language="json"
            label={label + ' JSON'}
            value={source}
            onChange={onChange}
            disabled={disabled}
          />
        </>
      ) : (
        <ObjectFields
          schema={schema!}
          onChange={(next) => onChange(JSON.stringify(next, null, 2))}
          disabled={disabled}
          depth={0}
        />
      )}
    </section>
  )
}
function ObjectFields({
  schema,
  onChange,
  disabled,
  depth,
}: {
  schema: SchemaObject
  onChange(value: SchemaObject): void
  disabled: boolean
  depth: number
}) {
  const [error, setError] = useState('')
  const properties = schema.properties as SchemaObject
  const required = Array.isArray(schema.required) ? schema.required : []
  const update = (
    name: string,
    nextName: string,
    field: SchemaObject | null,
    mandatory: boolean,
  ) => {
    try {
      onChange(changeField(schema, name, nextName, field, mandatory))
      setError('')
      return ''
    } catch (failure) {
      const message = failure instanceof Error ? failure.message : '字段无法更新。'
      setError(message)
      return message
    }
  }
  return (
    <div className={styles.fields}>
      <div className={styles.tableScroll}>
        <table>
          <thead>
            <tr>
              <th>字段名称</th>
              <th>类型</th>
              <th>必填</th>
              <th>说明</th>
              <th>操作</th>
            </tr>
          </thead>
          <tbody>
            {Object.entries(properties).map(([name, raw]) => (
              <FieldRow
                key={name}
                name={name}
                field={isObject(raw) ? raw : null}
                required={required.includes(name)}
                disabled={disabled}
                onChange={(nextName, field, mandatory) => update(name, nextName, field, mandatory)}
              />
            ))}
          </tbody>
        </table>
      </div>
      {Object.entries(properties).map(
        ([name, raw]) =>
          isObject(raw) && (
            <details className={styles.constraints} key={name}>
              <summary>{name} · 约束与嵌套字段</summary>
              <Constraints
                field={raw}
                disabled={disabled}
                onChange={(field) => update(name, name, field, required.includes(name))}
              />
              {depth < 8 && fieldTableSupported(raw) && (
                <ObjectFields
                  schema={raw}
                  onChange={(field) => update(name, name, field, required.includes(name))}
                  disabled={disabled}
                  depth={depth + 1}
                />
              )}
              {depth < 8 &&
                raw.type === 'array' &&
                isObject(raw.items) &&
                fieldTableSupported(raw.items) && (
                  <ObjectFields
                    schema={raw.items}
                    onChange={(items) =>
                      update(name, name, { ...raw, items }, required.includes(name))
                    }
                    disabled={disabled}
                    depth={depth + 1}
                  />
                )}
            </details>
          ),
      )}
      {error && <p role="alert">{error}</p>}
      <button
        className={sharedStyles.button}
        type="button"
        disabled={disabled || Object.keys(properties).length >= 128}
        onClick={() => {
          let suffix = 1
          while (Object.hasOwn(properties, 'field_' + suffix)) suffix++
          update('', 'field_' + suffix, newField(), true)
        }}
      >
        ＋ 添加字段
      </button>
    </div>
  )
}
function FieldRow({
  name,
  field,
  required,
  disabled,
  onChange,
}: {
  name: string
  field: SchemaObject | null
  required: boolean
  disabled: boolean
  onChange(name: string, field: SchemaObject | null, required: boolean): string
}) {
  const [draftName, setDraftName] = useState(name)
  const [nameError, setNameError] = useState('')
  const type = typeof field?.type === 'string' ? field.type : 'advanced'
  return (
    <tr>
      <td>
        <input
          aria-label={name + ' 字段名称'}
          value={draftName}
          aria-invalid={Boolean(nameError)}
          disabled={disabled || !field}
          maxLength={128}
          onChange={(event) => {
            setDraftName(event.target.value)
            setNameError(event.target.value.trim() ? '' : '字段名称不能为空。')
          }}
          onBlur={() => {
            if (field && draftName !== name) setNameError(onChange(draftName, field, required))
          }}
        />
      </td>
      <td>
        <select
          aria-label={name + ' 类型'}
          value={type}
          disabled={disabled || !field}
          onChange={(event) => {
            if (!field) return
            // Type conversion is explicit; incompatible constraints remain visible for final validation.
            onChange(
              name,
              { ...newField(event.target.value), ...field, type: event.target.value },
              required,
            )
          }}
        >
          {[
            ['string', '文本'],
            ['number', '数字'],
            ['integer', '整数'],
            ['boolean', '布尔值'],
            ['object', '对象'],
            ['array', '数组'],
            ['null', '空值'],
            ['advanced', '高级结构'],
          ].map(([value, label]) => (
            <option key={value} value={value} disabled={value === 'advanced'}>
              {label}
            </option>
          ))}
        </select>
      </td>
      <td>
        <input
          aria-label={name + ' 必填'}
          type="checkbox"
          checked={required}
          disabled={disabled || !field}
          onChange={(event) => field && onChange(name, field, event.target.checked)}
        />
      </td>
      <td>
        <input
          aria-label={name + ' 说明'}
          value={typeof field?.description === 'string' ? field.description : ''}
          disabled={disabled || !field}
          onChange={(event) =>
            field && onChange(name, { ...field, description: event.target.value }, required)
          }
        />
      </td>
      <td>
        <button
          type="button"
          className={sharedStyles.button}
          disabled={disabled}
          onClick={() => onChange(name, null, false)}
        >
          删除
        </button>
      </td>
    </tr>
  )
}
function Constraints({
  field,
  onChange,
  disabled,
}: {
  field: SchemaObject
  onChange(field: SchemaObject): void
  disabled: boolean
}) {
  const numeric =
    field.type === 'string'
      ? [
          ['minLength', '最短长度'],
          ['maxLength', '最长长度'],
          ['x-platform-max-bytes', '最大字节数'],
        ]
      : field.type === 'array'
        ? [
            ['minItems', '最少项数'],
            ['maxItems', '最多项数'],
          ]
        : ['number', 'integer'].includes(String(field.type))
          ? [
              ['minimum', '最小值'],
              ['maximum', '最大值'],
            ]
          : []
  return (
    <div className={styles.constraintGrid}>
      {numeric.map(([key, label]) => (
        <label key={key}>
          <span>{label}</span>
          <input
            type="number"
            value={typeof field[key] === 'number' ? field[key] : ''}
            disabled={disabled}
            onChange={(event) => {
              const next = { ...field }
              if (event.target.value === '') delete next[key]
              else next[key] = Number(event.target.value)
              onChange(next)
            }}
          />
        </label>
      ))}
      <JsonConstraint
        key={'const:' + JSON.stringify(field.const)}
        name="固定值（JSON）"
        value={field.const}
        disabled={disabled}
        onChange={(value) => {
          const next = { ...field }
          if (value === undefined) delete next.const
          else next.const = value
          onChange(next)
        }}
      />
      <JsonConstraint
        key={'enum:' + JSON.stringify(field.enum)}
        name="可选值（JSON 数组）"
        value={field.enum}
        disabled={disabled}
        array
        onChange={(value) => {
          const next = { ...field }
          if (value === undefined) delete next.enum
          else next.enum = value
          onChange(next)
        }}
      />
      {field.type === 'array' && (
        <label>
          <span>数组元素类型</span>
          <select
            disabled={disabled}
            value={isObject(field.items) ? String(field.items.type ?? 'advanced') : 'advanced'}
            onChange={(event) =>
              onChange({
                ...field,
                items: {
                  ...newField(event.target.value),
                  ...(isObject(field.items) ? field.items : {}),
                  type: event.target.value,
                },
              })
            }
          >
            {[
              ['string', '文本'],
              ['number', '数字'],
              ['integer', '整数'],
              ['boolean', '布尔值'],
              ['object', '对象'],
              ['advanced', '高级结构'],
            ].map(([value, label]) => (
              <option key={value} value={value} disabled={value === 'advanced'}>
                {label}
              </option>
            ))}
          </select>
        </label>
      )}
    </div>
  )
}
function JsonConstraint({
  name,
  value,
  onChange,
  disabled,
  array = false,
}: {
  name: string
  value: unknown
  onChange(value: unknown): void
  disabled: boolean
  array?: boolean
}) {
  const [source, setSource] = useState(value === undefined ? '' : JSON.stringify(value))
  const [error, setError] = useState('')
  const validate = (text: string) => {
    try {
      const next: unknown = text.trim() ? JSON.parse(text) : undefined
      if (array && next !== undefined && !Array.isArray(next)) throw new Error()
      return ''
    } catch {
      return array ? '请输入合法的 JSON 数组。' : '请输入合法的 JSON 值。'
    }
  }
  return (
    <label>
      <span>{name}</span>
      <input
        value={source}
        disabled={disabled}
        aria-invalid={Boolean(error)}
        onChange={(event) => {
          setSource(event.target.value)
          setError(validate(event.target.value))
        }}
        onBlur={() => {
          try {
            const next: unknown = source.trim() ? JSON.parse(source) : undefined
            if (array && next !== undefined && !Array.isArray(next)) throw new Error()
            onChange(next)
            setError('')
          } catch {
            setError(array ? '请输入合法的 JSON 数组。' : '请输入合法的 JSON 值。')
          }
        }}
      />
      {error && <small role="alert">{error}</small>}
    </label>
  )
}
