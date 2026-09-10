import { useEffect, useState } from 'react'
import type { Json, JsonObject } from '../../shared/api/types'
import { SchemaTree } from '../../shared/schema/SchemaTree'
import { initialValue, object, treeSchema, validateTree } from '../../shared/schema/tree'
import type { TreeSchema } from '../../shared/schema/tree'
import { pinnedNominalSchemas } from '../../shared/schema/nominals'
import { CodeEditor } from '../../shared/ui/CodeEditor'
import styles from '../../shared/ui/Primitives.module.css'
import { classNames } from '../../shared/ui/class-names'
const cx = classNames(styles)

export function RunInput({
  schema,
  disabled,
  onSubmit,
}: {
  schema: JsonObject
  disabled: boolean
  onSubmit(value: JsonObject): Promise<void>
}) {
  const [node, setNode] = useState<TreeSchema | null>(null)
  const [value, setValue] = useState<Json>(null)
  const [source, setSource] = useState('{}')
  const [advanced, setAdvanced] = useState(false)
  const [error, setError] = useState('')
  const [errors, setErrors] = useState<Record<string, string>>({})
  useEffect(() => {
    let active = true
    void pinnedNominalSchemas()
      .then((nominals) => {
        if (!object(schema.schema)) throw new Error('已发布的输入 Schema 不完整。')
        const prepared = treeSchema(schema.schema, nominals)
        if (active) {
          const initial = initialValue(prepared)
          setNode(prepared)
          setValue(initial)
          setSource(JSON.stringify(initial, null, 2))
        }
      })
      .catch((failure) => {
        if (active) setError(failure instanceof Error ? failure.message : '无法读取输入表单。')
      })
    return () => {
      active = false
    }
  }, [schema])
  return (
    <form
      className={cx('stack')}
      onSubmit={(event) => {
        event.preventDefault()
        if (!node || disabled) return
        try {
          const input: unknown = advanced ? JSON.parse(source) : value
          if (!object(input)) throw new Error('运行输入必须是 JSON 对象。')
          const validation = validateTree(node, input)
          setErrors(validation)
          if (Object.keys(validation).length) {
            setError('请补齐输入字段并修正校验错误。')
            return
          }
          setError('')
          void onSubmit(input)
        } catch {
          setError('请输入合法的 JSON 对象。')
        }
      }}
    >
      <div>
        <button
          type="button"
          className={cx('button')}
          disabled={!node || disabled}
          onClick={() => {
            try {
              if (advanced) {
                const next: unknown = JSON.parse(source)
                if (!object(next)) throw new Error()
                setValue(next)
              } else setSource(JSON.stringify(value, null, 2))
              setAdvanced(!advanced)
              setError('')
            } catch {
              setError('请先修正 JSON，再切换到表单。')
            }
          }}
        >
          {advanced ? '使用输入表单' : '高级 JSON 输入'}
        </button>
      </div>
      {advanced ? (
        <CodeEditor
          label="运行输入 JSON"
          language="json"
          value={source}
          onChange={setSource}
          disabled={disabled}
        />
      ) : node ? (
        <SchemaTree
          node={node}
          value={value}
          onChange={setValue}
          label="运行输入"
          errors={errors}
          disabled={disabled}
        />
      ) : (
        <p role="status">正在准备输入表单…</p>
      )}
      {error && (
        <p role="alert" data-ui="notice--error" className={cx('notice notice--error')}>
          {error}
        </p>
      )}
      <button className={cx('button button--primary')} disabled={disabled || !node}>
        开始运行
      </button>
    </form>
  )
}
