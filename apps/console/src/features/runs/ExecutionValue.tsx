import { useEffect, useState } from 'react'
import type { PlatformClient } from '../../shared/api/client'
import type { ArtifactRef, ExecutionDetail } from '../../shared/api/types'
import { readExactArtifact } from '../../shared/api/artifact-content'
import { executionValueSections } from './execution-value'
import styles from './ExecutionCanvas.module.css'

export function ExecutionValue({
  client,
  detail,
}: {
  client: PlatformClient
  detail: ExecutionDetail
}) {
  const [selected, setSelected] = useState(
    detail.output_value_id ?? detail.values[0]?.value_id ?? '',
  )
  const [body, setBody] = useState<unknown>(undefined)
  const [error, setError] = useState('')
  const [loading, setLoading] = useState(false)
  const [retry, setRetry] = useState(0)
  const allowed = detail.values.some((value) => value.value_id === selected)
  const choose = (id: string) => {
    if (id !== selected) {
      setBody(undefined)
      setSelected(id)
    }
  }
  useEffect(() => {
    const c = new AbortController()
    setBody(undefined)
    setError('')
    if (!allowed) {
      setLoading(false)
      return
    }
    setLoading(true)
    void client
      .getRunValueContent(detail.run_id, selected, { signal: c.signal })
      .then(async ({ data }) => {
        const reference = data.value as Record<string, unknown>
        let value: unknown
        if (reference?.kind === 'inline') value = reference.value
        else if (reference?.kind === 'artifact') {
          if (Number((reference.artifact as ArtifactRef)?.byte_length) > 131072)
            throw new Error('内容超过 128 KiB 预览上限，请通过运行数据查看。')
          const bytes = await readExactArtifact(client, reference.artifact as ArtifactRef, {
            signal: c.signal,
            maximumBytes: 131072,
          })
          value = JSON.parse(await bytes.text())
        } else throw new Error('该数据没有可读取的正文。')
        if (new TextEncoder().encode(JSON.stringify(value)).length > 131072)
          throw new Error('内容超过 128 KiB 预览上限，请通过运行数据查看。')
        if (!c.signal.aborted) setBody(value)
      })
      .catch((e: unknown) => {
        if (!c.signal.aborted) setError(e instanceof Error ? e.message : '无法读取此数据。')
      })
      .finally(() => {
        if (!c.signal.aborted) setLoading(false)
      })
    return () => c.abort()
  }, [client, detail.run_id, allowed, selected, retry])
  const label = (id: string, index: number) =>
    id === detail.input_value_id
      ? '模型输入'
      : id === detail.output_value_id
        ? '模型输出'
        : `关联数据 ${index + 1}`
  return (
    <section className={styles.valuePanel}>
      <h4>{detail.source_kind === 'model_turn' ? '模型输入与输出' : '节点关联数据'}</h4>
      {detail.values.length === 0 ? (
        <p>此节点没有可查看的独立数据。编排节点可能只负责流程衔接。</p>
      ) : (
        <>
          {detail.values.length > 4 ? (
            <label>
              选择关联数据
              <select value={selected} onChange={(e) => choose(e.target.value)}>
                {detail.values.map((value, index) => (
                  <option key={value.value_id} value={value.value_id}>
                    {label(value.value_id, index)} · {value.storage_kind}
                  </option>
                ))}
              </select>
            </label>
          ) : (
            <div className={styles.valueTabs}>
              {detail.values.map((value, index) => (
                <button
                  key={value.value_id}
                  aria-pressed={selected === value.value_id}
                  onClick={() => choose(value.value_id)}
                >
                  {label(value.value_id, index)}
                </button>
              ))}
            </div>
          )}
          {loading && <p role="status">正在读取节点数据…</p>}
          {error && (
            <p role="alert">
              {error} <button onClick={() => setRetry((n) => n + 1)}>重试读取</button>
            </p>
          )}
          {!loading && !error && allowed && body !== undefined && (
            <>
              {executionValueSections(
                body,
                detail.source_kind === 'model_turn' || detail.node_kind === 'model_loop',
              ).map((section, index) =>
                section.collapsed ? (
                  <details key={index}>
                    <summary>{section.label}</summary>
                    <pre>{section.text}</pre>
                  </details>
                ) : (
                  <div key={index}>
                    <h5>{section.label}</h5>
                    <pre>{section.text}</pre>
                  </div>
                ),
              )}
              <details>
                <summary>原始数据</summary>
                <p className={styles.identifier}>{selected}</p>
                <pre>{JSON.stringify(body, null, 2)}</pre>
              </details>
            </>
          )}
          {detail.values_truncated && <p>仅展示前 64 条关联数据。</p>}
        </>
      )}
    </section>
  )
}
