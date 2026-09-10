import sharedStyles from '../../shared/ui/Primitives.module.css'
import { classNames } from '../../shared/ui/class-names.ts'
import localStyles from './Runs.module.css'
const cx = classNames(sharedStyles, localStyles)
import { useEffect, useRef, useState } from 'react'
import { PlatformClient, PlatformProblem } from '../../shared/api/client'
import { newReceipt } from '../../shared/api/security'
import type { JsonObject } from '../../shared/api/types'
import { parseDocument } from 'yaml'

export function RunSignal({
  client,
  runId,
  onAccepted,
  onPermissionLost,
}: {
  client: PlatformClient
  runId: string
  onAccepted(): void
  onPermissionLost(): void
}) {
  const [key, setKey] = useState('')
  const [withPayload, setWithPayload] = useState(false)
  const [value, setValue] = useState('{}')
  const [schemaDigest, setSchemaDigest] = useState('')
  const [classification, setClassification] = useState('internal')
  const [busy, setBusy] = useState(false)
  const [message, setMessage] = useState('')
  const pending = useRef<AbortController | null>(null)
  const receipt = useRef<{ intent: string; key: string } | null>(null)
  useEffect(
    () => () => {
      pending.current?.abort()
      receipt.current = null
    },
    [client, runId],
  )
  const send = async () => {
    if (busy) return
    let body: JsonObject
    try {
      if (!/^[a-z][a-z0-9_]{0,63}$/.test(key))
        throw new Error(
          'Signal key must start with a lowercase letter and use up to 64 lowercase letters, digits or underscores.',
        )
      if (withPayload && !/^sha256:[0-9a-f]{64}$/.test(schemaDigest))
        throw new Error('Supply the exact payload schema digest declared by this signal wait.')
      if (withPayload && parseDocument(value, { uniqueKeys: true, strict: true }).errors.length)
        throw new Error('Signal payload must have distinct JSON property names.')
      body = {
        payload: withPayload
          ? {
              classification,
              schema_digest: schemaDigest,
              value: { kind: 'inline', value: JSON.parse(value) },
            }
          : null,
      }
      if (new TextEncoder().encode(JSON.stringify(body)).length > 65_536)
        throw new Error('Signal submission exceeds this editor’s 65536-byte limit.')
    } catch (error) {
      setMessage(error instanceof Error ? error.message : 'Signal input is invalid.')
      return
    }
    const intent = JSON.stringify({ runId, key, body })
    if (receipt.current?.intent !== intent)
      receipt.current = { intent, key: newReceipt(`run-signal-${runId}-${key}`) }
    pending.current?.abort()
    const controller = new AbortController()
    pending.current = controller
    setBusy(true)
    setMessage('')
    try {
      await client.signalRun(runId, key, body, receipt.current.key, { signal: controller.signal })
      if (controller.signal.aborted) return
      setMessage('信号已接受，正在刷新运行状态。')
      onAccepted()
    } catch (error) {
      if (controller.signal.aborted) return
      if (error instanceof PlatformProblem && [401, 403].includes(error.status)) {
        setValue('{}')
        setKey('')
        setSchemaDigest('')
        receipt.current = null
        onPermissionLost()
      }
      setMessage(
        error instanceof Error
          ? error.message
          : 'Signal was not accepted. Retry preserves the same request identity.',
      )
    } finally {
      if (!controller.signal.aborted) setBusy(false)
    }
  }
  return (
    <details data-ui="nested-panel" className={cx('nested-panel')}>
      <summary>发送运行信号</summary>
      <form
        onSubmit={(event) => {
          event.preventDefault()
          void send()
        }}
      >
        <fieldset
          disabled={busy}
          className={cx('form-grid')}
          style={{ border: 0, padding: 0, margin: 0 }}
        >
          <label>
            <span>信号名称</span>
            <input
              value={key}
              maxLength={64}
              onChange={(event) => setKey(event.target.value)}
              required
            />
          </label>
          <label>
            <span>包含结构化数据</span>
            <input
              type="checkbox"
              checked={withPayload}
              onChange={(event) => setWithPayload(event.target.checked)}
            />
          </label>
          {withPayload && (
            <>
              <label>
                <span>信号 Schema 摘要</span>
                <input
                  value={schemaDigest}
                  maxLength={71}
                  onChange={(event) => setSchemaDigest(event.target.value)}
                  required
                />
              </label>
              <label>
                <span>信号数据分类</span>
                <select
                  value={classification}
                  onChange={(event) => setClassification(event.target.value)}
                >
                  {['public', 'internal', 'confidential', 'restricted'].map((item) => (
                    <option key={item}>{item}</option>
                  ))}
                </select>
              </label>
              <label className={cx('field--wide')}>
                <span>信号数据 JSON</span>
                <textarea
                  value={value}
                  maxLength={65_536}
                  rows={6}
                  spellCheck={false}
                  onChange={(event) => setValue(event.target.value)}
                />
              </label>
            </>
          )}
          <button className={cx('button')} type="submit">
            {busy ? 'Sending signal…' : 'Send signal'}
          </button>
        </fieldset>
      </form>
      <p className={cx('body-copy')}>
        使用当前 Plan 声明的信号。服务端检查数据结构与当前权限；相同输入重试保留请求身份。
      </p>
      <p role="status" aria-live="polite">
        {message}
      </p>
    </details>
  )
}
