import { useEffect, useRef, useState } from 'react'
import { PlatformClient, PlatformProblem } from '../api/client'
import { newReceipt } from '../api/security'
import type { JsonObject } from '../api/types'
import { parseDocument } from 'yaml'

export function RunSignal({ client, runId, onAccepted, onPermissionLost }: {
  client: PlatformClient; runId: string; onAccepted(): void; onPermissionLost(): void
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
  useEffect(() => () => { pending.current?.abort(); receipt.current = null }, [client, runId])
  const send = async () => {
    if (busy) return
    let body: JsonObject
    try {
      if (!/^[a-z][a-z0-9_]{0,63}$/.test(key)) throw new Error('Signal key must start with a lowercase letter and use up to 64 lowercase letters, digits or underscores.')
      if (withPayload && !/^sha256:[0-9a-f]{64}$/.test(schemaDigest)) throw new Error('Supply the exact payload schema digest declared by this signal wait.')
      if (withPayload && parseDocument(value, { uniqueKeys: true, strict: true }).errors.length) throw new Error('Signal payload must have distinct JSON property names.')
      body = { payload: withPayload ? { classification, schema_digest: schemaDigest, value: { kind: 'inline', value: JSON.parse(value) } } : null }
      if (new TextEncoder().encode(JSON.stringify(body)).length > 65_536) throw new Error('Signal submission exceeds this editor’s 65536-byte limit.')
    } catch (error) { setMessage(error instanceof Error ? error.message : 'Signal input is invalid.'); return }
    const intent = JSON.stringify({ runId, key, body })
    if (receipt.current?.intent !== intent) receipt.current = { intent, key: newReceipt(`run-signal-${runId}-${key}`) }
    pending.current?.abort()
    const controller = new AbortController(); pending.current = controller
    setBusy(true); setMessage('')
    try {
      await client.signalRun(runId, key, body, receipt.current.key, { signal: controller.signal })
      if (controller.signal.aborted) return
      setMessage('Signal accepted. Current Run state is being refreshed.')
      onAccepted()
    } catch (error) {
      if (controller.signal.aborted) return
      if (error instanceof PlatformProblem && [401, 403].includes(error.status)) {
        setValue('{}'); setKey(''); setSchemaDigest(''); receipt.current = null; onPermissionLost()
      }
      setMessage(error instanceof Error ? error.message : 'Signal was not accepted. Retry preserves the same request identity.')
    } finally { if (!controller.signal.aborted) setBusy(false) }
  }
  return <details className="nested-panel"><summary>Send Run signal</summary>
    <form onSubmit={(event) => { event.preventDefault(); void send() }}>
      <fieldset disabled={busy} className="form-grid" style={{ border: 0, padding: 0, margin: 0 }}>
        <label><span>Signal key</span><input value={key} maxLength={64} onChange={(event) => setKey(event.target.value)} required /></label>
        <label><span>Include typed payload</span><input type="checkbox" checked={withPayload} onChange={(event) => setWithPayload(event.target.checked)} /></label>
        {withPayload && <><label><span>Signal payload schema digest</span><input value={schemaDigest} maxLength={71} onChange={(event) => setSchemaDigest(event.target.value)} required /></label>
          <label><span>Signal payload classification</span><select value={classification} onChange={(event) => setClassification(event.target.value)}>{['public', 'internal', 'confidential', 'restricted'].map((item) => <option key={item}>{item}</option>)}</select></label>
          <label className="field--wide"><span>Signal payload JSON</span><textarea value={value} maxLength={65_536} rows={6} spellCheck={false} onChange={(event) => setValue(event.target.value)} /></label></>}
        <button className="button" type="submit">{busy ? 'Sending signal…' : 'Send signal'}</button>
      </fieldset>
    </form><p className="body-copy">Use the signal declared by this Run’s Plan. The server checks its exact payload schema and current permission. Retrying unchanged input keeps the same request identity.</p>
    <p role="status" aria-live="polite">{message}</p>
  </details>
}
