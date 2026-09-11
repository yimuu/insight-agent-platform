import sharedStyles from '../../shared/ui/Primitives.module.css'
import { classNames } from '../../shared/ui/class-names.ts'
import localStyles from './Runs.module.css'
const cx = classNames(sharedStyles, localStyles)
import { useEffect, useRef, useState } from 'react'
import type { PlatformClient } from '../../shared/api/client'
import type { ArtifactRef, JsonObject } from '../../shared/api/types'
import { readExactArtifact } from '../../shared/api/artifact-content'

/** Used only after an explicit authorized content response. React renders text, never HTML. */
export function AuthorizedContent({
  client,
  content,
  onError,
}: {
  client: PlatformClient
  content: JsonObject
  onError(error: unknown): void
}) {
  const [busy, setBusy] = useState(false)
  const pending = useRef<AbortController | null>(null)
  useEffect(() => () => pending.current?.abort(), [client, content])
  const value = content.value as JsonObject | undefined
  const artifact =
    value !== null &&
    typeof value === 'object' &&
    !Array.isArray(value) &&
    value.kind === 'artifact'
      ? (value.artifact as unknown as ArtifactRef)
      : null
  const download = async () => {
    if (!artifact || busy) return
    const controller = new AbortController()
    pending.current = controller
    setBusy(true)
    try {
      const blob = await readExactArtifact(client, artifact, {
        signal: controller.signal,
        maximumBytes: 1_073_741_824,
      })
      if (controller.signal.aborted) return
      const url = URL.createObjectURL(blob)
      try {
        const link = document.createElement('a')
        link.href = url
        link.download = artifact.artifact_id
        link.click()
      } finally {
        URL.revokeObjectURL(url)
      }
    } catch (error) {
      if (!controller.signal.aborted) onError(error)
    } finally {
      if (!controller.signal.aborted) setBusy(false)
    }
  }
  const inline = value?.kind === 'inline' ? value.value : undefined
  const answer =
    inline && typeof inline === 'object' && !Array.isArray(inline) && 'answer' in inline
      ? inline.answer
      : undefined
  return (
    <>
      {inline !== undefined && (
        <div className={cx('answer')}>
          {typeof answer === 'string' ? (
            <p>{answer}</p>
          ) : (
            <pre>{JSON.stringify(inline, null, 2)}</pre>
          )}
        </div>
      )}
      <details>
        <summary>结果数据与诊断</summary>
        <pre>{JSON.stringify(content, null, 2)}</pre>
      </details>
      {artifact && (
        <button className={cx('button')} disabled={busy} onClick={() => void download()}>
          {busy ? '正在读取授权文件…' : '下载授权文件内容'}
        </button>
      )}
    </>
  )
}
