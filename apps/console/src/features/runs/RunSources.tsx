import sharedStyles from '../../shared/ui/Primitives.module.css'
import { classNames } from '../../shared/ui/class-names.ts'
import localStyles from './Runs.module.css'
const cx = classNames(sharedStyles, localStyles)
import { useEffect, useRef, useState } from 'react'
import type { PlatformClient } from '../../shared/api/client'
import type { RunView } from '../../shared/api/types'
import type { CompiledAgent } from '../../shared/compiler/compiler'
import { frozenRunSources } from '../agents/published-run'
import { sourceLocations } from '../agents/plan-editor'

export function RunSources({ client, run }: { client: PlatformClient; run: RunView }) {
  const [compiled, setCompiled] = useState<CompiledAgent | null>(null)
  const [selected, setSelected] = useState('')
  const [error, setError] = useState('')
  const [busy, setBusy] = useState(false)
  const controller = useRef<AbortController | null>(null)
  useEffect(() => {
    let active = true
    controller.current?.abort()
    queueMicrotask(() => {
      if (active) {
        setCompiled(null)
        setSelected('')
        setError('')
        setBusy(false)
      }
    })
    return () => {
      active = false
      controller.current?.abort()
    }
  }, [client, run.run_id])
  const load = async () => {
    controller.current?.abort()
    const current = new AbortController()
    controller.current = current
    setCompiled(null)
    setSelected('')
    setError('')
    setBusy(true)
    try {
      const result = await frozenRunSources(client, run, current.signal)
      if (!current.signal.aborted) {
        setCompiled(result)
        setSelected(Object.keys(JSON.parse(result.typedPlan).nodes)[0] ?? '')
      }
    } catch (reason) {
      if (!current.signal.aborted) {
        setCompiled(null)
        setSelected('')
        setError(reason instanceof Error ? reason.message : '无法读取冻结源码。')
      }
    } finally {
      if (!current.signal.aborted) setBusy(false)
    }
  }
  const nodes = compiled ? Object.keys(JSON.parse(compiled.typedPlan).nodes) : []
  const locations = sourceLocations(compiled?.sourceMap, selected)
  return (
    <article data-ui="panel" className={cx('panel')}>
      <p data-ui="kicker" className={cx('kicker')}>
        运行源码
      </p>
      <h2>Plan 源码位置</h2>
      <p className={cx('body-copy')}>
        按当前内容权限读取本次运行的已发布源码。源码位置描述冻结的 Plan，执行进度请查看运行时间线。
      </p>
      <button className={cx('button')} onClick={() => void load()} disabled={busy}>
        {busy ? 'Verifying frozen source…' : '查看冻结源码'}
      </button>
      {error && (
        <p data-ui="notice--error" className={cx('notice notice--error')} role="alert">
          {error}
        </p>
      )}
      {compiled && (
        <>
          <label>
            <span>冻结 Plan 节点</span>
            <select value={selected} onChange={(event) => setSelected(event.target.value)}>
              {nodes.map((node) => (
                <option key={node}>{node}</option>
              ))}
            </select>
          </label>
          <ul>
            {locations.map((location, index) => (
              <li key={index}>
                <code>
                  {location.file}:{location.line}:{location.column}
                </code>{' '}
                · {location.kind} · <code>{location.source_pointer || '/'}</code>
              </li>
            ))}
          </ul>
          <details>
            <summary>已验证的源码身份</summary>
            <p>
              Plan <code>{compiled.typedPlanDigest}</code>
            </p>
            <p>
              源码映射 <code>{compiled.sourceMapDigest}</code>
            </p>
          </details>
        </>
      )}
    </article>
  )
}
