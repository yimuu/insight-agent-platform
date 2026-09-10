import sharedStyles from '../../shared/ui/Primitives.module.css'
import { classNames } from '../../shared/ui/class-names.ts'
const cx = classNames(sharedStyles)

import { useEffect, useRef, useState } from 'react'

import { PlatformClient } from '../../shared/api/client'

import type { ArtifactView, OperationView } from '../../shared/api/types'

import { Status, Metric, SearchForm } from '../../shared/ui/console-ui'
import { errorNotice, formatTime } from '../../shared/ui/feedback'
import type { Notice } from '../../shared/ui/feedback'
function useSelectionGeneration() {
  const generation = useRef(0)
  useEffect(
    () => () => {
      generation.current++
    },
    [],
  )
  return {
    next: () => ++generation.current,
    current: () => generation.current,
    accepts: (request: number) => request === generation.current,
  }
}

export function Settings({
  client,
  report,
  tenant,
  setTenant,
  ready,
  endpoint,
}: {
  client: PlatformClient | null
  report: (notice: Notice | null) => void
  tenant: string
  setTenant: (value: string) => void
  ready: boolean | null
  endpoint: string
}) {
  const [tenantLabel, setTenantLabel] = useState(tenant)
  const [diagnostic, setDiagnostic] = useState<'artifact' | 'operation'>('artifact')
  return (
    <section className={cx('stack')}>
      <article data-ui="panel" className={cx('panel')}>
        <div data-ui="panel__heading" className={cx('panel__heading')}>
          <div>
            <p data-ui="kicker" className={cx('kicker')}>
              当前会话
            </p>
            <h2>工作空间连接</h2>
          </div>
          <Status value={ready === null ? 'unchecked' : ready ? 'ready' : 'unavailable'} />
        </div>
        <dl className={cx('metrics')}>
          <Metric label="服务地址" value={endpoint} />
          <Metric label="协议版本" value="insight.platform/v1" />
          <Metric label="凭据存储" value="Memory only" />
        </dl>
        <label className={cx('secondary-field')}>
          <span>工作空间显示名称</span>
          <input
            value={tenantLabel}
            onChange={(event) => setTenantLabel(event.target.value)}
            onBlur={() => setTenant(tenantLabel)}
            maxLength={128}
          />
        </label>
      </article>
      <article data-ui="panel" className={cx('panel')}>
        <p data-ui="kicker" className={cx('kicker')}>
          发布检查
        </p>
        <h2>发布时检查可用能力</h2>
        <p className={cx('body-copy')}>
          编译前读取工作空间的当前模型及策略绑定。缺失或停用的能力会阻止发布，请先完成对应配置。
        </p>
      </article>
      <article data-ui="panel" className={cx('panel')}>
        <div data-ui="panel__heading" className={cx('panel__heading')}>
          <div>
            <p data-ui="kicker" className={cx('kicker')}>
              高级诊断
            </p>
            <h2>文件与后台操作查询</h2>
          </div>
          <div className={cx('segmented')}>
            <button
              className={cx(diagnostic === 'artifact' ? 'active' : '')}
              onClick={() => setDiagnostic('artifact')}
            >
              文件
            </button>
            <button
              className={cx(diagnostic === 'operation' ? 'active' : '')}
              onClick={() => setDiagnostic('operation')}
            >
              后台操作
            </button>
          </div>
        </div>
        {diagnostic === 'artifact' ? (
          <Artifacts client={client} report={report} />
        ) : (
          <Operations client={client} report={report} />
        )}
      </article>
    </section>
  )
}

function Artifacts({
  client,
  report,
}: {
  client: PlatformClient | null
  report: (notice: Notice | null) => void
}) {
  const [id, setId] = useState('')
  const generation = useSelectionGeneration()
  const [artifact, setArtifact] = useState<ArtifactView | null>(null)
  const [busy, setBusy] = useState(false)
  const load = async () => {
    if (!client) return
    const request = generation.next()
    setBusy(true)
    try {
      const current = await client.getArtifact(id.trim())
      if (!generation.accepts(request)) return
      setArtifact(current.data)
    } catch (error) {
      if (generation.accepts(request)) report(errorNotice(error))
    } finally {
      if (generation.accepts(request)) setBusy(false)
    }
  }
  const download = async () => {
    if (!client || !artifact) return
    const request = generation.current()
    setBusy(true)
    try {
      const content = await client.downloadArtifact(artifact.artifact_id)
      if (!generation.accepts(request)) return
      if (content.blob.size !== artifact.expected_size_bytes)
        throw new Error('artifact_size_mismatch')
      const url = URL.createObjectURL(content.blob)
      const anchor = document.createElement('a')
      anchor.href = url
      anchor.download = artifact.artifact_id
      anchor.click()
      URL.revokeObjectURL(url)
    } catch (error) {
      if (generation.accepts(request)) report(errorNotice(error))
    } finally {
      if (generation.accepts(request)) setBusy(false)
    }
  }
  return (
    <div data-ui="nested-panel" className={cx('nested-panel')}>
      <SearchForm
        label="文件 ID"
        placeholder="art_…"
        value={id}
        onChange={(value) => {
          generation.next()
          setId(value)
          setArtifact(null)
          setBusy(false)
        }}
        onSubmit={load}
        busy={busy}
      />
      {artifact && (
        <>
          <dl className={cx('metrics')}>
            <Metric label="文件 ID" value={artifact.artifact_id} mono />
            <Metric label="状态" value={artifact.state} />
            <Metric label="用途" value={artifact.purpose} />
            <Metric label="大小" value={`${artifact.expected_size_bytes} bytes`} />
          </dl>
          <button
            className={cx('button button--primary')}
            disabled={artifact.state !== 'ready'}
            onClick={download}
          >
            授权下载
          </button>
        </>
      )}
    </div>
  )
}

function Operations({
  client,
  report,
}: {
  client: PlatformClient | null
  report: (notice: Notice | null) => void
}) {
  const [id, setId] = useState('')
  const generation = useSelectionGeneration()
  const [operation, setOperation] = useState<OperationView | null>(null)
  const [busy, setBusy] = useState(false)
  const load = async () => {
    if (!client) return
    const request = generation.next()
    setBusy(true)
    try {
      const current = await client.getOperation(id.trim())
      if (!generation.accepts(request)) return
      setOperation(current.data)
    } catch (error) {
      if (generation.accepts(request)) report(errorNotice(error))
    } finally {
      if (generation.accepts(request)) setBusy(false)
    }
  }
  return (
    <div data-ui="nested-panel" className={cx('nested-panel')}>
      <SearchForm
        label="后台操作 ID"
        placeholder="job_…"
        value={id}
        onChange={(value) => {
          generation.next()
          setId(value)
          setOperation(null)
          setBusy(false)
        }}
        onSubmit={load}
        busy={busy}
      />
      {operation && (
        <>
          <dl className={cx('metrics')}>
            <Metric label="状态" value={operation.state} />
            <Metric label="类型" value={operation.kind} />
            <Metric label="更新时间" value={formatTime(operation.updated_at)} />
          </dl>
          {operation.error && (
            <div data-ui="notice--error" className={cx('notice notice--error')}>
              {operation.error.message}
            </div>
          )}
        </>
      )}
    </div>
  )
}
