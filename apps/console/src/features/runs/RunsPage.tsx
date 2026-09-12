import { ExecutionCanvas } from './ExecutionCanvas'
import { Icon } from '../../shared/ui/Icon'
import { RunInput } from './RunInput'
import { displayState } from '../../shared/i18n/display'
import sharedStyles from '../../shared/ui/Primitives.module.css'
import { classNames } from '../../shared/ui/class-names.ts'
import localStyles from './Runs.module.css'
const cx = classNames(sharedStyles, localStyles)

import { RunValues } from './RunValues'
import { RunSignal } from './RunSignal'
import { RunSources } from './RunSources'
import { publishedRunDefaults } from '../agents/published-run'
import { AuthorizedContent } from './AuthorizedContent'

import type { RunEventHistory } from '../../shared/api/sse'

import { useEffect, useRef, useState } from 'react'

import { PlatformClient, PlatformProblem } from '../../shared/api/client'
import { utcTimestamp } from '../../shared/api/time'
import { discoverTaskIds, newReceipt, safeJson } from '../../shared/api/security'

import type {
  AgentSummary,
  JsonObject,
  RunEvent,
  RunSummary,
  RunView,
} from '../../shared/api/types'

import { Status, NoticeBox, Metric, SearchForm } from '../../shared/ui/console-ui'
import { errorNotice, formatTime } from '../../shared/ui/feedback'
import type { Notice } from '../../shared/ui/feedback'
const TERMINAL_RUNS = new Set(['succeeded', 'failed', 'cancelled', 'timed_out'])
export function Runs({
  client,
  report,
  launchAgent,
  onTask,
}: {
  client: PlatformClient | null
  report: (notice: Notice | null) => void
  launchAgent: AgentSummary | null
  onTask: (id: string) => void
}) {
  const [id, setId] = useState('')
  const [activeRunId, setActiveRunId] = useState('')
  const [run, setRun] = useState<RunView | null>(null)
  const [result, setResult] = useState<JsonObject | null>(null)
  const [events, setEvents] = useState<RunEvent[]>([])
  const [submittedInput, setSubmittedInput] = useState<JsonObject | null>(null)
  const [detailTab, setDetailTab] = useState<'canvas' | 'diagnostics'>('canvas')
  const [cursor, setCursor] = useState('')
  const [followError, setFollowError] = useState<Notice | null>(null)
  const [history, setHistory] = useState<RunEventHistory | null>(null)
  const followController = useRef<AbortController | null>(null)
  const refreshRun = useRef<() => void>(() => {})
  const selectionGeneration = useRef(0)
  const [busy, setBusy] = useState(false)
  const [listLoaded, setListLoaded] = useState(false)
  const [listFailed, setListFailed] = useState(false)
  const [agentOptions, setAgentOptions] = useState<AgentSummary[]>([])
  const [summaries, setSummaries] = useState<RunSummary[]>([])
  const [nextCursor, setNextCursor] = useState<string | null>(null)
  const [stateFilter, setStateFilter] = useState('')
  const [agentFilter, setAgentFilter] = useState('')
  const [runAgent, setRunAgent] = useState<AgentSummary | null>(launchAgent)
  const [runDefaults, setRunDefaults] = useState<Awaited<
    ReturnType<typeof publishedRunDefaults>
  > | null>(null)
  const createIntent = useRef<{ intent: string; body: JsonObject; receipt: string } | null>(null)
  const creating = useRef(false)

  useEffect(() => {
    if (!launchAgent || !client) return
    const controller = new AbortController()
    queueMicrotask(() => {
      if (!controller.signal.aborted) {
        setSubmittedInput(null)
        setRunAgent(launchAgent)
        clearSelection()
        setRunDefaults(null)
        createIntent.current = null
      }
    })
    publishedRunDefaults(client, launchAgent, controller.signal)
      .then((defaults) => {
        if (!controller.signal.aborted) setRunDefaults(defaults)
      })
      .catch((error: unknown) => {
        if (!controller.signal.aborted) report(errorNotice(error))
      })
    return () => {
      controller.abort()
    }
  }, [client, launchAgent, report])

  useEffect(() => {
    const controller = new AbortController()
    const { signal } = controller
    followController.current = controller
    const clear = () => {
      setSubmittedInput(null)
      setRun(null)
      setResult(null)
      setEvents([])
      setCursor('')
      setHistory(null)
      setBusy(false)
    }
    const invalidateSelection = () => {
      selectionGeneration.current++
    }
    if (!client || !activeRunId) {
      refreshRun.current = () => {}
      return () => {
        controller.abort()
      }
    }

    let reading = false
    let refreshRequested = false
    let lastEventId: unknown
    // Coalesce event bursts and serialize reads. Events only request an authority refresh.
    const readCurrentRun = async () => {
      refreshRequested = true
      if (reading) return
      reading = true
      try {
        while (refreshRequested && !signal.aborted) {
          refreshRequested = false
          const current = await client.getRun(activeRunId, { signal })
          if (signal.aborted) return
          setRun((existing) =>
            existing && existing.version > current.data.version ? existing : current.data,
          )
          if (TERMINAL_RUNS.has(current.data.state) && current.data.output_value_id !== null) {
            try {
              const output = await client.getRunResult(activeRunId, { signal })
              if (!signal.aborted) setResult(output.data)
            } catch (error) {
              if (!signal.aborted) setResult(null)
              if (!(error instanceof PlatformProblem) || error.status !== 409) throw error
            }
          } else {
            setResult(null)
          }
        }
      } catch (error) {
        if (!signal.aborted) {
          setResult(null)
          if (error instanceof PlatformProblem && [401, 403].includes(error.status)) {
            controller.abort()
            clear()
          }
          report(errorNotice(error))
        }
      } finally {
        reading = false
        if (!signal.aborted) setBusy(false)
      }
    }
    refreshRun.current = () => {
      if (signal.aborted) return
      setBusy(true)
      void readCurrentRun()
    }
    void client
      .followRunEvents(activeRunId, {
        signal,
        onClear: clear,
        onHistory(snapshot) {
          if (!signal.aborted)
            setHistory((previous) => ({
              ...snapshot,
              truncated: Boolean(previous?.truncated || snapshot.truncated),
            }))
        },
        onUpdate(snapshot) {
          if (signal.aborted) return
          setEvents([...snapshot.events])
          setCursor(snapshot.cursor ?? '')
          const nextEventId = snapshot.events.at(-1)?.data.event_id
          if (nextEventId !== lastEventId) {
            lastEventId = nextEventId
            void readCurrentRun()
          }
        },
      })
      .catch((error: unknown) => {
        if (signal.aborted) return
        if (error instanceof PlatformProblem && [401, 403].includes(error.status))
          controller.abort()
        const detail = error instanceof Error ? error.message : '无法继续读取运行事件。'
        setFollowError({
          tone: 'error',
          text: `已停止跟随时间线。${detail} 部分早期历史可能不可用，刷新可查看当前运行状态。`,
          traceId: error instanceof PlatformProblem ? error.traceId : null,
        })
        setBusy(false)
      })
    refreshRun.current()
    const onVisible = () => {
      if (document.visibilityState === 'visible') void readCurrentRun()
    }
    document.addEventListener('visibilitychange', onVisible)
    return () => {
      controller.abort()
      document.removeEventListener('visibilitychange', onVisible)
      refreshRun.current = () => {}
      invalidateSelection()
    }
  }, [activeRunId, client, report])

  const clearSelection = () => {
    selectionGeneration.current++
    followController.current?.abort()
    setActiveRunId('')
    setRun(null)
    setResult(null)
    setEvents([])
    setCursor('')
    setFollowError(null)
    setHistory(null)
    setBusy(false)
  }

  const loadList = async (pageCursor?: string) => {
    if (!client) return report({ tone: 'error', text: '请先连接工作空间。' })
    setBusy(true)
    setListFailed(false)
    report(null)
    try {
      const response = await client.listRuns({
        agentId: agentFilter || undefined,
        state: stateFilter || undefined,
        cursor: pageCursor,
      })
      setSummaries(response.data.items)
      setListLoaded(true)
      setNextCursor(response.data.next_cursor)
    } catch (error) {
      setListFailed(true)
      report(errorNotice(error))
    } finally {
      setBusy(false)
    }
  }

  useEffect(() => {
    if (!client) return
    let active = true
    void client
      .listAgents()
      .then((response) => {
        if (active) setAgentOptions(response.data.items)
      })
      .catch(() => {})
    void client
      .listRuns({})
      .then((response) => {
        if (active) {
          setSummaries(response.data.items)
          setListLoaded(true)
          setListFailed(false)
          setNextCursor(response.data.next_cursor)
        }
      })
      .catch((error) => {
        if (active) {
          setListFailed(true)
          report(errorNotice(error))
        }
      })
    return () => {
      active = false
    }
  }, [client, report])

  const load = (selectedId = id.trim()) => {
    if (!client) return report({ tone: 'error', text: '请先连接工作空间。' })
    if (!selectedId) return
    if (selectedId === activeRunId) {
      refreshRun.current()
      return
    }
    clearSelection()
    setSubmittedInput(null)
    setId(selectedId)
    setActiveRunId(selectedId)
    setBusy(true)
    report(null)
  }

  const create = async (value: JsonObject) => {
    if (!client || !runAgent || !runDefaults || creating.current) return
    creating.current = true
    const generation = selectionGeneration.current
    setBusy(true)
    report(null)
    try {
      const intent = JSON.stringify({
        agent: runAgent.agent_id,
        defaults: runDefaults,
        input: value,
      })
      if (createIntent.current?.intent !== intent)
        createIntent.current = {
          intent,
          receipt: newReceipt(`run-create-${runAgent.agent_id}`),
          body: {
            agent_id: runAgent.agent_id,
            expected_agent_deployment: runDefaults.exactDeployment,
            input: {
              classification: runDefaults.classification,
              schema_digest: runDefaults.schemaDigest,
              value: { kind: 'inline', value },
            },
            // oxlint-disable-next-line react/purity -- Freeze one deadline at the explicit submission intent.
            deadline: utcTimestamp(new Date(Date.now() + runDefaults.deadlineSeconds * 1000)),
          },
        }
      const response = await client.createRun(
        createIntent.current.body,
        createIntent.current.receipt,
      )
      if (generation !== selectionGeneration.current) return
      createIntent.current = null
      setSubmittedInput(value)
      setId(response.data.run_id)
      clearSelection()
      setActiveRunId(response.data.run_id)
      report({ tone: 'success', text: `${runAgent.name} 已开始运行。` })
    } catch (error) {
      if (generation === selectionGeneration.current) report(errorNotice(error))
    } finally {
      creating.current = false
      if (generation === selectionGeneration.current) setBusy(false)
    }
  }

  const act = async (action: 'pause' | 'resume' | 'cancel') => {
    if (!client || !run) return
    const controller = followController.current
    setBusy(true)
    report(null)
    try {
      const response = await client.runAction(
        run.run_id,
        action,
        run.etag,
        newReceipt(`run-${action}-${run.run_id}-v${run.version}`),
      )
      if (controller?.signal.aborted) return
      setRun((existing) =>
        existing && existing.version > response.data.version ? existing : response.data,
      )
      refreshRun.current()
      report({ tone: 'success', text: `操作已提交。`, traceId: response.traceId })
    } catch (error) {
      if (!controller?.signal.aborted) report(errorNotice(error))
    } finally {
      if (!controller?.signal.aborted) setBusy(false)
    }
  }
  const taskIds = discoverTaskIds(events)

  return (
    <section className={cx('stack')}>
      {(activeRunId || runAgent) && (
        <div className={cx('page-toolbar')}>
          <button
            className={cx('button')}
            onClick={() => {
              clearSelection()
              setRunAgent(null)
              setRunDefaults(null)
              setSubmittedInput(null)
              void loadList()
            }}
          >
            ← 返回运行记录
          </button>
          {run && <Status value={run.state} />}
        </div>
      )}
      {(activeRunId || runAgent) && (
        <div className={cx('debug-workspace')}>
          <article className={cx('debug-preview')}>
            <header className={cx('debug-heading')}>
              <div>
                <h2>{runAgent?.display_name ?? '运行预览'}</h2>
                <span>单次调试</span>
              </div>
              {activeRunId && runAgent && TERMINAL_RUNS.has(run?.state ?? '') && (
                <button
                  className={cx('button')}
                  onClick={() => {
                    clearSelection()
                    setSubmittedInput(null)
                  }}
                >
                  重新调试
                </button>
              )}
            </header>
            <div className={cx('debug-messages')}>
              {submittedInput && (
                <div className={cx('debug-user')}>
                  <small>你</small>
                  <pre>
                    {Object.keys(submittedInput).length === 1 &&
                    typeof Object.values(submittedInput)[0] === 'string'
                      ? String(Object.values(submittedInput)[0])
                      : JSON.stringify(submittedInput, null, 2)}
                  </pre>
                </div>
              )}
              {result && client ? (
                <div className={cx('debug-assistant')}>
                  <small>Agent</small>
                  <AuthorizedContent
                    client={client}
                    content={result}
                    onError={(error) => {
                      setResult(null)
                      report(errorNotice(error))
                    }}
                  />
                </div>
              ) : activeRunId ? (
                <div className={cx('debug-assistant')} role="status">
                  <small>Agent</small>
                  <p>
                    {run && TERMINAL_RUNS.has(run.state)
                      ? run.state === 'succeeded'
                        ? '运行已完成，没有可显示的输出。'
                        : `运行${displayState(run.state)}，请查看右侧执行详情。`
                      : '正在执行，可在右侧查看进度…'}
                  </p>
                </div>
              ) : (
                <div className={cx('debug-welcome')}>
                  <h3>试一试你的 Agent</h3>
                  <p>输入任务，查看回答和每一步执行过程。</p>
                </div>
              )}
            </div>
            {!activeRunId && runAgent && (
              <div className={cx('debug-composer')}>
                {runDefaults ? (
                  <RunInput schema={runDefaults.inputSchema} disabled={busy} onSubmit={create} />
                ) : (
                  <p role="status">正在准备输入…</p>
                )}
              </div>
            )}
            {run && !TERMINAL_RUNS.has(run.state) && (
              <div className={cx('debug-composer')}>
                <button
                  className={cx('button button--danger')}
                  onClick={() => act('cancel')}
                  disabled={busy || run.state === 'cancelling'}
                >
                  {run.state === 'cancelling' ? '正在停止…' : '停止运行'}
                </button>
              </div>
            )}
          </article>
          <section className={cx('debug-detail')}>
            <div className={cx('debug-tabs')} role="tablist" aria-label="调试详情">
              <button
                role="tab"
                id="execution-canvas-tab"
                aria-controls="execution-canvas-panel"
                aria-selected={detailTab === 'canvas'}
                onClick={() => setDetailTab('canvas')}
              >
                执行画布
              </button>
              <button
                role="tab"
                id="execution-diagnostics-tab"
                aria-controls="execution-diagnostics-panel"
                aria-selected={detailTab === 'diagnostics'}
                onClick={() => setDetailTab('diagnostics')}
              >
                运行信息
              </button>
            </div>
            <div
              role="tabpanel"
              id="execution-canvas-panel"
              aria-labelledby="execution-canvas-tab"
              hidden={detailTab !== 'canvas'}
            >
              <ExecutionCanvas
                key={activeRunId || 'draft'}
                events={events}
                client={client}
                runId={activeRunId ?? undefined}
              />
            </div>
            <div
              role="tabpanel"
              id="execution-diagnostics-panel"
              aria-labelledby="execution-diagnostics-tab"
              hidden={detailTab !== 'diagnostics'}
            >
              {run ? (
                <dl className={cx('metrics')}>
                  <Metric label="状态" value={displayState(run.state)} />
                  <Metric label="开始时间" value={formatTime(run.started_at)} />
                  <Metric label="更新时间" value={formatTime(run.updated_at)} />
                  <Metric label="运行 ID" value={run.run_id} mono />
                </dl>
              ) : (
                <p>发送任务后查看本次运行的信息。</p>
              )}
            </div>
          </section>
        </div>
      )}
      {!activeRunId && !runAgent && (
        <article data-ui="panel" className={cx('panel')}>
          <div data-ui="panel__heading" className={cx('panel__heading')}>
            <div>
              <h2>最近运行</h2>
            </div>
            <button className={cx('button')} onClick={() => loadList()} disabled={busy}>
              刷新
            </button>
          </div>
          <form
            className={cx('filter-bar')}
            onSubmit={(event) => {
              event.preventDefault()
              void loadList()
            }}
          >
            <label>
              <span>智能体</span>
              <select value={agentFilter} onChange={(event) => setAgentFilter(event.target.value)}>
                <option value="">全部智能体</option>
                {agentOptions.map((agent) => (
                  <option key={agent.agent_id} value={agent.agent_id}>
                    {agent.display_name}
                  </option>
                ))}
              </select>
            </label>
            <label>
              <span>状态</span>
              <select value={stateFilter} onChange={(event) => setStateFilter(event.target.value)}>
                <option value="">全部状态</option>
                {[
                  'queued',
                  'running',
                  'waiting',
                  'cancelling',
                  'succeeded',
                  'failed',
                  'cancelled',
                  'timed_out',
                ].map((state) => (
                  <option key={state} value={state}>
                    {displayState(state)}
                  </option>
                ))}
              </select>
            </label>
            <button className={cx('button')} disabled={busy}>
              筛选
            </button>
          </form>
          {!listLoaded && !listFailed && (
            <p role="status" className={cx('empty-content')}>
              正在加载运行记录…
            </p>
          )}
          {listFailed && (
            <div className={cx('empty-content')} role="alert">
              <h3>运行记录加载失败</h3>
              <button className={cx('button')} onClick={() => loadList()}>
                重新加载
              </button>
            </div>
          )}
          {listLoaded && !listFailed && summaries.length === 0 && (
            <div className={cx('empty-content')}>
              <Icon name="runs" />
              <h3>暂无运行记录</h3>
              <p>
                {agentFilter || stateFilter
                  ? '试试其他筛选条件。'
                  : '从智能体页面选择一个已发布的智能体，点击“运行”。'}
              </p>
            </div>
          )}
          {summaries.map((summary) => (
            <button
              className={cx('run-row')}
              key={summary.run_id}
              onClick={() => load(summary.run_id)}
            >
              <span>
                <strong>{summary.agent_name}</strong>
                <small>{formatTime(summary.started_at)}</small>
              </span>
              <Status value={summary.state} />
              <span>
                {summary.waiting_task_count
                  ? `${summary.waiting_task_count} 项待办`
                  : summary.result_available
                    ? '结果已就绪'
                    : TERMINAL_RUNS.has(summary.state)
                      ? '查看详情'
                      : '进行中'}
              </span>
            </button>
          ))}
          {nextCursor && (
            <button className={cx('button')} onClick={() => loadList(nextCursor)}>
              下一页
            </button>
          )}
        </article>
      )}
      {!activeRunId && !runAgent && (
        <details className={cx('list-tools')}>
          <summary>通过运行 ID 查找</summary>
          <SearchForm
            label="通过 ID 打开运行"
            placeholder="run_…"
            value={id}
            onChange={(value) => {
              clearSelection()
              setId(value)
            }}
            onSubmit={() => load()}
            busy={busy}
          />
        </details>
      )}
      {activeRunId && !run && !followError && <p role="status">正在加载运行详情…</p>}
      {run && (
        <details data-ui="panel" className={cx('panel')}>
          <summary>更多运行控制与元数据</summary>
          <div data-ui="panel__heading" className={cx('panel__heading')}>
            <div>
              <p data-ui="kicker" className={cx('kicker')}>
                运行详情
              </p>
              <h2>{run.state === 'succeeded' ? '已完成' : '当前进度'}</h2>
            </div>
            <Status value={run.state} />
          </div>
          <dl className={cx('metrics')}>
            <Metric label="开始时间" value={formatTime(run.started_at)} />
            <Metric label="更新时间" value={formatTime(run.updated_at)} />
            <Metric label="截止时间" value={formatTime(run.deadline)} />
          </dl>
          <div className={cx('actions')}>
            {!TERMINAL_RUNS.has(run.state) && (
              <>
                <button
                  className={cx('button')}
                  onClick={() => act('pause')}
                  disabled={busy || run.state === 'cancelling'}
                >
                  暂停
                </button>
                <button
                  className={cx('button')}
                  onClick={() => act('resume')}
                  disabled={busy || run.state === 'cancelling'}
                >
                  继续
                </button>
                <button
                  className={cx('button button--danger')}
                  onClick={() => act('cancel')}
                  disabled={busy}
                >
                  取消
                </button>
              </>
            )}
            <button className={cx('button')} onClick={() => load()} disabled={busy}>
              刷新
            </button>
          </div>
          <details className={cx('diagnostics')}>
            <summary>高级诊断</summary>
            <dl className={cx('metrics')}>
              <Metric label="运行 ID" value={run.run_id} mono />
              <Metric label="版本" value={run.version} />
              <Metric label="智能体部署" value={run.agent_deployment_id} mono />
              <Metric label="ETag" value={run.etag} mono />
              <Metric label="游标" value={cursor || 'origin'} mono />
            </dl>
          </details>
        </details>
      )}
      {history?.truncated && (
        <div className={cx('notice notice--info')} role="status">
          部分早期历史已不在当前时间线中。最早保留序号： {history.replayFloor}；当前最高序号：{' '}
          {history.highWaterSequence}。当前运行状态独立读取。
        </div>
      )}
      <NoticeBox notice={followError} />
      {events.length > 0 && (
        <article data-ui="panel" className={cx('panel')}>
          <div data-ui="panel__heading" className={cx('panel__heading')}>
            <div>
              <p data-ui="kicker" className={cx('kicker')}>
                执行时间线
              </p>
              <h2>诊断与任务</h2>
            </div>
          </div>
          <details>
            <summary>原始事件（{events.length} 条）</summary>
            <ol data-ui="timeline" className={cx('timeline')}>
              {events.map((event) => (
                <li key={String(event.data.event_id)}>
                  <span className={cx('timeline__dot')} />
                  <div>
                    <div className={cx('timeline__header')}>
                      <strong>{displayState(event.event)}</strong>
                    </div>
                    <details>
                      <summary>事件详情</summary>
                      <pre>{safeJson(event.data)}</pre>
                      <code>{event.id}</code>
                    </details>
                  </div>
                </li>
              ))}
            </ol>
          </details>
          {taskIds.length > 0 && (
            <div data-ui="linked-tasks" className={cx('linked-tasks')}>
              <strong>待处理任务</strong>
              {taskIds.map((taskId) => (
                <button className={cx('button')} key={taskId} onClick={() => onTask(taskId)}>
                  打开任务
                </button>
              ))}
            </div>
          )}
        </article>
      )}
      {run && (
        <details data-ui="panel" className={cx('panel')}>
          <summary>高级运行诊断</summary>
          {run && client && (
            <RunSignal
              key={`signal:${run.run_id}`}
              client={client}
              runId={run.run_id}
              onAccepted={() => refreshRun.current()}
              onPermissionLost={clearSelection}
            />
          )}
          {run && client && <RunValues key={run.run_id} client={client} runId={run.run_id} />}
          {run && client && <RunSources key={`sources:${run.run_id}`} client={client} run={run} />}
        </details>
      )}
    </section>
  )
}
