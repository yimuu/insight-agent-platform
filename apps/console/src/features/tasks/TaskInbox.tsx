import { displayState } from '../../shared/i18n/display'
import { Status } from '../../shared/ui/console-ui'
import { formatTime } from '../../shared/ui/feedback'
import sharedStyles from '../../shared/ui/Primitives.module.css'
import { classNames } from '../../shared/ui/class-names.ts'
import localStyles from './TaskSchemaForm.module.css'
const cx = classNames(sharedStyles, localStyles)
import { useCallback, useEffect, useRef, useState } from 'react'
import { PlatformClient, PlatformProblem } from '../../shared/api/client.ts'
import { newReceipt } from '../../shared/api/security.ts'
import type {
  Json,
  JsonObject,
  TaskForm,
  TaskView,
  TaskQueryPurpose,
} from '../../shared/api/types.ts'
import { TaskSchemaForm } from './TaskSchemaForm.tsx'

type Notice = { tone: 'error' | 'success' | 'info'; text: string; traceId?: string | null }
type Action = 'submit-input' | 'approve' | 'reject' | 'cancel'
type Filters = { purpose: TaskQueryPurpose; state: string; kind: string; runId: string }
const initialFilters: Filters = { purpose: 'respondable', state: 'pending', kind: '', runId: '' }
const permissionLost = (error: unknown) =>
  error instanceof PlatformProblem && (error.status === 401 || error.status === 403)
const message = (error: unknown) =>
  error instanceof Error ? error.message : '任务请求失败，请查看诊断详情。'

function validActions(actions: unknown): actions is TaskView['allowed_actions'] {
  return (
    Array.isArray(actions) &&
    actions.length <= 4 &&
    new Set(actions).size === actions.length &&
    actions.every((action) => ['submit_input', 'approve', 'reject', 'cancel'].includes(action))
  )
}

function taskFormMatches(task: TaskView, form: TaskForm): boolean {
  return (
    form.schema_version === 2 &&
    form.task_id === task.task_id &&
    form.generation === task.generation &&
    form.version === task.version &&
    form.etag === task.etag &&
    form.safe_prompt_key === task.safe_prompt_key &&
    form.allowed_actions.every((action) => task.allowed_actions.includes(action)) &&
    typeof form.response_schema_digest === 'string' &&
    form.response_schema_digest.length > 0 &&
    form.response_schema_digest === task.response_schema_digest &&
    form.response_schema?.canonical_digest === form.response_schema_digest
  )
}

export function TaskInbox({
  client,
  report,
  selectedId,
  subjectKey,
}: {
  client: PlatformClient | null
  report(notice: Notice | null): void
  selectedId: string
  subjectKey: string
}) {
  const [id, setId] = useState(selectedId)
  const [filters, setFilters] = useState(initialFilters)
  const [filterDraft, setFilterDraft] = useState(initialFilters)
  const [items, setItems] = useState<TaskView[]>([])
  const [cursor, setCursor] = useState<string | null>(null)
  const [page, setPage] = useState(1)
  const [listBusy, setListBusy] = useState(false)
  const [listError, setListError] = useState('')
  const [task, setTask] = useState<TaskView | null>(null)
  const [form, setForm] = useState<TaskForm | null>(null)
  const [formError, setFormError] = useState('')
  const [detailBusy, setDetailBusy] = useState(false)
  const [mutating, setMutating] = useState(false)
  const [classification, setClassification] = useState('internal')
  const listRequest = useRef<{ generation: number; abort?: AbortController }>({ generation: 0 })
  const detailRequest = useRef<{ generation: number; abort?: AbortController }>({ generation: 0 })
  const mutationRequest = useRef<{ generation: number; abort?: AbortController }>({ generation: 0 })
  const receipt = useRef<{ intent: string; key: string } | null>(null)
  const mutationBusy = useRef(false)

  const clearDetail = useCallback(() => {
    detailRequest.current.abort?.abort()
    detailRequest.current.generation++
    mutationRequest.current.abort?.abort()
    mutationRequest.current.generation++
    receipt.current = null
    mutationBusy.current = false
    setTask(null)
    setForm(null)
    setFormError('')
    setDetailBusy(false)
    setMutating(false)
    setClassification('internal')
  }, [])
  const clearProtected = useCallback(() => {
    listRequest.current.abort?.abort()
    listRequest.current.generation++
    setItems([])
    setCursor(null)
    setListBusy(false)
    setId('')
    clearDetail()
  }, [clearDetail])
  useEffect(
    () => () => {
      for (const request of [listRequest, detailRequest, mutationRequest]) {
        request.current.abort?.abort()
        request.current.generation++
      }
    },
    [],
  )

  const loadPage = useCallback(
    async (next?: string, pageNumber = 1) => {
      if (!client) {
        setListError('请先连接工作空间。')
        return
      }
      listRequest.current.abort?.abort()
      const abort = new AbortController()
      const generation = ++listRequest.current.generation
      listRequest.current.abort = abort
      setListBusy(true)
      setListError('')
      setItems([])
      setCursor(null)
      try {
        const response = await client.listTasks(
          { ...filters, cursor: next },
          { signal: abort.signal },
        )
        if (generation !== listRequest.current.generation || abort.signal.aborted) return
        if (
          response.data.schema_version !== 1 ||
          !Array.isArray(response.data.items) ||
          response.data.items.length > 25 ||
          response.data.items.some(
            (item) => item.schema_version !== 2 || !validActions(item.allowed_actions),
          )
        )
          throw new Error('Unsupported Task list response.')
        setItems(response.data.items)
        setCursor(response.data.next_cursor)
        setPage(pageNumber)
      } catch (error) {
        if (generation !== listRequest.current.generation || abort.signal.aborted) return
        if (permissionLost(error)) clearProtected()
        setListError(message(error))
      } finally {
        if (generation === listRequest.current.generation) setListBusy(false)
      }
    },
    [client, filters, clearProtected],
  )
  useEffect(() => {
    let active = true
    const request = listRequest.current
    queueMicrotask(() => {
      if (active) void loadPage()
    })
    return () => {
      active = false
      request.abort?.abort()
      request.generation++
    }
  }, [loadPage])

  const loadTask = useCallback(
    async (taskId: string) => {
      clearDetail()
      setId(taskId)
      if (!client || !taskId) return
      const generation = detailRequest.current.generation
      const abort = new AbortController()
      detailRequest.current.abort = abort
      setDetailBusy(true)
      try {
        const response = await client.getTask(taskId, {
          signal: abort.signal,
          purpose: filters.purpose,
        })
        if (generation !== detailRequest.current.generation || abort.signal.aborted) return
        if (
          response.data.task_id !== taskId ||
          response.data.schema_version !== 2 ||
          !validActions(response.data.allowed_actions)
        )
          throw new Error('Unsupported Task response.')
        setTask(response.data)
        if (response.data.allowed_actions.includes('submit_input')) {
          const frozen = await client.getTaskForm(taskId, { signal: abort.signal })
          if (generation !== detailRequest.current.generation || abort.signal.aborted) return
          if (
            !validActions(frozen.data.allowed_actions) ||
            frozen.etag !== frozen.data.etag ||
            !taskFormMatches(response.data, frozen.data)
          )
            throw new Error('加载表单时任务已变更，请重新加载任务。')
          setForm(frozen.data)
        }
      } catch (error) {
        if (generation !== detailRequest.current.generation || abort.signal.aborted) return
        if (permissionLost(error)) {
          clearProtected()
          setListError(message(error))
        } else
          setFormError(
            error instanceof PlatformProblem && error.code === 'task_form_unavailable'
              ? '此任务没有可用的响应表单，暂时无法编辑响应。'
              : message(error),
          )
      } finally {
        if (generation === detailRequest.current.generation) setDetailBusy(false)
      }
    },
    [client, filters.purpose, clearDetail, clearProtected],
  )
  useEffect(() => {
    let active = true
    queueMicrotask(() => {
      if (active && selectedId) void loadTask(selectedId)
    })
    return () => {
      active = false
    }
  }, [selectedId, loadTask])

  const act = async (action: Action, value?: Json) => {
    if (!client || !task || mutating || mutationBusy.current || detailBusy) return
    const allowed = action === 'submit-input' ? 'submit_input' : action
    if (
      !task.allowed_actions.includes(allowed) ||
      (form && !form.allowed_actions.includes(allowed))
    )
      throw new Error('This action is not currently available. Reload the Task.')
    if (action === 'submit-input' && (!form || !taskFormMatches(task, form) || value === undefined))
      throw new Error('Reload the Task’s frozen response form before submitting.')
    const body: JsonObject | undefined =
      action === 'submit-input'
        ? {
            classification,
            schema_digest: form!.response_schema_digest,
            value: { kind: 'inline', value },
          }
        : undefined
    if (body && new TextEncoder().encode(JSON.stringify(body)).length > 65_536)
      throw new Error('The complete Task response exceeds the 65536-byte submission limit.')
    const etag = action === 'submit-input' ? form!.etag : task.etag
    const intent = JSON.stringify({ taskId: task.task_id, etag, action, body })
    if (receipt.current?.intent !== intent)
      receipt.current = {
        intent,
        key: newReceipt(`task-${action}-${task.task_id}-v${task.version}`),
      }
    const abort = new AbortController()
    mutationRequest.current.abort = abort
    const generation = ++mutationRequest.current.generation
    mutationBusy.current = true
    setMutating(true)
    report(null)
    try {
      const result = await client.taskAction(
        task.task_id,
        action,
        etag,
        receipt.current.key,
        body,
        { signal: abort.signal },
      )
      if (generation !== mutationRequest.current.generation || abort.signal.aborted) return
      if (
        result.data.task_id !== task.task_id ||
        result.data.schema_version !== 2 ||
        !validActions(result.data.allowed_actions)
      )
        throw new Error('Unsupported Task mutation response.')
      setTask(result.data)
      setForm(null)
      setFormError('')
      receipt.current = null
      setItems((previous) =>
        previous.map((item) => (item.task_id === result.data.task_id ? result.data : item)),
      )
      report({ tone: 'success', text: `操作已提交。`, traceId: result.traceId })
    } catch (error) {
      if (generation !== mutationRequest.current.generation || abort.signal.aborted) return
      if (permissionLost(error)) {
        clearProtected()
        setListError(message(error))
      } else if (
        error instanceof PlatformProblem &&
        (error.status === 409 || error.status === 412)
      ) {
        await loadTask(task.task_id)
        report({
          tone: 'error',
          text: `${message(error)} 已重新读取任务，请检查最新状态后再操作。`,
          traceId: error.traceId,
        })
      } else
        report({
          tone: 'error',
          text: message(error),
          traceId: error instanceof PlatformProblem ? error.traceId : null,
        })
      throw error
    } finally {
      if (generation === mutationRequest.current.generation) {
        mutationBusy.current = false
        setMutating(false)
      }
    }
  }
  const invoke = (action: Action) => {
    void act(action).catch(() => {})
  }
  const busy = detailBusy || mutating
  return (
    <section className={cx('stack')}>
      <article data-ui="panel" className={cx('panel')}>
        <div data-ui="panel__heading" className={cx('panel__heading')}>
          <div>
            <p data-ui="kicker" className={cx('kicker')}>
              待办中心
            </p>
            <h2>当前任务</h2>
          </div>
          <button className={cx('button')} disabled={listBusy} onClick={() => void loadPage()}>
            刷新任务
          </button>
        </div>
        <p className={cx('body-copy')}>
          仅显示当前会话可访问的任务，提交时服务端会再次检查操作权限。
        </p>
        <form
          className={cx('form-grid')}
          onSubmit={(event) => {
            event.preventDefault()
            clearDetail()
            setFilters({ ...filterDraft, runId: filterDraft.runId.trim() })
          }}
        >
          <label>
            <span>任务范围</span>
            <select
              value={filterDraft.purpose}
              onChange={(event) =>
                setFilterDraft({ ...filterDraft, purpose: event.target.value as TaskQueryPurpose })
              }
            >
              <option value="respondable">可处理任务</option>
              <option value="viewable">可查看任务</option>
            </select>
          </label>
          <label>
            <span>任务状态</span>
            <select
              value={filterDraft.state}
              onChange={(event) => setFilterDraft({ ...filterDraft, state: event.target.value })}
            >
              {[
                '',
                'pending',
                'responded',
                'declined',
                'approved',
                'rejected',
                'cancelled',
                'expired',
              ].map((state) => (
                <option key={state} value={state}>
                  {state ? displayState(state) : '全部状态'}
                </option>
              ))}
            </select>
          </label>
          <label>
            <span>任务类型</span>
            <select
              value={filterDraft.kind}
              onChange={(event) => setFilterDraft({ ...filterDraft, kind: event.target.value })}
            >
              {[
                '',
                'approval',
                'interaction_form',
                'interaction_url_consent',
                'interaction_business_input',
                'external_authorization',
                'human_work',
              ].map((kind) => (
                <option key={kind} value={kind}>
                  {kind ? displayState(kind) : '全部类型'}
                </option>
              ))}
            </select>
          </label>
          <label>
            <span>关联运行</span>
            <input
              value={filterDraft.runId}
              onChange={(event) => setFilterDraft({ ...filterDraft, runId: event.target.value })}
              maxLength={128}
              placeholder="全部运行"
            />
          </label>
          <div className={cx('actions')}>
            <button className={cx('button')} disabled={listBusy}>
              应用筛选
            </button>
          </div>
        </form>
        {listError && (
          <p data-ui="notice--error" className={cx('notice notice--error')} role="alert">
            {listError}
          </p>
        )}
        {listBusy ? (
          <p className={cx('body-copy')} role="status">
            正在加载任务…
          </p>
        ) : items.length ? (
          <div className={cx('task-inbox-list')}>
            {items.map((item) => (
              <div data-ui="panel__heading" className={cx('panel__heading')} key={item.task_id}>
                <div>
                  <strong>{item.safe_prompt_key}</strong>
                  <p className={cx('body-copy muted')}>
                    {displayState(item.task_kind)} · {displayState(item.state)}
                  </p>
                </div>
                <button
                  className={cx('button')}
                  onClick={() => {
                    report(null)
                    void loadTask(item.task_id)
                  }}
                >
                  打开 {item.safe_prompt_key}
                </button>
              </div>
            ))}
          </div>
        ) : (
          !listError && (
            <p className={cx('body-copy')} role="status">
              {cursor ? '本页没有可见任务，可继续查看下一页。' : '本页没有任务。'}
            </p>
          )
        )}
        <div className={cx('actions')}>
          <span className={cx('body-copy')}>页码 {page}</span>
          <button
            className={cx('button')}
            disabled={listBusy || page === 1}
            onClick={() => void loadPage()}
          >
            返回首页
          </button>
          <button
            className={cx('button')}
            disabled={listBusy || !cursor}
            onClick={() => {
              if (cursor) void loadPage(cursor, page + 1)
            }}
          >
            下一页任务
          </button>
        </div>
      </article>
      <article data-ui="panel" className={cx('panel')}>
        <form
          data-ui="search"
          className={cx('search')}
          onSubmit={(event) => {
            event.preventDefault()
            report(null)
            void loadTask(id.trim())
          }}
        >
          <label>
            <span>任务 ID</span>
            <input
              value={id}
              onChange={(event) => {
                clearDetail()
                setId(event.target.value)
                report(null)
              }}
              placeholder="int_… 或 apv_…"
              required
              autoComplete="off"
              maxLength={128}
            />
          </label>
          <button className={cx('button button--primary')} disabled={busy}>
            {detailBusy ? '加载中…' : '打开'}
          </button>
        </form>
      </article>
      {formError && !task && (
        <p data-ui="notice--error" className={cx('notice notice--error')} role="alert">
          {formError}
        </p>
      )}
      {task && (
        <article data-ui="panel" className={cx('panel')}>
          <div data-ui="panel__heading" className={cx('panel__heading')}>
            <div>
              <p data-ui="kicker" className={cx('kicker')}>
                任务详情
              </p>
              <h2>{task.safe_prompt_key}</h2>
            </div>
            <Status value={task.state} />
          </div>
          <p className={cx('body-copy')}>
            截止时间： {formatTime(task.deadline)}。代次 {task.generation}，版本 {task.version}.
          </p>
          {task.task_kind === 'approval' && task.state === 'pending' && (
            <p className={cx('body-copy')}>请审阅此审批请求并选择操作。</p>
          )}
          {task.task_kind === 'external_authorization' && task.state === 'pending' && (
            <p className={cx('body-copy')}>请通过此任务的授权流程完成操作。</p>
          )}
          {formError && (
            <p data-ui="notice--error" className={cx('notice notice--error')} role="alert">
              {formError}
            </p>
          )}
          {detailBusy && (
            <p className={cx('body-copy')} role="status">
              正在加载任务表单…
            </p>
          )}
          {form?.allowed_actions.includes('submit_input') && (
            <div key={`${subjectKey}:${task.task_id}:${task.generation}:${task.version}`}>
              <label>
                <span>回复数据分类</span>
                <select
                  value={classification}
                  disabled={busy}
                  onChange={(event) => setClassification(event.target.value)}
                >
                  {['public', 'internal', 'confidential', 'restricted'].map((value) => (
                    <option key={value} value={value}>
                      {value}
                    </option>
                  ))}
                </select>
              </label>
              <TaskSchemaForm
                schema={form.response_schema}
                responseSchemaDigest={form.response_schema_digest}
                disabled={busy}
                onSubmit={(value) => act('submit-input', value)}
              />
            </div>
          )}
          <div className={cx('actions')}>
            {task.allowed_actions.includes('approve') && (
              <button
                className={cx('button button--primary')}
                onClick={() => invoke('approve')}
                disabled={busy}
              >
                批准
              </button>
            )}
            {task.allowed_actions.includes('reject') && (
              <button
                className={cx('button button--danger')}
                onClick={() => invoke('reject')}
                disabled={busy}
              >
                拒绝
              </button>
            )}
            {(form?.allowed_actions ?? task.allowed_actions).includes('cancel') && (
              <button className={cx('button')} onClick={() => invoke('cancel')} disabled={busy}>
                取消
              </button>
            )}
            <button
              className={cx('button')}
              onClick={() => {
                report(null)
                void loadTask(task.task_id)
              }}
              disabled={busy}
            >
              重新加载任务
            </button>
          </div>
          <details className={cx('diagnostics')}>
            <summary>高级诊断</summary>
            <dl className={cx('metrics')}>
              <div data-ui="metric" className={cx('metric')}>
                <dt>任务 ID</dt>
                <dd className={cx('mono')}>{task.task_id}</dd>
              </div>
              <div data-ui="metric" className={cx('metric')}>
                <dt>ETag</dt>
                <dd className={cx('mono')}>{task.etag}</dd>
              </div>
            </dl>
          </details>
        </article>
      )}
    </section>
  )
}
